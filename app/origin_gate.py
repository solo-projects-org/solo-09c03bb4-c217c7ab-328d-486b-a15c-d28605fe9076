"""Per-origin request scheduling.

Rules implemented here:

* One origin == (scheme, host, port). Every origin has its own gate.
* At most one request in flight per origin. The next request starts only after
  the previous response body has been fully read (or the connection was
  closed). The crawler holds the gate across the whole request/read cycle.
* A 429 with an integer ``Retry-After`` pauses the whole origin for that many
  integer seconds. Requests already queued for that origin wait for the pause
  to expire as well. Other origins are unaffected.
* Redirect hops each go through the gate of the *target* origin, so a chain
  A -> B -> A can never deadlock: a request never holds one origin's slot
  while waiting for another origin.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, Tuple
from urllib.parse import urlsplit


Origin = Tuple[str, str, int]


def origin_of(url: str) -> Origin:
    """Return the (scheme, host, port) origin of *url*.

    Default ports are normalised so ``http://host`` and ``http://host:80``
    share one gate.  Raises ``ValueError`` for non-http(s) URLs.
    """
    parts = urlsplit(url)
    scheme = (parts.scheme or "").lower()
    if scheme not in ("http", "https"):
        raise ValueError("仅支持 http/https URL")
    host = (parts.hostname or "").lower()
    if not host:
        raise ValueError("URL 缺少主机名")
    port = parts.port
    if port is None:
        port = 443 if scheme == "https" else 80
    return scheme, host, port


def origin_label(origin: Origin) -> str:
    scheme, host, port = origin
    return f"{scheme}://{host}:{port}"


@dataclass
class _Waiter:
    future: asyncio.Future


@dataclass
class OriginState:
    origin: Origin
    # Strict FIFO queue. A waiter is admitted only when it is at the front,
    # no request is in flight, and any Retry-After pause has expired.
    waiters: Deque[_Waiter] = field(default_factory=deque)
    in_flight: Optional[str] = None
    pause_until: float = 0.0  # monotonic clock

    def snapshot(self) -> dict:
        return {
            "in_flight": self.in_flight,
            "queued": len(self.waiters),
        }


class OriginGate:
    """FIFO per-origin concurrency + Retry-After pause gate."""

    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self._states: Dict[Origin, OriginState] = {}

    def state(self, origin: Origin) -> OriginState:
        return self._states.setdefault(origin, OriginState(origin))

    def in_flight(self, origin: Origin) -> Optional[str]:
        return self.state(origin).in_flight

    def queued(self, origin: Origin) -> int:
        return len(self.state(origin).waiters)

    def pause_remaining(self, origin: Origin) -> float:
        return max(0.0, self.state(origin).pause_until - self._clock())

    def pause_origin(self, origin: Origin, seconds: Optional[int]) -> None:
        """Pause *origin* for *seconds*.

        Only non-negative integer values are accepted; ``None`` means there
        was no usable integer Retry-After header.
        """
        if seconds is None or seconds < 0:
            return
        st = self.state(origin)
        until = self._clock() + float(seconds)
        if until > st.pause_until:
            st.pause_until = until
            # The head waiter may be sleeping on an older, shorter deadline.
            self._notify_head(st)

    def _notify_head(self, st: OriginState) -> None:
        if st.waiters and not st.waiters[0].future.done():
            st.waiters[0].future.set_result(None)

    async def acquire(self, url_or_origin, label: str = "") -> Origin:
        """Wait for the origin's slot, mark it in flight, return the origin.

        The caller must call :meth:`release` after the response body has been
        read or the connection closed.
        """
        origin = (url_or_origin if isinstance(url_or_origin, tuple)
                  else origin_of(url_or_origin))
        st = self.state(origin)
        waiter = _Waiter(future=asyncio.get_running_loop().create_future())
        st.waiters.append(waiter)
        try:
            while True:
                if self._can_proceed(st, waiter):
                    st.waiters.remove(waiter)
                    st.in_flight = label or "?"
                    return origin
                if waiter.future.done():
                    # Woken but conditions changed (e.g. another timer fired
                    # while a new pause was installed).  Reset and re-check.
                    waiter.future = asyncio.get_running_loop().create_future()
                await waiter.future
        except asyncio.CancelledError:
            with contextlib.suppress(ValueError):
                st.waiters.remove(waiter)
            if not waiter.future.done():
                waiter.future.cancel()
            # Let the new head re-evaluate (it may now be admissible).
            self._notify_head(st)
            raise

    def _can_proceed(self, st: OriginState, waiter: _Waiter) -> bool:
        if st.in_flight is not None:
            return False
        if not st.waiters or st.waiters[0] is not waiter:
            return False
        remaining = st.pause_until - self._clock()
        if remaining > 0:
            # Wake up at the pause deadline.  Never block the event loop by
            # sleeping while holding anything; just return and await.
            asyncio.get_running_loop().call_later(
                remaining, self._notify_head, st)
            return False
        return True

    def release(self, origin: Origin) -> None:
        st = self.state(origin)
        st.in_flight = None
        self._notify_head(st)

    def wake(self, origin: Origin) -> None:
        self._notify_head(self.state(origin))

    def as_dict(self) -> Dict[str, dict]:
        result = {}
        now = self._clock()
        for origin, st in self._states.items():
            result[origin_label(origin)] = {
                **st.snapshot(),
                "pause_remaining": max(0.0, round(st.pause_until - now, 3)),
            }
        return result
