"""Object-pool backed request/response handler.

Reset contract (see README for the full story):

Before any object is made available for reuse, ``Request.reset`` /
``Response.reset`` must put it back into the *exact* state of a freshly
constructed instance:

* scalar slots (``username``/``amount``/``status``/``error``) return to their
  neutral defaults;
* mutable containers (``headers``/``fields``) are emptied in place;
* ``body`` (a bytearray) is emptied but may keep its allocated capacity.

The only legal release path is ``ObjectPool._put_back``; it is reached solely
through ``BorrowPool``'s context-manager ``__exit__``, which runs for the
normal path, exceptions, and ``CancelledError`` alike.  An object may be
returned at most once, so reset is never skipped and never doubled up.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from collections import namedtuple


class HandlerError(Exception):
    """Raised for an unknown route (body is deliberately populated first)."""


class CancelledError(Exception):
    """Cooperative cancellation, signalled by an ``is_set()``-able object."""


Result = namedtuple("Result", ("status", "username", "amount", "headers", "fields", "body"))


class Request:
    __slots__ = ("username", "amount", "headers", "body")

    def __init__(self) -> None:
        self.username: str = ""
        self.amount: int = 0
        self.headers: dict[str, str] = {}
        self.body: bytearray = bytearray()

    def reset(self) -> None:
        self.username = ""
        self.amount = 0
        self.headers.clear()
        self.body.clear()


class Response:
    __slots__ = ("status", "error", "headers", "fields", "body")

    def __init__(self) -> None:
        self.status: int = 200
        self.error: str = ""
        self.headers: dict[str, str] = {}
        self.fields: dict[str, object] = {}
        self.body: bytearray = bytearray()

    def reset(self) -> None:
        self.status = 200
        self.error = ""
        self.headers.clear()
        self.fields.clear()
        self.body.clear()


class ObjectPool:
    """A thread-safe pool of ``Request``/``Response`` instances.

    ``allocations`` counts how many real objects were ever built; it stays
    bounded by the high-water mark of concurrent borrowers instead of growing
    per request.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._free: dict[type, list] = {}
        self.allocations = 0

    def _take(self, cls):
        with self._lock:
            idle = self._free.get(cls)
            if idle:
                return idle.pop()
            self.allocations += 1
        return cls()

    def _put_back(self, cls, obj) -> None:
        with self._lock:
            idle = self._free.setdefault(cls, [])
            obj.reset()
            idle.append(obj)


class BorrowPool:
    """Hands out a ``(request, response)`` pair and guarantees return."""

    def __init__(self) -> None:
        self._pool = ObjectPool()

    @property
    def allocations(self) -> int:
        return self._pool.allocations

    @contextmanager
    def borrow(self):
        request = self._pool._take(Request)
        response = self._pool._take(Response)
        try:
            yield request, response
        finally:
            # Resetting on the single release path covers normal completion,
            # raised errors, and cancellation alike.  Reset happens under the
            # pool lock so a subsequent borrower always observes a clean pair.
            self._pool._put_back(Request, request)
            self._pool._put_back(Response, response)


class Handler:
    def __init__(self) -> None:
        self.pool = BorrowPool()

    def handle(self, username: str, amount: int, headers, body=b"",
               path: str = "/echo", cancel_event=None) -> Result:
        """Process one request and return an immutable detached ``Result``.

        The pooled request/response are returned (and reset) before this method
        comes back, so callers can never keep a live pooled object.
        """
        with self.pool.borrow() as (request, response):
            self._populate_request(request, username, amount, headers, body)
            self._dispatch(request, response, path, cancel_event)
            return self._snapshot(response)

    @staticmethod
    def _populate_request(request, username, amount, headers, body) -> None:
        request.username = username
        request.amount = amount
        request.headers.update(headers)
        request.body.extend(body)

    @staticmethod
    def _check_cancel(cancel_event) -> None:
        if cancel_event is not None and cancel_event.is_set():
            raise CancelledError("request cancelled")

    def _dispatch(self, request, response, path, cancel_event) -> None:
        # In a real handler this body transfer is the expensive, interruptible
        # part: cancellation must still leave the objects safe to recycle.
        response.headers["X-Request-User"] = request.username
        response.body.extend(request.body)
        self._check_cancel(cancel_event)

        if path == "/ping":
            response.status = 200
            response.fields["pong"] = True
            return
        if path == "/echo":
            response.status = 200
            response.fields["username"] = request.username
            response.fields["amount"] = request.amount
            response.fields["echo_len"] = len(response.body)
            response.headers["X-Echo"] = "1"
            return
        if path == "/boom":
            response.status = 500
            response.error = "boom"
            response.fields["username"] = request.username
            raise HandlerError(response.error)

        response.status = 404
        response.error = f"unknown path: {path}"
        raise HandlerError(response.error)

    @staticmethod
    def _snapshot(response) -> Result:
        # Copy only the small outbound payload.  The large body is transferred
        # byte-for-byte into a fresh bytes object; pooled containers and the
        # body buffer itself are never handed out and are reset on release.
        return Result(
            status=response.status,
            username=response.fields.get("username", ""),
            amount=response.fields.get("amount", 0),
            headers=dict(response.headers),
            fields=dict(response.fields),
            body=bytes(response.body),
        )
