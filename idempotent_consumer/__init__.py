"""Idempotent event consumer demo package."""

from .events import Event, gen_batch

__all__ = ["Event", "gen_batch"]
