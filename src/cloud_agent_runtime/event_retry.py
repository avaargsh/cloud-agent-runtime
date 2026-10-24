from __future__ import annotations

import asyncio
import math
import random
import time
from dataclasses import dataclass, field
from typing import Callable

from .event_delivery import EventDeliveryUncertain, TemporalEventDispatcher
from .event_inbox import EventConflictError, SQLiteEventInbox


@dataclass(frozen=True)
class RetryOutcome:
    """One bounded reconciliation attempt; never a business-execution result."""

    status: str
    event_id: str | None = None
    attempts: int = 0
    next_attempt_at: float | None = None


@dataclass
class EventRetryCoordinator:
    """One-shot, single-writer reconciliation for one canonical Runtime Run.

    The caller owns scheduling and must ensure only one coordinator dispatches
    for a SQLite database at a time. This class does not spawn a process or
    create a second task queue / workflow engine.
    """

    inbox: SQLiteEventInbox
    dispatcher: TemporalEventDispatcher
    clock: Callable[[], float] = field(default=time.time)
    jitter: Callable[[], float] = field(default=random.random)
    base_delay_seconds: float = 2.0
    max_delay_seconds: float = 60.0
    max_attempts: int = 8
    attempt_timeout_seconds: float = 15.0

    def __post_init__(self) -> None:
        if self.dispatcher.inbox is not self.inbox:
            raise ValueError("coordinator and dispatcher must share the same Inbox")
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts < 1
        ):
            raise ValueError("max_attempts must be a positive integer")
        if any(
            isinstance(n, bool)
            or not isinstance(n, (int, float))
            or not math.isfinite(n)
            or n <= 0
            for n in (self.base_delay_seconds, self.max_delay_seconds, self.attempt_timeout_seconds)
        ) or self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("retry delays must be finite positive ordered seconds")

    def _next_delay(self, attempt: int) -> float:
        fraction = self.jitter()
        if isinstance(fraction, bool) or not isinstance(fraction, (int, float)) or not math.isfinite(fraction) or not 0 <= fraction <= 1:
            raise ValueError("jitter must return a fraction between zero and one")
        maximum = min(self.max_delay_seconds, self.base_delay_seconds * (2 ** min(attempt - 1, 30)))
        return maximum * (0.5 + 0.5 * fraction)

    async def reconcile_once(self, *, run_id: str, tenant_id: str) -> RetryOutcome:
        """Attempt the oldest pending event once, ordered per Run.

        Return a status that an external scheduler/operator may inspect.
        An unconfirmed event is never removed, even at the retry limit.
        """
        pending = self.inbox.list_pending(run_id=run_id, tenant_id=tenant_id, limit=1)
        if not pending:
            return RetryOutcome("idle")
        event = pending[0]
        previous = self.inbox.get_retry_state(event)
        if previous and previous.blocked:
            return RetryOutcome("blocked", event.event_id, previous.attempts)
        now = self.clock()  # Absolute UTC epoch; persists across process restarts.
        if not math.isfinite(now):
            raise ValueError("clock must return a finite epoch timestamp")
        if previous and now < previous.next_attempt_at:
            return RetryOutcome(
                "deferred", event.event_id, previous.attempts,
                previous.next_attempt_at,
            )

        reason = "transport_unknown"
        blocked = False
        try:
            confirmed = await asyncio.wait_for(
                self.dispatcher.deliver_pending(
                    run_id=run_id, tenant_id=tenant_id, limit=1
                ),
                timeout=self.attempt_timeout_seconds,
            )
            if confirmed != 1:
                raise EventDeliveryUncertain(
                    "pending event was not confirmed", reason="confirmation_timeout"
                )
        except EventConflictError:
            reason, blocked = "payload_conflict", True
        except PermissionError:
            reason, blocked = "identity_conflict", True
        except EventDeliveryUncertain as exc:
            reason = exc.reason
            blocked = reason == "terminal_unconfirmed"
        except Exception:
            # A failed Query can happen before or after a Signal was accepted.
            # An unknown failure is bounded, never considered delivered.
            reason = "transport_unknown"
        else:
            # A committed receipt also clears the retry schedule transactionally.
            return RetryOutcome("confirmed", event.event_id)

        attempt = (previous.attempts if previous else 0) + 1
        blocked = blocked or attempt >= self.max_attempts
        next_at = now if blocked else now + self._next_delay(attempt)
        state = self.inbox.record_retry_failure(
            event, next_attempt_at=next_at,
            last_error_class=reason, blocked=blocked,
        )
        return RetryOutcome(
            "blocked" if blocked else "retry_scheduled",
            event.event_id, state.attempts,
            None if blocked else state.next_attempt_at,
        )
