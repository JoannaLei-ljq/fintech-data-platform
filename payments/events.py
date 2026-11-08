"""Thin client for the record_payment_event() database function.

All rules live in the database (migrations/oltp/V005__payment_event_write_path.sql); this
module only passes events in and turns the function's SQLSTATEs into typed exceptions.
Application connections should run as a login role that is a member of v2_payment_app.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

import psycopg


class PaymentEventError(Exception):
    """Base class: the event was rejected and nothing was written."""
    retryable = False


class InvalidEvent(PaymentEventError):        # PE001
    pass


class UnknownPayment(PaymentEventError):      # PE002
    pass


class SequenceGap(PaymentEventError):         # PE003: earlier events are missing; retry later
    retryable = True


class SequenceConflict(PaymentEventError):    # PE004: that sequence number is already taken
    pass


class IllegalTransition(PaymentEventError):   # PE005
    pass


class EventIdConflict(PaymentEventError):     # PE006: event_id reused with a different payload
    pass


_ERRORS = {"PE001": InvalidEvent, "PE002": UnknownPayment, "PE003": SequenceGap,
           "PE004": SequenceConflict, "PE005": IllegalTransition, "PE006": EventIdConflict}


@dataclass(frozen=True)
class PaymentEvent:
    event_id: uuid.UUID
    transaction_id: int
    event_sequence: int
    event_type: str            # payment.created | payment.authorized | payment.captured | payment.failed
    occurred_at: datetime
    source: str                # api | processor
    amount_minor: int | None = None
    currency: str | None = None
    failure_code: str | None = None
    user_id: int | None = None       # payment.created only
    merchant_id: int | None = None   # payment.created only


@dataclass(frozen=True)
class RecordResult:
    outcome: str               # recorded | duplicate
    transaction_status: str
    last_event_sequence: int
    received_at: datetime


_CALL = "SELECT * FROM record_payment_event(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"


def record_payment_event(conn: psycopg.Connection, event: PaymentEvent) -> RecordResult:
    """Record one event atomically.

    In autocommit mode the call is its own transaction and is committed on success. Otherwise
    it joins the caller's transaction inside a savepoint: a rejected event is undone without
    aborting the caller's transaction, and nothing is committed until the caller commits.
    """
    params = (event.event_id, event.transaction_id, event.event_sequence, event.event_type,
              event.occurred_at, event.source, event.amount_minor, event.currency,
              event.failure_code, event.user_id, event.merchant_id)
    try:
        if conn.autocommit:
            with conn.transaction():
                row = conn.execute(_CALL, params).fetchone()
        else:
            conn.execute("SAVEPOINT record_payment_event")
            try:
                row = conn.execute(_CALL, params).fetchone()
            except psycopg.Error:
                conn.execute("ROLLBACK TO SAVEPOINT record_payment_event")
                raise
            conn.execute("RELEASE SAVEPOINT record_payment_event")
    except psycopg.Error as exc:
        error = _ERRORS.get(exc.sqlstate or "")
        if error is None:
            raise
        raise error(exc.diag.message_primary) from exc
    return RecordResult(*row)
