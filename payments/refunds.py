"""Thin client for the request_refund() / complete_refund() database functions.

All rules live in the database (migrations/oltp/V006__refund_processing.sql); this module
passes requests in and turns the functions' SQLSTATEs into typed exceptions. Transaction
handling matches payments.events: autocommit = one transaction per call; otherwise the call
joins the caller's transaction inside a savepoint and nothing is committed here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import psycopg


class RefundError(Exception):
    """Base class: the request was rejected and nothing was written."""


class InvalidRefund(RefundError):                # RF001
    pass


class UnknownPaymentOrRefund(RefundError):       # RF002
    pass


class PaymentNotRefundable(RefundError):         # RF003: the payment is not captured
    pass


class CurrencyMismatch(RefundError):             # RF004
    pass


class RefundExceedsRemaining(RefundError):       # RF005: over-refund (requested + succeeded)
    pass


class IdempotencyKeyConflict(RefundError):       # RF006: key reused for a different request
    pass


class IllegalRefundTransition(RefundError):      # RF007: terminal status cannot change
    pass


_ERRORS = {"RF001": InvalidRefund, "RF002": UnknownPaymentOrRefund, "RF003": PaymentNotRefundable,
           "RF004": CurrencyMismatch, "RF005": RefundExceedsRemaining, "RF006": IdempotencyKeyConflict,
           "RF007": IllegalRefundTransition}


@dataclass(frozen=True)
class RefundResult:
    outcome: str            # recorded | duplicate
    refund_id: int
    refund_status: str      # requested | succeeded | failed
    refundable_minor: int   # payment amount minus requested and succeeded refunds, after this call


def _call(conn: psycopg.Connection, sql: str, params: tuple) -> RefundResult:
    try:
        if conn.autocommit:
            with conn.transaction():
                row = conn.execute(sql, params).fetchone()
        else:
            conn.execute("SAVEPOINT refund_write")
            try:
                row = conn.execute(sql, params).fetchone()
            except psycopg.Error:
                conn.execute("ROLLBACK TO SAVEPOINT refund_write")
                raise
            conn.execute("RELEASE SAVEPOINT refund_write")
    except psycopg.Error as exc:
        error = _ERRORS.get(exc.sqlstate or "")
        if error is None:
            raise
        raise error(exc.diag.message_primary) from exc
    return RefundResult(*row)


def request_refund(conn: psycopg.Connection, *, idempotency_key: str, transaction_id: int,
                   amount_minor: int, currency: str, reason: str) -> RefundResult:
    return _call(conn, "SELECT * FROM request_refund(%s, %s, %s, %s, %s)",
                 (idempotency_key, transaction_id, amount_minor, currency, reason))


def complete_refund(conn: psycopg.Connection, refund_id: int, status: str, *,
                    failure_code: str | None = None, completed_at: datetime | None = None) -> RefundResult:
    return _call(conn, "SELECT * FROM complete_refund(%s, %s, %s, %s)",
                 (refund_id, status, failure_code, completed_at))
