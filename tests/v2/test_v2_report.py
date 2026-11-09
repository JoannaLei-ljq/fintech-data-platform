"""L3: report formatting (pure functions)."""

from decimal import Decimal

from sync.v2_report import fmt_rate, major


def test_zero_rate_prints_as_zero_and_missing_rate_as_dash():
    assert fmt_rate(Decimal("0.00")) == "0.00"
    assert fmt_rate(Decimal("66.67")) == "66.67"
    assert fmt_rate(None) == "-"


def test_amounts_use_each_currencys_minor_units_exactly():
    assert (major(15000, 2), major(1500, 0), major(12345, 3), major(-700, 2)) == ("150.00", "1500", "12.345", "-7.00")
