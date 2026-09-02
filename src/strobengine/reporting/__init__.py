"""Shared utilities for reporting modules."""


def us_to_ms(us: float | None) -> float:
    """Convert microseconds to milliseconds, rounded to 2 decimal places."""
    if us is None:
        return 0.0
    return round(us / 1000, 2)
