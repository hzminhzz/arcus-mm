"""Pure quote calculations for the bounded maker cycle."""

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

from arcus_bot.types import InputError, ProtocolError


def aligned_units(value: Decimal, unit: Decimal, label: str) -> int:
    """Convert a human-readable amount into exact market increments."""
    if value <= 0 or unit <= 0:
        raise InputError(f"{label} and its market increment must be positive")
    units = value / unit
    if units != units.to_integral_value():
        raise InputError(f"{label} {value} is not a multiple of {unit}")
    return int(units)


def passive_entry_price(side: str, best_bid: Decimal, best_ask: Decimal) -> Decimal:
    """Choose the same-side BBO price, never a marketable limit."""
    if best_bid <= 0 or best_ask <= best_bid:
        raise ProtocolError("Arcus returned an invalid or crossed order book")
    match side:
        case "BUY":
            return best_bid
        case "SELL":
            return best_ask
        case _:
            raise InputError("side must be BUY or SELL")


def passive_take_profit_price(
    side: str,
    fill_price: Decimal,
    best_bid: Decimal,
    best_ask: Decimal,
    tick_size: Decimal,
    profit_percent: Decimal,
) -> Decimal:
    """Price a passive exit at or beyond the requested profit target."""
    fraction = profit_percent / Decimal(100)
    if side == "BUY":
        target = max(fill_price * (Decimal(1) + fraction), best_ask)
        return (target / tick_size).to_integral_value(rounding=ROUND_CEILING) * tick_size
    if side == "SELL":
        target = min(fill_price * (Decimal(1) - fraction), best_bid)
        return (target / tick_size).to_integral_value(rounding=ROUND_FLOOR) * tick_size
    raise InputError("side must be BUY or SELL")


def estimated_round_trip_notional(entry_price: Decimal, quantity: Decimal) -> Decimal:
    """Estimate entry plus exit notional at the entry price."""
    return Decimal(2) * entry_price * quantity
