"""Behavior tests for the Arcus cycle strategy and account cache."""

from decimal import Decimal

import pytest

from arcus_bot.bots.cycle.quoter import (
    aligned_units,
    estimated_round_trip_notional,
    passive_entry_price,
    passive_take_profit_price,
)
from arcus_bot.sdk.account import AccountState
from arcus_bot.sdk.orders import ArcusOrders
from arcus_bot.types import AccountRef, Config, InputError


@pytest.fixture
def config() -> Config:
    """Provide one small BTC market strategy configuration."""
    return Config(
        account=AccountRef(
            address="0x1234567890abcdef1234567890abcdef12345678",
            account_index=0,
            market_id=1,
            market="BTC-USD",
        ),
        side="BUY",
        quantity=Decimal("0.01"),
        tick_size=Decimal("0.5"),
        step_size=Decimal("0.001"),
        take_profit_percent=Decimal("0.02"),
        max_order_notional=Decimal("1000"),
        max_total_volume=Decimal("5000"),
        cycles=1,
        wait_seconds=0,
        entry_timeout_seconds=30,
        mainnet=False,
    )


@pytest.mark.parametrize(
    ("side", "expected"),
    [("BUY", Decimal("100")), ("SELL", Decimal("101"))],
)
def test_entry_price_uses_own_side_of_book(side: str, expected: Decimal) -> None:
    # Given a valid, uncrossed book.
    # When choosing an entry for either direction.
    price = passive_entry_price(side, Decimal("100"), Decimal("101"))

    # Then the limit rests on the selected side rather than crossing the spread.
    assert price == expected


def test_take_profit_price_respects_target_and_passive_side() -> None:
    # Given a filled long entry at 100 and an ask below the profit target.
    price = passive_take_profit_price(
        "BUY",
        Decimal("100"),
        Decimal("100.01"),
        Decimal("100.01"),
        Decimal("0.01"),
        Decimal("0.02"),
    )

    # When calculating a passive take-profit exit.
    # Then it rests above the bid at the requested target or better.
    assert price == Decimal("100.02")


def test_order_payload_uses_one_timestamp_and_alo(config: Config) -> None:
    # Given an aligned entry quote and a fixed nonce.
    timestamp = 1_234_567_891_234

    # When building an Arcus order payload.
    body, signature_payload = ArcusOrders.build_place_payload(
        config,
        "BUY",
        Decimal("25000"),
        Decimal("0.01"),
        timestamp,
    )

    # Then body timestamp, signature nonce, and post-only fields agree.
    assert body["timestamp"] == timestamp
    assert body["timeInForce"] == "ALO"
    assert body["orderType"] == "LIMIT"
    assert f'"ct":{timestamp}'.encode() in signature_payload
    assert b'"p":50000' in signature_payload
    assert b'"q":10' in signature_payload
    assert b'"t":3' in signature_payload


def test_round_trip_notional_includes_both_legs() -> None:
    # Given a unit quantity and a 100-dollar entry.
    # When estimating both entry and exit notional.
    volume = estimated_round_trip_notional(Decimal("100"), Decimal("2"))

    # Then the estimate includes each leg.
    assert volume == Decimal("400")


def test_alignment_rejects_non_market_increment() -> None:
    # Given a price between valid market ticks.
    # When converting it into exchange-native units.
    # Then the quote is rejected before signing.
    with pytest.raises(InputError, match="not a multiple"):
        _ = aligned_units(Decimal("100.25"), Decimal("0.5"), "price")


def test_account_cache_tracks_position_fill_and_order() -> None:
    # Given account channel snapshots and live updates for one market.
    state = AccountState(market_id=1)
    state.apply(
        {
            "type": "subscribed",
            "channel": "positions",
            "contents": {
                "lastSequenceId": 10,
                "positions": {
                    "1": {
                        "marketId": 1,
                        "side": "LONG",
                        "size": "0.02",
                        "sequenceNumber": 10,
                    }
                }
            },
        }
    )
    state.apply(
        {
            "type": "channel_data",
            "channel": "orders",
            "contents": {
                "marketId": 1,
                "orderId": "ord-1",
                "status": "FILLED",
                "state": "FILLED",
                "originalSize": "0.02",
                "remainingSize": "0",
                "avgFillPrice": "25000",
                "sequenceNumber": 10,
            },
        }
    )
    state.apply(
        {
            "type": "channel_data",
            "channel": "userFills",
            "contents": {
                "marketId": 1,
                "tradeId": "fill-1",
                "orderId": "ord-1",
                "side": "BUY",
                "fillPrice": "25000",
                "fillSize": "0.02",
                "fee": "0",
                "role": "MAKER",
            },
        }
    )

    # When the monitor reads the cache.
    # Then it reports the signed position, terminal order, and maker fill fee.
    assert state.position_ready
    assert state.position == Decimal("0.02")
    assert state.order_states["ord-1"].status == "FILLED"
    assert not state.open_orders
    assert state.fills[0].role == "MAKER"
    assert state.fills[0].fee == Decimal(0)
