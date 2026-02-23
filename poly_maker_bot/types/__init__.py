# poly_maker_bot/types/__init__.py
"""Custom type definitions for the poly_maker_bot package."""

from poly_maker_bot.types.common import (
    EthAddress,
    FlexibleDatetime,
    HexString,
    Keccak256,
    Keccak256OrPadded,
    EmptyString,
    TimeseriesPoint,
    parse_flexible_datetime,
    validate_eth_address,
    validate_keccak256,
    hexbytes_to_str,
    validate_keccak_or_padded,
)
from poly_maker_bot.types.web3_types import (
    TransactionLog,
    TransactionReceipt,
)

__all__ = [
    # common.py
    "EthAddress",
    "FlexibleDatetime",
    "HexString",
    "Keccak256",
    "Keccak256OrPadded",
    "EmptyString",
    "TimeseriesPoint",
    "parse_flexible_datetime",
    "validate_eth_address",
    "validate_keccak256",
    "hexbytes_to_str",
    "validate_keccak_or_padded",
    # web3_types.py
    "TransactionLog",
    "TransactionReceipt",
]
