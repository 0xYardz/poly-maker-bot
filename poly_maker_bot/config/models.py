"""
Configuration models for the bot.

This module contains all configuration classes and constants organized by
logical groups for better maintainability and testability.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

@dataclass
class MarketFilterConfig:
    """Configuration for market filtering."""

    # Market slug exclusions - comma-separated substrings
    excluded_slug_patterns: str = os.environ.get("POLY_MAKER_BOT_EXCLUDED_SLUG_PATTERNS", "")


@dataclass
class BotConfig:
    """Main bot configuration container."""
    market_filter: MarketFilterConfig = field(default_factory=MarketFilterConfig)

