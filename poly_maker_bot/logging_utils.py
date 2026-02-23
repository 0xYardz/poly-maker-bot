# poly_maker_bot/logging_utils.py
from __future__ import annotations

import logging
import os
import sys


# ANSI color codes
class Colors:
  RESET = "\033[0m"
  BOLD = "\033[1m"

  # Log level colors
  DEBUG = "\033[36m"     # Cyan
  INFO = "\033[32m"      # Green
  WARNING = "\033[33m"   # Yellow
  ERROR = "\033[31m"     # Red
  CRITICAL = "\033[35m"  # Magenta (bold red background)

  # Additional colors for message highlighting
  GRAY = "\033[90m"      # Gray (for timestamps, etc.)
  WHITE = "\033[37m"     # White


class ColoredFormatter(logging.Formatter):
  """Custom formatter that adds colors based on log level."""

  LEVEL_COLORS = {
    logging.DEBUG: Colors.DEBUG,
    logging.INFO: Colors.INFO,
    logging.WARNING: Colors.WARNING,
    logging.ERROR: Colors.ERROR,
    logging.CRITICAL: Colors.CRITICAL,
  }

  def __init__(self, fmt: str, datefmt: str, use_colors: bool = True):
    super().__init__(fmt=fmt, datefmt=datefmt)
    self.use_colors = use_colors

  def format(self, record: logging.LogRecord) -> str:
    if not self.use_colors:
      return super().format(record)

    # Get color for this level
    level_color = self.LEVEL_COLORS.get(record.levelno, Colors.WHITE)

    # Color the level name
    original_levelname = record.levelname
    record.levelname = f"{level_color}{record.levelname}{Colors.RESET}"

    # Format the message
    formatted = super().format(record)

    # Restore original levelname (in case record is reused)
    record.levelname = original_levelname

    # Color the timestamp gray
    formatted = formatted.replace(
      f"[{self.formatTime(record, self.datefmt)}.{record.msecs:03.0f}]",
      f"{Colors.GRAY}[{self.formatTime(record, self.datefmt)}.{record.msecs:03.0f}]{Colors.RESET}"
    )

    return formatted


def setup_logging(level: int | None = None, use_colors: bool = True) -> None:
  """
  Setup logging with optional colored output.

  Args:
    level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    use_colors: Whether to use ANSI colors (auto-disabled if not a TTY)
  """
  if level is None:
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

  root = logging.getLogger()
  root.setLevel(level)

  # avoid duplicate handlers if called multiple times
  if any(isinstance(h, logging.StreamHandler) for h in root.handlers):
    return

  # Auto-disable colors if output is not a terminal (e.g., redirected to file)
  if use_colors and not sys.stdout.isatty():
    use_colors = False

  fmt = "[%(asctime)s.%(msecs)03d] [%(levelname)s] %(name)s: %(message)s"
  datefmt = "%Y-%m-%dT%H:%M:%S"
  handler = logging.StreamHandler(sys.stdout)
  handler.setFormatter(ColoredFormatter(fmt=fmt, datefmt=datefmt, use_colors=use_colors))
  root.addHandler(handler)

  # Suppress noisy third-party loggers
  logging.getLogger("httpx").setLevel(logging.WARNING)
  logging.getLogger("httpcore").setLevel(logging.WARNING)
