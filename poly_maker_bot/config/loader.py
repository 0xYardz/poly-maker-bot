# poly_maker_bot/config.py
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import yaml
import os

CONFIG_PATH = os.environ.get("POLYMM_CONFIG", "config.yaml")


@dataclass
class ExchangeConfig:
  api_url: str
  data_api_url: str
  gamma_api_url: str
  ws_url: str
  ws_user_url: str
  rtds_ws_url: str
  funder_env: str
  private_key_env: str
  builder_api_key_env: str
  builder_secret_env: str
  builder_passphrase_env: str

@dataclass
class Config:
  polymarket: ExchangeConfig


def load_config(path: str | Path = CONFIG_PATH) -> Config:
  with open(path, "r") as f:
    raw = yaml.safe_load(f)

  return Config(
    polymarket=ExchangeConfig(**raw["exchanges"]["polymarket"]),
  )
