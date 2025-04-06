import json
from pathlib import Path
from typing import Literal

import httpx

PROJECT_DIR = Path(__file__).parent.parent
FIXTURES_DIR = PROJECT_DIR / "fixtures"

FILE_NAMES: dict[str, str] = {
        "spot": "exchangeInfoSpot.json",
        "usdm": "exchangeInfoUSDM.json",
        "coinm": "exchangeInfoCoinM.json",
    }


def download_exchange_info(endpoint: Literal["spot", "usdm", "coinm"]) -> None:
    endpoints = {
        "spot": "https://api.binance.com/api/v3/exchangeInfo",
        "usdm": "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "coinm": "https://dapi.binance.com/dapi/v1/exchangeInfo"
    }
    url = endpoints[endpoint]
    file_name = FILE_NAMES[endpoint]
    file_path = FIXTURES_DIR / file_name
    response = httpx.get(url)
    response.raise_for_status()
    response = response.json()
    with open(file_path, "w+") as f:
        json.dump(response, f)


def check_symbol_key_value_unique(endpoint: Literal["spot", "usdm", "coinm"], key: str) -> None:
    file_name = FILE_NAMES[endpoint]
    file_path = FIXTURES_DIR / file_name
    with open(file_path) as f:
        response: dict = json.load(f)
    symbols: list[dict] = response.get("symbols", [])
    values = set()
    for symbol in symbols:
        values.add(symbol.get(key))
    print(values)
    


if __name__ == "__main__":
    # download_exchange_info("usdm")
    check_symbol_key_value_unique("usdm", "contractType")
