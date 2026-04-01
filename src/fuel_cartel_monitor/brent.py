"""Fetch Brent crude oil prices from multiple sources."""
import csv
import io
import logging
import os
from datetime import date

import duckdb
import httpx

logger = logging.getLogger(__name__)

CRUDE_PRICE_API_BASE = "https://www.crudepriceapi.com/api/prices"


def _fetch_brent_usd_crude_api() -> dict[str, float]:
    """Fetch recent Brent prices from CrudePriceAPI (near real-time).

    Returns dict mapping date_str -> USD/barrel price.
    Free tier: 100 requests/month, updated every 5 minutes.
    """
    api_key = os.environ.get("CRUDE_PRICE_API_KEY", "")
    if not api_key:
        raise ValueError("CRUDE_PRICE_API_KEY not set")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    prices: dict[str, float] = {}

    with httpx.Client(timeout=30.0) as client:
        r = client.get(f"{CRUDE_PRICE_API_BASE}/past-month", headers=headers)
        r.raise_for_status()
        data = r.json()

        for entry in data.get("data", []):
            try:
                ts = entry.get("created_at", entry.get("date", ""))
                d = ts[:10]  # "2026-03-31T12:00:00.000Z" -> "2026-03-31"
                p = float(entry.get("price", 0))
                if d and p > 0:
                    prices[d] = p
            except (ValueError, TypeError, KeyError):
                continue

    logger.info("CrudePriceAPI: fetched %d Brent records", len(prices))
    return prices


def _fetch_brent_usd_eia(date_from: date, date_to: date) -> dict[str, float]:
    """Fetch Brent prices from EIA (fallback, ~8 day delay)."""
    api_key = os.environ.get("EIA_API_KEY", "DEMO_KEY")
    url = (
        f"https://api.eia.gov/v2/petroleum/pri/spt/data/"
        f"?api_key={api_key}&frequency=daily&data[0]=value"
        f"&facets[series][]=RBRTE"
        f"&sort[0][column]=period&sort[0][direction]=asc"
        f"&start={date_from.isoformat()}"
        f"&end={date_to.isoformat()}&length=500"
    )
    prices: dict[str, float] = {}
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url)
        response.raise_for_status()
        data = response.json()
        for entry in data.get("response", {}).get("data", []):
            try:
                prices[entry["period"]] = float(entry["value"])
            except (KeyError, ValueError, TypeError):
                continue
    logger.info("EIA: fetched %d Brent records", len(prices))
    return prices


def _fetch_eur_usd_rates(date_from: date, date_to: date) -> dict[str, float]:
    """Fetch EUR/USD exchange rates from ECB."""
    url = (
        "https://data-api.ecb.europa.eu/service/data/"
        f"EXR/D.USD.EUR.SP00.A?format=csvdata"
        f"&startPeriod={date_from.isoformat()}"
        f"&endPeriod={date_to.isoformat()}"
    )
    rates: dict[str, float] = {}
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url)
        response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.text))
        for row in reader:
            try:
                d = row.get("TIME_PERIOD", "").strip()
                v = float(row.get("OBS_VALUE", "0").strip())
                if d and v > 0:
                    rates[d] = v
            except (ValueError, KeyError):
                continue
    return rates


def fetch_brent_prices(date_from: date, date_to: date) -> list[dict]:
    """Fetch daily Brent crude prices in EUR/litre.

    Strategy: CrudePriceAPI (near real-time) + EIA (historical fallback).
    Merges both sources, CrudePriceAPI wins on overlapping dates.
    Converts USD/barrel to EUR/litre using ECB exchange rates.
    """
    brent_usd: dict[str, float] = {}

    # 1. EIA for historical data (reliable but ~8 day delay)
    try:
        brent_usd.update(_fetch_brent_usd_eia(date_from, date_to))
    except Exception as exc:
        logger.warning("EIA fetch failed: %s", exc)

    # 2. CrudePriceAPI for recent data (overwrites EIA on overlap)
    try:
        brent_usd.update(_fetch_brent_usd_crude_api())
    except Exception as exc:
        logger.warning("CrudePriceAPI fetch failed: %s", exc)

    if not brent_usd:
        return []

    # 3. EUR/USD rates from ECB
    eur_usd_rates: dict[str, float] = {}
    try:
        eur_usd_rates = _fetch_eur_usd_rates(date_from, date_to)
    except Exception as exc:
        logger.warning("ECB EUR/USD fetch failed: %s", exc)

    # 4. Convert USD/barrel to EUR/litre
    # 1 barrel = 158.987 litres
    results: list[dict] = []
    for date_str, usd_price in brent_usd.items():
        eur_rate = eur_usd_rates.get(date_str, 1.08)
        price_eur_per_litre = (usd_price / 158.987) / eur_rate
        results.append({
            "date": date_str,
            "price_eur": round(price_eur_per_litre, 4),
            "price_usd": round(usd_price, 2),
        })

    return sorted(results, key=lambda x: x["date"])


def ingest_brent(
    con: duckdb.DuckDBPyConnection, date_from: date, date_to: date
) -> int:
    """Fetch and store Brent prices in the database."""
    prices = fetch_brent_prices(date_from, date_to)
    if not prices:
        logger.warning("No Brent prices fetched for %s to %s", date_from, date_to)
        return 0

    for row in prices:
        con.execute(
            "INSERT OR REPLACE INTO brent_prices (date, price_eur, price_usd) "
            "VALUES (CAST(? AS DATE), ?, ?)",
            [row["date"], row["price_eur"], row["price_usd"]],
        )

    logger.info("Ingested %d Brent price records", len(prices))
    return len(prices)
