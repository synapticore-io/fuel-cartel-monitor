"""CLI entry point for fuel-price-monitor."""
import argparse
import json
import logging
import statistics
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from fuel_price_monitor import analysis
from fuel_price_monitor.brent import ingest_brent
from fuel_price_monitor.db import get_connection
from fuel_price_monitor.ingest import (
    ingest_date_range,
    ingest_latest,
    ingest_prices_api,
    ingest_stations_api,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def cmd_ingest(args: argparse.Namespace) -> None:
    """Handle the 'ingest' subcommand."""
    con = get_connection()

    if args.api_stations:
        count = ingest_stations_api(con, lat=args.lat, lng=args.lng, radius_km=args.radius)
        print(json.dumps({"stations_ingested": count}))
        return

    if args.api_prices:
        count = ingest_prices_api(con)
        print(json.dumps({"prices_ingested": count}))
        return

    if args.brent:
        d_from = (
            date.fromisoformat(args.date_from)
            if args.date_from
            else date.today() - timedelta(days=90)
        )
        d_to = date.fromisoformat(args.date_to) if args.date_to else date.today()
        count = ingest_brent(con, d_from, d_to)
        print(json.dumps({"brent_records": count}))
        return

    if args.latest:
        result = ingest_latest(con)
        print(json.dumps(result, indent=2, default=str))
        return

    today = date.today()

    if args.days:
        date_from = today - timedelta(days=args.days - 1)
        date_to = today - timedelta(days=1)
    elif args.date_from and args.date_to:
        date_from = date.fromisoformat(args.date_from)
        date_to = date.fromisoformat(args.date_to)
    elif args.date_from:
        date_from = date.fromisoformat(args.date_from)
        date_to = today - timedelta(days=1)
    else:
        print(
            "Error: specify --latest, --days N, or --from DATE [--to DATE]",
            file=sys.stderr,
        )
        sys.exit(1)

    result = ingest_date_range(con, date_from, date_to)
    print(json.dumps(result, indent=2, default=str))


def _resolve_window(args: argparse.Namespace) -> tuple[date, date, str]:
    """Return (date_from, date_to_exclusive, label) from --month or --days."""
    month = getattr(args, "month", None)
    if month:
        start, end = analysis._month_bounds(month)
        return start, end, month
    days = getattr(args, "days", 30) or 30
    today = date.today()
    return today - timedelta(days=days), today + timedelta(days=1), f"last-{days}d"


def cmd_analyze(args: argparse.Namespace) -> None:
    """Handle the 'analyze' subcommand."""
    con = get_connection()
    df, dt, _ = _resolve_window(args)

    if args.type == "leader-follower":
        results = analysis.leader_follower_lag(
            con, args.lat, args.lng, df, dt,
            radius_km=args.radius, fuel_type=args.fuel,
        )
        print(json.dumps([vars(r) for r in results], indent=2))

    elif args.type == "rockets-feathers":
        results = analysis.rockets_and_feathers(
            con, args.lat, args.lng, df, dt,
            radius_km=args.radius, fuel_type=args.fuel,
        )
        print(json.dumps([vars(r) for r in results], indent=2))

    elif args.type == "sync":
        result = analysis.price_sync_index(
            con, args.lat, args.lng, df, dt,
            radius_km=args.radius, fuel_type=args.fuel,
        )
        print(json.dumps(result, indent=2))

    elif args.type == "brent-decoupling":
        results = analysis.brent_decoupling(con, df, dt, fuel_type=args.fuel)
        print(json.dumps([vars(r) for r in results], indent=2))

    elif args.type == "regional":
        results = analysis.regional_comparison(
            con, fuel_type=args.fuel,
            date_from=df.isoformat(), date_to=dt.isoformat(),
        )
        print(json.dumps(results, indent=2))

    elif args.type == "breakdown":
        result = analysis.price_breakdown(con, args.fuel, df, dt)
        print(json.dumps(result, indent=2))

    else:
        print(f"Error: unknown analysis type: {args.type!r}", file=sys.stderr)
        sys.exit(1)


def cmd_serve(_args: argparse.Namespace) -> None:
    """Handle the 'serve' subcommand — start the MCP server."""
    from fuel_price_monitor.server import main

    main()


def cmd_stats(_args: argparse.Namespace) -> None:
    """Handle the 'stats' subcommand."""
    con = get_connection()
    stats = analysis.database_stats(con)
    print(json.dumps(stats, indent=2, default=str))


def _spread_anomaly_summary(series: list[dict]) -> dict:
    """Summarize spread-anomaly statistics from a brent_decoupling series.

    Counts days where the retail-minus-Brent spread deviates >2σ above the
    rolling 30-day mean (the `is_abnormal` flag from the SQL macro). Also
    reports the largest z-score, its date, and the period's spread mean/σ.
    Defensible framing: "abnormal days out of N" instead of point-to-point
    retail rise minus point-to-point Brent rise.
    """
    if not series:
        return {}
    abnormal_days = sum(1 for r in series if r["is_abnormal"])
    period_days = len(series)
    max_z = max(series, key=lambda r: r["spread_z_score"])
    spreads = [r["spread"] for r in series]
    mean_spread = statistics.mean(spreads)
    stddev_spread = statistics.stdev(spreads) if len(spreads) > 1 else 0.0
    return {
        "period_days": period_days,
        "abnormal_days": abnormal_days,
        "abnormal_pct": round(abnormal_days / period_days * 100, 1),
        "max_z_score": round(max_z["spread_z_score"], 2),
        "max_z_date": max_z["date"],
        "threshold_z": 2.0,
        "mean_spread_cents": round(mean_spread * 100, 1),
        "stddev_spread_cents": round(stddev_spread * 100, 1),
    }


def cmd_export(args: argparse.Namespace) -> None:
    """Export dashboard data as JSON. With --month writes dashboard-YYYY-MM.json + index."""
    con = get_connection()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)

    regions = [
        {"name": "Hannover", "lat": 52.37, "lng": 9.73},
        {"name": "Hamburg", "lat": 53.55, "lng": 9.99},
        {"name": "Berlin", "lat": 52.52, "lng": 13.41},
        {"name": "München", "lat": 48.14, "lng": 11.58},
        {"name": "Köln", "lat": 50.94, "lng": 6.96},
    ]

    radius = args.radius
    fuel = args.fuel
    df, dt, label = _resolve_window(args)
    month = getattr(args, "month", None)

    dashboard = {
        "generated_at": datetime.now().isoformat(),
        "parameters": {
            "radius_km": radius,
            "fuel_type": fuel,
            "date_from": df.isoformat(),
            "date_to": dt.isoformat(),
            "month": month,
            "window_label": label,
        },
        "stats": analysis.database_stats(con),
        "regions": {},
    }

    for region in regions:
        name = region["name"]
        lat, lng = region["lat"], region["lng"]
        logger.info("Exporting %s (lat=%s, lng=%s)", name, lat, lng)
        lf = analysis.leader_follower_lag(con, lat, lng, df, dt, radius_km=radius, fuel_type=fuel)
        rf = analysis.rockets_and_feathers(con, lat, lng, df, dt, radius_km=radius, fuel_type=fuel)
        dashboard["regions"][name] = {
            "lat": lat,
            "lng": lng,
            "leader_follower": [vars(r) for r in lf],
            "rockets_feathers": [vars(r) for r in rf],
        }

    # best_time, brand_ranking, consumer_impact: pro Kraftstoff exportieren,
    # damit der Fuel-Switcher im Dashboard alle Charts sauber umschaltet
    dashboard["best_time"] = {
        ft: analysis.best_time_to_tank(con, ft, df, dt) for ft in ("diesel", "e5")
    }
    dashboard["brand_ranking"] = {
        ft: analysis.brand_ranking(con, ft, date_from=df, date_to=dt) for ft in ("diesel", "e5")
    }
    dashboard["consumer_impact"] = {
        ft: analysis.consumer_impact(con, ft, date_from=df, date_to=dt) for ft in ("diesel", "e5")
    }

    for ft in ["diesel", "e5"]:
        decoupling = analysis.brent_decoupling(con, df, dt, fuel_type=ft)
        dashboard[f"brent_decoupling_{ft}"] = [vars(r) for r in decoupling]

    dashboard["spread_anomaly"] = {
        "diesel": _spread_anomaly_summary(dashboard["brent_decoupling_diesel"]),
        "e5": _spread_anomaly_summary(dashboard["brent_decoupling_e5"]),
    }

    dashboard["price_breakdown"] = {
        "diesel": analysis.price_breakdown(con, "diesel", df, dt),
        "e5": analysis.price_breakdown(con, "e5", df, dt),
        "constants": {
            "energy_tax_diesel_eur": analysis.ENERGY_TAX_DIESEL_EUR,
            "energy_tax_e5_eur": analysis.ENERGY_TAX_E5_EUR,
            "co2_price_eur_per_ton": analysis.CO2_PRICE_EUR_PER_TON,
            "co2_price_source": analysis.CO2_PRICE_SOURCE,
            "co2_kg_per_liter_diesel": analysis.CO2_KG_PER_LITER_DIESEL,
            "co2_kg_per_liter_e5": analysis.CO2_KG_PER_LITER_E5,
            "vat_rate": analysis.VAT_RATE,
        },
    }

    filename = f"dashboard-{month}.json" if month else "dashboard.json"
    data_path = out / filename
    data_path.write_text(json.dumps(dashboard, indent=2, default=str), encoding="utf-8")
    logger.info("Dashboard data exported to %s", data_path)

    if month:
        _update_index(out)

    print(json.dumps({"exported_to": str(data_path), "regions": len(regions), "month": month}))


def _update_index(out_dir: Path) -> None:
    """Scan out_dir for dashboard-YYYY-MM.json files and write index.json listing them."""
    months = sorted(
        p.name.removeprefix("dashboard-").removesuffix(".json")
        for p in out_dir.glob("dashboard-*.json")
    )
    index = {
        "months": months,
        "latest": months[-1] if months else None,
        "updated_at": datetime.now().isoformat(),
    }
    (out_dir / "index.json").write_text(
        json.dumps(index, indent=2), encoding="utf-8"
    )
    logger.info("Updated index.json with %d months", len(months))


def cmd_archive(args: argparse.Namespace) -> None:
    """Dump price_changes and brent_prices for --month as zstd-compressed Parquet."""
    con = get_connection()
    month = args.month
    start, end = analysis._month_bounds(month)
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)

    # DuckDB's COPY ... TO uses forward slashes even on Windows; use as_posix()
    prices_path = (out / f"prices-{month}.parquet").as_posix()
    brent_path = (out / f"brent-{month}.parquet").as_posix()
    stations_path = (out / "stations.parquet").as_posix()

    con.execute(
        f"""COPY (
              SELECT * FROM price_changes
              WHERE CAST(timestamp AS DATE) >= CAST(? AS DATE)
                AND CAST(timestamp AS DATE) <  CAST(? AS DATE)
            ) TO '{prices_path}' (FORMAT PARQUET, COMPRESSION ZSTD)""",
        [str(start), str(end)],
    )
    prices_rows = con.execute(
        "SELECT COUNT(*) FROM price_changes "
        "WHERE CAST(timestamp AS DATE) >= CAST(? AS DATE) "
        "  AND CAST(timestamp AS DATE) <  CAST(? AS DATE)",
        [str(start), str(end)],
    ).fetchone()[0]

    con.execute(
        f"""COPY (
              SELECT * FROM brent_prices
              WHERE date >= CAST(? AS DATE) AND date < CAST(? AS DATE)
            ) TO '{brent_path}' (FORMAT PARQUET, COMPRESSION ZSTD)""",
        [str(start), str(end)],
    )
    brent_rows = con.execute(
        "SELECT COUNT(*) FROM brent_prices WHERE date >= CAST(? AS DATE) AND date < CAST(? AS DATE)",
        [str(start), str(end)],
    ).fetchone()[0]

    # Stations ist (fast) statisch — einmal global genügt, bei jedem archive überschreiben
    con.execute(
        f"""COPY (SELECT * FROM stations) TO '{stations_path}' """
        f"""(FORMAT PARQUET, COMPRESSION ZSTD)"""
    )
    station_rows = con.execute("SELECT COUNT(*) FROM stations").fetchone()[0]

    result = {
        "month": month,
        "prices_file": prices_path,
        "prices_rows": prices_rows,
        "brent_file": brent_path,
        "brent_rows": brent_rows,
        "stations_file": stations_path,
        "station_rows": station_rows,
    }
    print(json.dumps(result, indent=2))


def main() -> None:
    """Main entry point for the CLI."""
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(
        prog="fuel-price-monitor",
        description="Detect oligopolistic pricing patterns in German fuel markets",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- ingest ---
    ingest_parser = subparsers.add_parser("ingest", help="Ingest Tankerkoenig CSV data")
    ingest_parser.add_argument(
        "--latest", action="store_true", help="Ingest the latest available day"
    )
    ingest_parser.add_argument("--from", dest="date_from", metavar="DATE", help="Start date (ISO)")
    ingest_parser.add_argument("--to", dest="date_to", metavar="DATE", help="End date (ISO)")
    ingest_parser.add_argument(
        "--days", type=int, metavar="N", help="Ingest the last N days"
    )
    ingest_parser.add_argument(
        "--api-stations", action="store_true",
        help="Fetch stations near --lat/--lng via Tankerkoenig live API",
    )
    ingest_parser.add_argument(
        "--api-prices", action="store_true",
        help="Snapshot current prices for all stations in DB via live API",
    )
    ingest_parser.add_argument(
        "--brent", action="store_true",
        help="Fetch Brent crude oil prices (use with --from/--to)",
    )
    ingest_parser.add_argument(
        "--lat", type=float, default=52.37, help="Latitude (default: Hannover)"
    )
    ingest_parser.add_argument(
        "--lng", type=float, default=9.73, help="Longitude (default: Hannover)"
    )
    ingest_parser.add_argument(
        "--radius", type=float, default=25.0, help="Radius in km (for --api-stations)"
    )
    ingest_parser.set_defaults(func=cmd_ingest)

    # --- analyze ---
    analyze_parser = subparsers.add_parser("analyze", help="Run an analysis")
    analyze_parser.add_argument(
        "type",
        choices=[
            "leader-follower", "rockets-feathers", "sync",
            "brent-decoupling", "regional", "breakdown",
        ],
        help="Analysis type",
    )
    analyze_parser.add_argument(
        "--lat", type=float, default=52.37, help="Latitude (default: Hannover)"
    )
    analyze_parser.add_argument(
        "--lng", type=float, default=9.73, help="Longitude (default: Hannover)"
    )
    analyze_parser.add_argument("--radius", type=float, default=25.0, help="Radius in km")
    analyze_parser.add_argument(
        "--fuel", choices=["diesel", "e5", "e10"], default="e5", help="Fuel type"
    )
    analyze_parser.add_argument("--days", type=int, default=30, help="Lookback days")
    analyze_parser.add_argument(
        "--month", metavar="YYYY-MM", help="Restrict to a specific month (overrides --days)"
    )
    analyze_parser.set_defaults(func=cmd_analyze)

    # --- serve ---
    serve_parser = subparsers.add_parser("serve", help="Start the MCP server")
    serve_parser.set_defaults(func=cmd_serve)

    # --- stats ---
    stats_parser = subparsers.add_parser("stats", help="Show database statistics")
    stats_parser.set_defaults(func=cmd_stats)

    # --- export ---
    export_parser = subparsers.add_parser("export", help="Export analysis as JSON for dashboard")
    export_parser.add_argument(
        "--output", default="docs/data", help="Output directory (default: docs/data)"
    )
    export_parser.add_argument("--radius", type=float, default=25.0, help="Radius in km")
    export_parser.add_argument(
        "--fuel", choices=["diesel", "e5", "e10"], default="e5", help="Fuel type"
    )
    export_parser.add_argument("--days", type=int, default=30, help="Lookback days")
    export_parser.add_argument(
        "--month", metavar="YYYY-MM",
        help="Export a specific month as dashboard-YYYY-MM.json and refresh index.json",
    )
    export_parser.set_defaults(func=cmd_export)

    # --- archive ---
    archive_parser = subparsers.add_parser(
        "archive", help="Dump a month's price_changes + brent_prices as Parquet archives"
    )
    archive_parser.add_argument(
        "--month", metavar="YYYY-MM", required=True, help="Month to archive"
    )
    archive_parser.add_argument(
        "--output", default="data/archive", help="Output directory (default: data/archive)"
    )
    archive_parser.set_defaults(func=cmd_archive)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
