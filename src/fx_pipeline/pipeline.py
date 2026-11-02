"""Command line entry point.

    fx-pipeline backfill --start 2026-01-01 [--end 2026-11-02]
    fx-pipeline daily            # incremental: every unsettled day from the watermark up to today
    fx-pipeline export           # data/fx_rates.csv + docs/data.json for the dashboard
    fx-pipeline check            # data quality checks, exit 1 on problems

Business days are attempted; weekends are skipped without a request. A 404 is
recorded as "no_bulletin" (public holiday), so it is never fetched again.
Bulletins are published around 15:30 Istanbul time, so "today" is only
attempted after 15:45 Istanbul time.
"""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from .quality import run_checks
from .source import NoBulletin, fetch_xml, parse_bulletin
from .store import Store

ROOT = Path.cwd()
ISTANBUL = timezone(timedelta(hours=3))
DEFAULT_START = date(2026, 1, 1)


def istanbul_now() -> datetime:
    return datetime.now(ISTANBUL)


def latest_publishable_day(now: datetime) -> date:
    return now.date() if (now.hour, now.minute) >= (15, 45) else now.date() - timedelta(days=1)


def business_days(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def ingest(store: Store, days, fetch: Callable[[date], bytes] = fetch_xml, max_consecutive_failures: int = 5) -> dict[str, int]:
    stats = {"loaded": 0, "no_bulletin": 0, "failed": 0, "skipped": 0, "rows": 0}
    consecutive_failures = 0
    for d in days:
        if consecutive_failures >= max_consecutive_failures:
            print(f"stopping after {consecutive_failures} failures in a row; the remaining days are retried next run")
            break
        if store.settled(d):
            stats["skipped"] += 1
            continue
        try:
            rates = parse_bulletin(fetch(d))
            n = store.upsert(rates)
            store.log(d, "loaded", n)
            print(f"  {d} loaded {n} rates", flush=True)
            stats["loaded"] += 1
            stats["rows"] += n
            consecutive_failures = 0
        except NoBulletin:
            store.log(d, "no_bulletin")
            print(f"  {d} no bulletin (holiday)", flush=True)
            stats["no_bulletin"] += 1
            consecutive_failures = 0
        except Exception as e:  # keep going; failed days are retried on the next run
            store.log(d, "failed", message=str(e)[:300])
            print(f"  {d} FAILED: {e}", flush=True)
            stats["failed"] += 1
            consecutive_failures += 1
    return stats


def export(store: Store, csv_path: Path, json_path: Path) -> None:
    con = store.con
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"copy (select * exclude (loaded_at) from fx_rates order by rate_date, currency) to '{csv_path}' (header, delimiter ',')")
    con.execute(f"copy (select rate_date, status, rows, message from ingestion_log order by rate_date) to '{csv_path.with_name('ingestion_log.csv')}' (header, delimiter ',')")
    series = con.execute("""
        select currency, strftime(rate_date, '%Y-%m-%d'), round(try_per_unit, 4)
        from fx_rates where currency in ('USD', 'EUR', 'GBP') and rate_date >= (select max(rate_date) - 400 from fx_rates)
        order by rate_date""").fetchall()
    latest = con.execute("""
        with l as (select max(rate_date) d from fx_rates),
        cur as (select * from fx_rates where rate_date = (select d from l)),
        prev as (select currency, try_per_unit p from fx_rates where rate_date = (select max(rate_date) from fx_rates where rate_date < (select d from l))),
        m30 as (select currency, try_per_unit p from fx_rates qualify row_number() over (partition by currency order by abs(date_diff('day', rate_date, (select d from l) - 30))) = 1),
        y0 as (select currency, try_per_unit p from fx_rates where rate_date = (select min(rate_date) from fx_rates where year(rate_date) = year((select d from l))))
        select cur.currency, cur.currency_name, cur.unit, cur.forex_buying, cur.forex_selling, cur.banknote_buying, cur.banknote_selling,
               round(cur.try_per_unit / prev.p - 1, 5), round(cur.try_per_unit / m30.p - 1, 5), round(cur.try_per_unit / y0.p - 1, 5)
        from cur left join prev using (currency) left join m30 using (currency) left join y0 using (currency)
        order by case cur.currency when 'USD' then 0 when 'EUR' then 1 when 'GBP' then 2 else 3 end, cur.currency""").fetchall()
    meta = con.execute("""select strftime(max(rate_date), '%Y-%m-%d'), count(distinct rate_date), count(*),
                                 (select count(*) from ingestion_log where status = 'no_bulletin'),
                                 (select count(*) from ingestion_log where status = 'failed')
                          from fx_rates""").fetchone()
    data = {
        "generated_at": istanbul_now().strftime("%Y-%m-%d %H:%M"),
        "latest_date": meta[0], "bulletins": meta[1], "rows": meta[2], "holidays_seen": meta[3], "failed_days": meta[4],
        "series": {c: [[d, v] for cc, d, v in series if cc == c] for c in ("USD", "EUR", "GBP")},
        "latest": [dict(zip(["currency", "name", "unit", "forex_buying", "forex_selling", "banknote_buying", "banknote_selling",
                             "chg_1d", "chg_30d", "chg_ytd"], r)) for r in latest],
        "quality_problems": run_checks(con),
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="fx-pipeline", description="CBRT exchange rate pipeline")
    ap.add_argument("--db", default="data/fx.duckdb", type=Path)
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("backfill")
    b.add_argument("--start", type=date.fromisoformat, required=True)
    b.add_argument("--end", type=date.fromisoformat)
    sub.add_parser("daily")
    sub.add_parser("export")
    sub.add_parser("check")
    a = ap.parse_args(argv)

    a.db.parent.mkdir(parents=True, exist_ok=True)
    store = Store(a.db)
    if store.restore_from_csv(Path("data/fx_rates.csv"), Path("data/ingestion_log.csv")):
        print("restored state from data/*.csv")
    last_day = latest_publishable_day(istanbul_now())
    if a.cmd == "backfill":
        stats = ingest(store, business_days(a.start, min(a.end or last_day, last_day)))
        print(f"backfill: {stats}")
    elif a.cmd == "daily":
        wm = store.last_attempted_date()
        start = (wm + timedelta(days=1)) if wm else DEFAULT_START
        # also retry any failed days from the last two weeks
        retry = [r[0] for r in store.con.execute("select rate_date from ingestion_log where status = 'failed' and rate_date >= ?",
                                                  [last_day - timedelta(days=14)]).fetchall()]
        stats = ingest(store, sorted(set(retry) | set(business_days(start, last_day))))
        print(f"daily: watermark {wm} → {last_day}: {stats}")
    elif a.cmd == "export":
        export(store, Path("data/fx_rates.csv"), Path("docs/data.json"))
        print("exported data/fx_rates.csv and docs/data.json")
    elif a.cmd == "check":
        problems = run_checks(store.con)
        for p in problems:
            print("  -", p)
        print(f"{len(problems)} quality problems")
        return 1 if problems else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
