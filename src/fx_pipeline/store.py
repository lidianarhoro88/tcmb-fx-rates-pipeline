"""DuckDB storage with idempotent upserts and an ingestion log (the watermark)."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from .source import Rate

DDL = """
create table if not exists fx_rates (
    rate_date        date    not null,
    currency         varchar not null,
    bulletin_no      varchar,
    unit             integer not null,
    currency_name    varchar,
    forex_buying     double,
    forex_selling    double,
    banknote_buying  double,
    banknote_selling double,
    try_per_unit     double,
    loaded_at        timestamp default current_timestamp,
    primary key (rate_date, currency)
);
create table if not exists ingestion_log (
    rate_date  date primary key,
    status     varchar not null,          -- loaded | no_bulletin | failed
    rows       integer,
    message    varchar,
    run_at     timestamp default current_timestamp
);
"""


class Store:
    def __init__(self, path: str | Path):
        self.con = duckdb.connect(str(path))
        self.con.execute(DDL)

    def restore_from_csv(self, rates_csv: Path, log_csv: Path) -> bool:
        """Rebuild state from the CSVs committed to the repo (CI runners start with no database)."""
        if self.con.execute("select count(*) from ingestion_log").fetchone()[0] or not log_csv.exists():
            return False
        if rates_csv.exists():
            self.con.execute(f"""insert into fx_rates (rate_date, currency, bulletin_no, unit, currency_name, forex_buying, forex_selling,
                                 banknote_buying, banknote_selling, try_per_unit)
                                 select * from read_csv('{rates_csv}', header = true)""")
        self.con.execute(f"insert into ingestion_log (rate_date, status, rows, message) select rate_date, status, rows, message from read_csv('{log_csv}', header = true, all_varchar = false)")
        return True

    def upsert(self, rates: list[Rate]) -> int:
        if not rates:
            return 0
        self.con.executemany(
            """insert or replace into fx_rates (rate_date, currency, bulletin_no, unit, currency_name, forex_buying,
               forex_selling, banknote_buying, banknote_selling, try_per_unit) values (?,?,?,?,?,?,?,?,?,?)""",
            [(r.rate_date, r.currency, r.bulletin_no, r.unit, r.name, r.forex_buying, r.forex_selling,
              r.banknote_buying, r.banknote_selling, r.try_per_unit) for r in rates])
        return len(rates)

    def log(self, d: date, status: str, rows: int = 0, message: str = "") -> None:
        self.con.execute("insert or replace into ingestion_log (rate_date, status, rows, message) values (?,?,?,?)",
                         [d, status, rows, message])

    def last_attempted_date(self) -> date | None:
        """Watermark: the latest date already settled (loaded or confirmed to have no bulletin)."""
        r = self.con.execute("select max(rate_date) from ingestion_log where status in ('loaded', 'no_bulletin')").fetchone()
        return r[0] if r else None

    def settled(self, d: date) -> bool:
        return self.con.execute("select 1 from ingestion_log where rate_date = ? and status in ('loaded','no_bulletin')", [d]).fetchone() is not None
