"""Data quality checks run after every load. Each returns a list of human-readable problems."""
from __future__ import annotations

import duckdb

CORE = ("USD", "EUR", "GBP")


def run_checks(con: duckdb.DuckDBPyConnection, max_daily_move: float = 0.15) -> list[str]:
    problems: list[str] = []
    for d, c in con.execute("""select rate_date, currency from fx_rates
                               where forex_selling < forex_buying or banknote_selling < banknote_buying""").fetchall():
        problems.append(f"{d} {c}: selling rate below buying rate")
    for d, c in con.execute("select rate_date, currency from fx_rates where forex_selling <= 0 or forex_buying <= 0").fetchall():
        problems.append(f"{d} {c}: non-positive rate")
    for (d,) in con.execute(f"""select rate_date from fx_rates group by rate_date
                                having count(*) filter (where currency in {CORE}) < {len(CORE)}""").fetchall():
        problems.append(f"{d}: a core currency (USD, EUR, GBP) is missing")
    for d, c, chg in con.execute(f"""
        select rate_date, currency, chg from (
            select rate_date, currency,
                   try_per_unit / lag(try_per_unit) over (partition by currency order by rate_date) - 1 as chg
            from fx_rates)
        where abs(chg) > {max_daily_move}""").fetchall():
        problems.append(f"{d} {c}: moved {chg:+.1%} in one bulletin, check before use")
    return problems
