import json
from datetime import date
from pathlib import Path

import pytest

from fx_pipeline.pipeline import business_days, export, ingest, latest_publishable_day
from fx_pipeline.quality import run_checks
from fx_pipeline.source import NoBulletin, Rate, parse_bulletin, url_for
from fx_pipeline.store import Store

FIX = Path(__file__).parent / "fixtures"


def fake_fetch(d: date) -> bytes:
    """Serve fixtures; 29 Oct (Republic Day) and any other missing day behave like a CBRT 404."""
    f = FIX / f"{d:%d%m%Y}.xml"
    if not f.exists():
        raise NoBulletin(d)
    return f.read_bytes()


@pytest.fixture
def store(tmp_path):
    return Store(tmp_path / "fx.duckdb")


def test_parse_bulletin():
    rates = {r.currency: r for r in parse_bulletin((FIX / "27102026.xml").read_bytes())}
    usd = rates["USD"]
    assert usd.rate_date == date(2026, 10, 27) and usd.bulletin_no == "2026/201"
    assert usd.forex_buying == 40.0 and usd.name == "US Dollar"
    assert rates["JPY"].unit == 100 and rates["JPY"].try_per_unit == pytest.approx(rates["JPY"].forex_selling / 100)
    assert rates["XDR"].forex_selling is None and rates["XDR"].try_per_unit is None


def test_urls():
    assert url_for(date(2026, 10, 27)) == "https://www.tcmb.gov.tr/kurlar/202610/27102026.xml"
    assert url_for(date(2026, 11, 2), today=date(2026, 11, 2)).endswith("/today.xml")


def test_business_days_skip_weekends():
    days = list(business_days(date(2026, 10, 30), date(2026, 11, 2)))     # Fri .. Mon
    assert days == [date(2026, 10, 30), date(2026, 11, 2)]


def test_publication_cutoff():
    from datetime import datetime
    from fx_pipeline.pipeline import ISTANBUL
    assert latest_publishable_day(datetime(2026, 11, 2, 15, 30, tzinfo=ISTANBUL)) == date(2026, 11, 1)
    assert latest_publishable_day(datetime(2026, 11, 2, 16, 0, tzinfo=ISTANBUL)) == date(2026, 11, 2)


def test_ingest_holiday_and_idempotency(store):
    days = list(business_days(date(2026, 10, 27), date(2026, 10, 30)))
    first = ingest(store, days, fetch=fake_fetch)
    assert first == {"loaded": 3, "no_bulletin": 1, "failed": 0, "skipped": 0, "rows": 15}
    second = ingest(store, days, fetch=fake_fetch)
    assert second["skipped"] == 4 and second["loaded"] == 0
    assert store.con.execute("select count(*) from fx_rates").fetchone()[0] == 15
    assert store.last_attempted_date() == date(2026, 10, 30)


def test_failed_days_are_retried(store):
    calls = {"n": 0}

    def flaky(d):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError("network down")
        return fake_fetch(d)

    assert ingest(store, [date(2026, 10, 27)], fetch=flaky)["failed"] == 1
    assert not store.settled(date(2026, 10, 27))
    assert ingest(store, [date(2026, 10, 27)], fetch=flaky)["loaded"] == 1


def test_upsert_replaces_corrected_bulletin(store):
    ingest(store, [date(2026, 10, 27)], fetch=fake_fetch)
    r = parse_bulletin(fake_fetch(date(2026, 10, 27)))[0]
    corrected = Rate(**{**r.__dict__, "forex_selling": 41.0})
    store.upsert([corrected])
    assert store.con.execute("select forex_selling, count(*) over () from fx_rates where currency='USD'").fetchone() == (41.0, 1)


def test_quality_checks(store):
    ingest(store, list(business_days(date(2026, 10, 27), date(2026, 10, 30))), fetch=fake_fetch)
    assert run_checks(store.con) == []
    store.con.execute("update fx_rates set forex_selling = 70, try_per_unit = 70 where currency = 'USD' and rate_date = '2026-10-30'")
    problems = run_checks(store.con)
    assert any("moved" in p and "USD" in p for p in problems)
    store.con.execute("update fx_rates set forex_selling = 1 where currency = 'EUR' and rate_date = '2026-10-30'")
    assert any("selling rate below buying" in p for p in run_checks(store.con))


def test_export(store, tmp_path):
    ingest(store, list(business_days(date(2026, 10, 27), date(2026, 10, 30))), fetch=fake_fetch)
    export(store, tmp_path / "fx.csv", tmp_path / "data.json")
    d = json.loads((tmp_path / "data.json").read_text())
    assert d["latest_date"] == "2026-10-30" and d["bulletins"] == 3 and d["holidays_seen"] == 1
    assert [x["currency"] for x in d["latest"]][:3] == ["USD", "EUR", "GBP"]
    assert d["latest"][0]["chg_1d"] == pytest.approx(40.28 / 40.18 - 1, abs=1e-4)
    assert len(d["series"]["USD"]) == 3
    assert (tmp_path / "fx.csv").read_text().count("\n") == 16


def test_state_survives_via_csv(store, tmp_path):
    ingest(store, list(business_days(date(2026, 10, 27), date(2026, 10, 30))), fetch=fake_fetch)
    export(store, tmp_path / "fx_rates.csv", tmp_path / "data.json")
    fresh = Store(tmp_path / "new.duckdb")
    assert fresh.restore_from_csv(tmp_path / "fx_rates.csv", tmp_path / "ingestion_log.csv")
    assert fresh.con.execute("select count(*) from fx_rates").fetchone()[0] == 15
    assert fresh.last_attempted_date() == date(2026, 10, 30)
    assert ingest(fresh, [date(2026, 10, 29)], fetch=fake_fetch)["skipped"] == 1   # holiday remembered
