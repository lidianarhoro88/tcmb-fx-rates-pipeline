"""Fetch and parse CBRT daily exchange-rate bulletins.

The CBRT publishes one XML bulletin per business day at about 15:30 Istanbul time:
  today:    https://www.tcmb.gov.tr/kurlar/today.xml
  any day:  https://www.tcmb.gov.tr/kurlar/YYYYMM/DDMMYYYY.xml
Weekends and public holidays have no bulletin (HTTP 404). Some currencies are
quoted per 100 units (JPY), and banknote rates are blank for currencies the
bank does not trade in cash.
"""
from __future__ import annotations

import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime

BASE = "https://www.tcmb.gov.tr/kurlar"


class NoBulletin(Exception):
    """No bulletin exists for this date (weekend or public holiday)."""


@dataclass(frozen=True)
class Rate:
    rate_date: date
    bulletin_no: str
    currency: str
    unit: int
    name: str
    forex_buying: float | None
    forex_selling: float | None
    banknote_buying: float | None
    banknote_selling: float | None

    @property
    def try_per_unit(self) -> float | None:
        """Forex selling rate for ONE unit of the currency (JPY is quoted per 100)."""
        return None if self.forex_selling is None else self.forex_selling / self.unit


def url_for(d: date, today: date | None = None) -> str:
    if today is not None and d == today:
        return f"{BASE}/today.xml"
    return f"{BASE}/{d:%Y%m}/{d:%d%m%Y}.xml"


def fetch_xml(d: date, retries: int = 3, timeout: int = 20) -> bytes:
    """Download the bulletin for date d. Raises NoBulletin on 404, retries transient errors."""
    req = urllib.request.Request(url_for(d), headers={"User-Agent": "tcmb-fx-rates-pipeline/1.0 (+github.com/lidianarhoro88)"})
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise NoBulletin(d) from e
            if attempt == retries:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == retries:
                raise
        time.sleep(2 ** attempt)
    raise RuntimeError("unreachable")


def _nice_name(raw: str) -> str:
    """'US DOLLAR' → 'US Dollar', 'SPECIAL DRAWING RIGHT (SDR)' → 'Special Drawing Right (SDR)'."""
    keep = {"US", "SDR", "(SDR)", "UAE"}
    return " ".join(w if w in keep else w.capitalize() for w in raw.split())


def _num(el: ET.Element, tag: str) -> float | None:
    t = el.findtext(tag)
    return float(t) if t and t.strip() else None


def parse_bulletin(xml: bytes) -> list[Rate]:
    root = ET.fromstring(xml)
    rate_date = datetime.strptime(root.attrib["Tarih"], "%d.%m.%Y").date()
    bulletin = root.attrib.get("Bulten_No", "")
    rates = []
    for c in root.findall("Currency"):
        rates.append(Rate(
            rate_date=rate_date, bulletin_no=bulletin, currency=c.attrib["CurrencyCode"],
            unit=int(c.findtext("Unit") or 1), name=_nice_name((c.findtext("CurrencyName") or "").strip()),
            forex_buying=_num(c, "ForexBuying"), forex_selling=_num(c, "ForexSelling"),
            banknote_buying=_num(c, "BanknoteBuying"), banknote_selling=_num(c, "BanknoteSelling"),
        ))
    return rates
