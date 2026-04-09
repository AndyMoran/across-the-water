"""
Data fetchers for Across the Water.

Each function fetches one dataset, logs the fetch timestamp, and returns
a tidy DataFrame ready to be saved as parquet.

Fetch timestamps are logged explicitly — backtest integrity constraint.
ENTSO-E notices are sometimes filed late; the backtest uses fetch timestamps,
not event timestamps.
"""
import io
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import timedelta
from pathlib import Path

import requests
import pandas as pd

from .utils import log, utc_now, SAMPLE_START, MAIN_END

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# ENTSO-E
ENTSO_BASE        = "https://web-api.tp.entsoe.eu/api"
ENTSO_DOC_TYPE    = "A77"           # Unavailability of production/generation units
ENTSO_PROCESS_UNPLANNED = "A53"
ENTSO_PROCESS_PLANNED   = "A54"
ENTSO_FR_ZONE     = "10YFR-RTE------C"
ENTSO_NS          = {"ns": "urn:iec62325.351:tc57wg16:451-6:outagedocument:3:0"}
ENTSO_PAGE_SIZE   = 200

# ENTSO-E generation (A75) — German wind
ENTSO_DE_ZONE     = "10Y1001A1001A83F"   # Germany bidding zone
ENTSO_GEN_NS      = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
ENTSO_PSR_WIND_ON = "B19"               # Wind onshore
ENTSO_PSR_WIND_OFF= "B18"               # Wind offshore

# Elexon
ELEXON_MID_URL    = "https://data.elexon.co.uk/bmrs/api/v1/datasets/MID"

# NESO
NESO_DATASET_ID   = "8f2fe0af-871c-488d-8bad-960426f24601"
NESO_BASE_URL     = "https://api.neso.energy/dataset"
NESO_YEAR_RESOURCES = {
    2018: "fcb12133-0db0-4f27-a4a5-1669fd9f6d33",
    2019: "dd9de980-d724-415a-b344-d8ae11321432",
    2020: "33ba6857-2a55-479f-9308-e5c4c53d4381",
    2021: "18c69c42-f20d-46f0-84e9-e279045befc6",
    2022: "bb44a1b5-75b1-4db2-8491-257f23385006",
    2023: "bf5ab335-9b40-4ea4-b93a-ab4af7bce003",
    2024: "f6d02c0f-957b-48cb-82ee-09003f2ba759",
    2025: "b2bde559-3455-4021-b179-dfe60c0337b0",
    2026: "8a4a771c-3929-4e56-93ad-cdf13219dea5",
}

# RTE
RTE_BASE          = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
RTE_DATASET       = "eco2mix-national-tr/records"

# Transient HTTP status codes worth retrying
_RETRY_STATUSES   = {429, 500, 502, 503, 504}


# ─────────────────────────────────────────────────────────────────────────────
# Shared HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "across-the-water/1.0 (research)"})


def _get(url: str, params: dict = None, timeout: int = 60) -> requests.Response:
    """
    GET with retry on transient errors only.
    Retries on network timeouts and 429/5xx status codes.
    Fails immediately on 4xx client errors (except 429).
    Uses exponential backoff: 2s, 4s, 8s.
    """
    for attempt in range(4):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            # Fail fast on permanent client errors
            if r.status_code < 400 or r.status_code not in _RETRY_STATUSES:
                r.raise_for_status()
                return r
            # Transient server error — fall through to retry
            log.warning("HTTP %d on attempt %d — retrying", r.status_code, attempt + 1)
        except requests.Timeout:
            log.warning("Timeout on attempt %d — retrying", attempt + 1)
        except requests.ConnectionError:
            log.warning("Connection error on attempt %d — retrying", attempt + 1)

        if attempt == 3:
            r.raise_for_status()  # raise on final attempt

        wait = 2 ** (attempt + 1)
        time.sleep(wait)

    raise RuntimeError("_get: exceeded retries")  # should never reach here


def _stamp() -> str:
    return utc_now().isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# XML helpers (ENTSO-E)
# ─────────────────────────────────────────────────────────────────────────────

def _xtext(el, path: str, ns: dict) -> str:
    node = el.find(path, ns)
    return node.text.strip() if node is not None and node.text else ""


def _xfloat(el, path: str, ns: dict) -> float:
    try:
        return float(_xtext(el, path, ns))
    except ValueError:
        return float("nan")


def _resolution_minutes(res: str) -> int:
    known = {"PT1M": 1, "PT15M": 15, "PT30M": 30, "PT60M": 60}
    if res not in known:
        raise ValueError(f"Unknown ENTSO-E resolution '{res}' — cannot expand safely")
    return known[res]


# ─────────────────────────────────────────────────────────────────────────────
# 1. ENTSO-E — French nuclear unavailability
# ─────────────────────────────────────────────────────────────────────────────

def fetch_entso_unavailability(
    api_key: str,
    start: str = SAMPLE_START,
    end: str = MAIN_END,
) -> pd.DataFrame:
    """
    Fetch French nuclear unavailability notices from ENTSO-E (document A77).

    Hard-won implementation notes:
      - A77 requires BiddingZone_Domain, not in_Domain.
      - Responses are ZIP files — one XML per notice.
      - Namespace: 'outagedocument' (lowercase d).
      - Structure: Available_Period > Point (not Period > Point).
      - Asset tags prefixed with production_RegisteredResource.*.
      - Max 200 notices per request — paginate with offset.
      - Monthly chunks stay under the 200-instance limit.

    Returns one row per notice × time point:
      asset_id, asset_name, unit_mw, unavailable_mw,
      outage_type ('planned'|'unplanned'), fetch_timestamp
    """
    fetch_ts = _stamp()
    log.info("Fetching ENTSO-E FR nuclear unavailability (fetch_ts=%s)", fetch_ts)

    def _parse_notice(xml_text: str) -> list[dict]:
        """Parse one notice XML into a list of point records."""
        root = ET.fromstring(xml_text)
        records = []
        for ts in root.findall("ns:TimeSeries", ENTSO_NS):
            asset_id   = _xtext(ts, "ns:production_RegisteredResource.mRID", ENTSO_NS)
            asset_name = _xtext(ts, "ns:production_RegisteredResource.name", ENTSO_NS)
            unit_mw    = _xfloat(
                ts,
                "ns:production_RegisteredResource.pSRType"
                ".powerSystemResources.nominalP",
                ENTSO_NS,
            )
            biz_type     = _xtext(ts, "ns:businessType", ENTSO_NS)
            outage_type  = "unplanned" if biz_type == ENTSO_PROCESS_UNPLANNED else "planned"

            for period in ts.findall("ns:Available_Period", ENTSO_NS):
                p_start = pd.Timestamp(
                    _xtext(period, "ns:timeInterval/ns:start", ENTSO_NS), tz="UTC"
                )
                res_min = _resolution_minutes(
                    _xtext(period, "ns:resolution", ENTSO_NS)
                )
                for pt in period.findall("ns:Point", ENTSO_NS):
                    pos = int(_xtext(pt, "ns:position", ENTSO_NS) or "1")
                    qty = _xfloat(pt, "ns:quantity", ENTSO_NS)
                    records.append({
                        "asset_id":        asset_id,
                        "asset_name":      asset_name,
                        "unit_mw":         unit_mw,
                        "unavailable_mw":  qty,
                        "datetime_utc":    p_start + timedelta(minutes=res_min * (pos - 1)),
                        "outage_type":     outage_type,
                        "fetch_timestamp": fetch_ts,
                    })
        return records

    def _fetch_month(process_type: str, p_start: str, p_end: str) -> list[dict]:
        """Fetch one month + process type, paginating until ZIP has < PAGE_SIZE files."""
        records = []
        offset  = 0
        while True:
            r = _get(ENTSO_BASE, params={
                "securityToken":      api_key,
                "documentType":       ENTSO_DOC_TYPE,
                "processType":        process_type,
                "BiddingZone_Domain": ENTSO_FR_ZONE,
                "periodStart":        p_start,
                "periodEnd":          p_end,
                "offset":             str(offset),
            })

            if r.content[:2] != b"PK":
                # Non-ZIP response (empty result or unexpected plain XML)
                log.debug("ENTSO-E non-ZIP response at offset %d — stopping", offset)
                break

            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                xml_names = [n for n in z.namelist() if n.endswith(".xml")]
                for name in xml_names:
                    try:
                        records.extend(_parse_notice(z.read(name).decode("utf-8")))
                    except (ET.ParseError, ValueError) as e:
                        log.warning("Skipping malformed notice %s: %s", name, e)

            if len(xml_names) < ENTSO_PAGE_SIZE:
                break   # last page
            offset += ENTSO_PAGE_SIZE

        return records

    start_ts = pd.Timestamp(start)
    end_ts   = pd.Timestamp(end)
    all_records: list[dict] = []
    current = start_ts

    while current < end_ts:
        chunk_end   = min(current + pd.DateOffset(months=1), end_ts)
        p_start_str = current.strftime("%Y%m%d%H%M")
        p_end_str   = chunk_end.strftime("%Y%m%d%H%M")

        for process_type in (ENTSO_PROCESS_UNPLANNED, ENTSO_PROCESS_PLANNED):
            try:
                batch = _fetch_month(process_type, p_start_str, p_end_str)
                all_records.extend(batch)
                log.debug("ENTSO-E %s %s-%s: %d records",
                          process_type, p_start_str, p_end_str, len(batch))
            except requests.HTTPError as e:
                log.warning("ENTSO-E %s %s-%s HTTP error: %s",
                            process_type, p_start_str, p_end_str, e)

        current = chunk_end

    if not all_records:
        log.warning("ENTSO-E returned 0 records — check API key / date range")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df = df.set_index("datetime_utc").sort_index()
    log.info("ENTSO-E: %d rows, %d unique assets", len(df), df["asset_id"].nunique())
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 2. Elexon Insights API — GB DA prices
# ─────────────────────────────────────────────────────────────────────────────

def fetch_elexon_da_prices(
    start: str = SAMPLE_START,
    end: str = MAIN_END,
) -> pd.DataFrame:
    """
    Fetch GB day-ahead prices (MID) from Elexon. Returns daily
    volume-weighted average price (GBP/MWh).

    Hard-won notes:
      - URL must be built as a string — requests encodes colons -> 400.
      - Max 7-day window per request.
    """
    fetch_ts = _stamp()
    log.info("Fetching Elexon GB DA prices (fetch_ts=%s)", fetch_ts)

    frames = []
    current = pd.Timestamp(start, tz="UTC")
    end_ts  = pd.Timestamp(end,   tz="UTC")
    errors  = 0

    while current < end_ts:
        batch_end = min(current + timedelta(days=7), end_ts)
        url = (
            f"{ELEXON_MID_URL}"
            f"?from={current.strftime('%Y-%m-%dT%H:%MZ')}"
            f"&to={batch_end.strftime('%Y-%m-%dT%H:%MZ')}"
        )
        try:
            r = SESSION.get(url, timeout=30)
            r.raise_for_status()
            rows = r.json().get("data", [])
            if rows:
                frames.append(pd.DataFrame(rows))
        except requests.HTTPError as e:
            errors += 1
            log.debug("Elexon batch %s failed: %s", current.date(), e)
        current = batch_end

    if errors:
        log.warning("Elexon: %d batch errors", errors)
    if not frames:
        log.warning("Elexon: no data returned")
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["settlementDate"]).dt.normalize()
    df = df[df["volume"] > 0]

    daily = (
        df.groupby("date")
        .apply(
            lambda x: (x["price"] * x["volume"]).sum() / x["volume"].sum(),
            include_groups=False,
        )
        .rename("gb_da_price")
        .reset_index()
    )
    daily["date"] = pd.to_datetime(daily["date"], utc=True)
    daily = daily.set_index("date").sort_index()
    daily["fetch_timestamp"] = fetch_ts

    log.info("Elexon DA prices: %d daily rows", len(daily))
    return daily


# ─────────────────────────────────────────────────────────────────────────────
# 3. NESO — IFA/IFA2 flows, wind, demand
# ─────────────────────────────────────────────────────────────────────────────

def fetch_neso_historic_demand(
    start: str = SAMPLE_START,
    end: str = MAIN_END,
) -> pd.DataFrame:
    """
    Fetch NESO half-hourly demand, wind, solar, IFA/IFA2 flows.
    One CSV per calendar year; SQL datastore returns 409 — direct download only.
    Date format varies by year; format='mixed' handles both variants.
    """
    fetch_ts   = _stamp()
    log.info("Fetching NESO Historic Demand Data (fetch_ts=%s)", fetch_ts)

    start_year = pd.Timestamp(start).year
    end_year   = pd.Timestamp(end).year
    frames     = []

    for year, resource_id in sorted(NESO_YEAR_RESOURCES.items()):
        if not (start_year <= year <= end_year):
            continue
        url = (
            f"{NESO_BASE_URL}/{NESO_DATASET_ID}"
            f"/resource/{resource_id}/download/demanddata_{year}.csv"
        )
        try:
            r = SESSION.get(url, timeout=60)
            r.raise_for_status()
            frames.append(pd.read_csv(io.StringIO(r.text)))
            log.debug("NESO %d: %d rows", year, len(frames[-1]))
        except requests.HTTPError as e:
            log.warning("NESO %d failed: %s", year, e)

    if not frames:
        log.warning("NESO: no data returned")
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df["datetime_utc"] = (
        pd.to_datetime(df["SETTLEMENT_DATE"], format="mixed", dayfirst=True, utc=True)
        + pd.to_timedelta((df["SETTLEMENT_PERIOD"].astype(int) - 1) * 30, unit="min")
    )
    df = df.rename(columns={
        "ND":                        "demand_actual_mw",
        "EMBEDDED_WIND_GENERATION":  "wind_actual_mw",
        "EMBEDDED_SOLAR_GENERATION": "solar_actual_mw",
        "IFA_FLOW":                  "ifa1_flow_mw",
        "IFA2_FLOW":                 "ifa2_flow_mw",
    })
    df = df.set_index("datetime_utc").sort_index()
    df = df.loc[pd.Timestamp(start, tz="UTC") : pd.Timestamp(end, tz="UTC")]
    df["wind_forecast_mw"]   = float("nan")
    df["demand_forecast_mw"] = float("nan")
    df["fetch_timestamp"]    = fetch_ts

    log.info("NESO historic demand: %d half-hourly rows", len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 4. RTE eCO2mix — FR generation mix + forecasts
# ─────────────────────────────────────────────────────────────────────────────

def fetch_rte_generation(
    start: str = SAMPLE_START,
    end: str = MAIN_END,
) -> pd.DataFrame:
    """
    Fetch RTE eCO2mix national generation (nuclear actual/forecast, exports,
    imports). Paginates until exhausted.
    """
    fetch_ts = _stamp()
    log.info("Fetching RTE eCO2mix (fetch_ts=%s)", fetch_ts)

    url   = f"{RTE_BASE}/{RTE_DATASET}"
    where = (
        f"date_heure >= '{pd.Timestamp(start).strftime('%Y-%m-%dT%H:%M:%S+00:00')}'"
        f" AND date_heure < '{pd.Timestamp(end).strftime('%Y-%m-%dT%H:%M:%S+00:00')}'"
    )
    records, offset, limit = [], 0, 100

    while True:
        try:
            r    = _get(url, params={"where": where, "limit": limit,
                                     "offset": offset, "order_by": "date_heure ASC"})
            rows = r.json().get("results", [])
            if not rows:
                break
            records.extend(rows)
            if len(rows) < limit:
                break
            offset += limit
        except requests.HTTPError as e:
            log.warning("RTE batch at offset %d failed: %s", offset, e)
            break

    if not records:
        log.warning("RTE: no records returned")
        return pd.DataFrame()

    df = pd.DataFrame(records).rename(columns={
        "date_heure":     "datetime_utc",
        "nucleaire":      "fr_nuclear_actual_mw",
        "nucleaire_prev": "fr_nuclear_forecast_mw",
        "exportations":   "fr_exports_mw",
        "importations":   "fr_imports_mw",
    })
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df = df.set_index("datetime_utc").sort_index()

    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["fetch_timestamp"] = fetch_ts
    log.info("RTE eCO2mix: %d rows", len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 5. open-meteo — French temperature
# ─────────────────────────────────────────────────────────────────────────────

def fetch_fr_temperature(
    start: str = SAMPLE_START,
    end: str = MAIN_END,
    lat: float = 46.6,
    lon: float = 2.3,
) -> pd.DataFrame:
    """Daily mean temperature for France (open-meteo archive API)."""
    fetch_ts = _stamp()
    log.info("Fetching FR temperature from open-meteo (fetch_ts=%s)", fetch_ts)

    r    = _get("https://archive-api.open-meteo.com/v1/archive", params={
        "latitude":   lat,  "longitude":  lon,
        "start_date": pd.Timestamp(start).strftime("%Y-%m-%d"),
        "end_date":   pd.Timestamp(end).strftime("%Y-%m-%d"),
        "daily":      "temperature_2m_mean",
        "timezone":   "UTC",
    })
    data = r.json()

    df = pd.DataFrame({
        "date":      pd.to_datetime(data["daily"]["time"], utc=True),
        "fr_temp_c": data["daily"]["temperature_2m_mean"],
    }).set_index("date")

    df["fr_temp_monthly_mean"] = (
        df["fr_temp_c"].groupby(df.index.month).transform(lambda x: x.expanding().mean())
    )
    df["fr_temp_deviation"] = df["fr_temp_c"] - df["fr_temp_monthly_mean"]
    df["fetch_timestamp"]   = fetch_ts

    log.info("FR temperature: %d daily rows", len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 6. EMBER — TTF natural gas spot
# ─────────────────────────────────────────────────────────────────────────────

_EMBER_TTF_URL = (
    "https://ember-energy.org/app/uploads/2024/01/"
    "European_wholesale_electricity_and_gas_prices.csv"
)

def fetch_ttf_spot(
    start: str = SAMPLE_START,
    end: str = MAIN_END,
) -> pd.DataFrame:
    """
    Fetch TTF natural gas spot price via yfinance (TTF=F).
    Returns daily EUR/MWh close price.

    yfinance pulls from Yahoo Finance — no API key required.
    TTF=F is the front-month Dutch TTF futures contract, a standard
    proxy for European gas spot price used throughout the industry.

    Note: Yahoo Finance has occasional gaps on weekends/holidays;
    these are forward-filled (1 day max) since gas prices do not change
    on non-trading days and the regression uses daily data.
    """
    fetch_ts = _stamp()
    log.info("Fetching TTF spot via yfinance (fetch_ts=%s)", fetch_ts)

    try:
        import yfinance as yf
    except ImportError:
        import subprocess, sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "yfinance", "-q"])
        import yfinance as yf

    try:
        raw = yf.download(
            "TTF=F",
            start=pd.Timestamp(start).strftime("%Y-%m-%d"),
            end=pd.Timestamp(end).strftime("%Y-%m-%d"),
            auto_adjust=True,
            progress=False,
        )

        if raw.empty:
            raise ValueError("yfinance returned empty DataFrame for TTF=F")

        # Flatten multi-level columns if present
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        df = raw[["Close"]].rename(columns={"Close": "ttf_spot_eur_mwh"})
        df.index = pd.to_datetime(df.index, utc=True)
        df.index.name = "date"

        # Forward-fill weekend/holiday gaps (1 day max)
        df = df.resample("D").last().ffill(limit=4)

        # Filter to requested range
        df = df[
            (df.index >= pd.Timestamp(start, tz="UTC")) &
            (df.index <  pd.Timestamp(end,   tz="UTC"))
        ]

        df["ttf_is_d1_fallback"] = False
        df["fetch_timestamp"]    = fetch_ts
        df = df.sort_index()

        log.info("TTF spot (yfinance): %d daily rows", len(df))
        return df

    except Exception as e:
        log.warning("yfinance TTF fetch failed: %s", e)

    log.warning(
        "TTF spot: returning empty DataFrame. "
        "Ensure yfinance is installed: pip install yfinance"
    )
    return pd.DataFrame(columns=["ttf_spot_eur_mwh", "ttf_is_d1_fallback", "fetch_timestamp"])


# ─────────────────────────────────────────────────────────────────────────────
# 7. EPEX GB hourly ID settlement prices
# ─────────────────────────────────────────────────────────────────────────────

def fetch_epex_gb_id_hourly(data_dir: Path = None) -> pd.DataFrame:
    """
    Parse EPEX GB hourly ID settlement CSVs from data/epex_gb_id/.
    Files must be downloaded manually from:
      https://www.epexspot.com/en/market-data#trading-results-downloads
    """
    if data_dir is None:
        from .utils import DATA_DIR
        data_dir = DATA_DIR

    epex_dir  = data_dir / "epex_gb_id"
    epex_dir.mkdir(exist_ok=True)
    csv_files = sorted(epex_dir.glob("*.csv"))

    if not csv_files:
        log.warning(
            "No EPEX GB ID CSVs in %s. Download from "
            "https://www.epexspot.com/en/market-data#trading-results-downloads",
            epex_dir,
        )
        return pd.DataFrame(columns=["gb_id_price_gbp_mwh", "fetch_timestamp"])

    fetch_ts = _stamp()
    frames   = []
    for f in csv_files:
        try:
            frames.append(pd.read_csv(f, parse_dates=False))
        except Exception as e:
            log.warning("Could not parse %s: %s", f, e)

    if not frames:
        return pd.DataFrame(columns=["gb_id_price_gbp_mwh", "fetch_timestamp"])

    df = pd.concat(frames, ignore_index=True)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    date_col  = next((c for c in df.columns if "date"  in c), None)
    hour_col  = next((c for c in df.columns if "hour"  in c), None)
    price_col = next((c for c in df.columns if "price" in c), None)

    if not all([date_col, hour_col, price_col]):
        log.warning("EPEX CSV: cannot identify date/hour/price in columns %s", list(df.columns))
        return pd.DataFrame(columns=["gb_id_price_gbp_mwh", "fetch_timestamp"])

    df["datetime_utc"] = (
        pd.to_datetime(df[date_col], utc=True)
        + pd.to_timedelta(df[hour_col].astype(int) - 1, unit="h")
    )
    df = (
        df.rename(columns={price_col: "gb_id_price_gbp_mwh"})
          .set_index("datetime_utc")[["gb_id_price_gbp_mwh"]]
          .sort_index()
    )
    df["gb_id_price_gbp_mwh"] = pd.to_numeric(df["gb_id_price_gbp_mwh"], errors="coerce")
    df["fetch_timestamp"]     = fetch_ts

    log.info("EPEX GB ID hourly: %d rows from %d files", len(df), len(csv_files))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 8. ENTSO-E — German wind generation (onshore + offshore)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_de_wind_generation(
    api_key: str,
    start: str = SAMPLE_START,
    end: str = MAIN_END,
) -> pd.DataFrame:
    """
    Fetch German wind generation (onshore + offshore) from ENTSO-E.
    Document type A75 = Actual generation per production type.
    Process type A16 = Realised.

    Used as a continental renewable proxy in the SCM regression —
    high German wind suppresses continental wholesale prices and
    reduces the marginal cost of French exports, confounding the
    outage -> GB price relationship.

    Implementation notes:
      - Plain XML response (no ZIP).
      - Two TimeSeries per request: inBiddingZone (generation, wanted)
        and outBiddingZone (exports, discarded).
      - Resolution is PT15M — resampled to hourly mean on return.
      - Chunked by month to stay under ENTSO-E instance limits.
      - Onshore (B19) and offshore (B18) fetched separately, summed.

    Returns hourly DataFrame with de_wind_onshore_mw, de_wind_offshore_mw,
    de_wind_mw (combined) and fetch_timestamp.
    """
    fetch_ts = _stamp()
    log.info("Fetching DE wind generation from ENTSO-E (fetch_ts=%s)", fetch_ts)

    def _parse_generation_xml(text: str) -> list[dict]:
        """Parse A75 GL_MarketDocument — return inBiddingZone points only."""
        root    = ET.fromstring(text)
        records = []

        for ts in root.findall("ns:TimeSeries", ENTSO_GEN_NS):
            # Only want inBiddingZone TimeSeries (actual generation)
            if ts.find("ns:inBiddingZone_Domain.mRID", ENTSO_GEN_NS) is None:
                continue

            for period in ts.findall("ns:Period", ENTSO_GEN_NS):
                p_start = pd.Timestamp(
                    _xtext(period, "ns:timeInterval/ns:start", ENTSO_GEN_NS), tz="UTC"
                )
                res_min = _resolution_minutes(
                    _xtext(period, "ns:resolution", ENTSO_GEN_NS)
                )
                for pt in period.findall("ns:Point", ENTSO_GEN_NS):
                    pos = int(_xtext(pt, "ns:position", ENTSO_GEN_NS) or "1")
                    qty = _xfloat(pt, "ns:quantity", ENTSO_GEN_NS)
                    records.append({
                        "datetime_utc": p_start + timedelta(minutes=res_min * (pos - 1)),
                        "mw":           qty,
                    })
        return records

    def _fetch_psr(psr_type: str, p_start: str, p_end: str) -> list[dict]:
        """Fetch one psrType for one month chunk."""
        r = _get(ENTSO_BASE, params={
            "securityToken": api_key,
            "documentType":  "A75",
            "processType":   "A16",
            "in_Domain":     ENTSO_DE_ZONE,
            "psrType":       psr_type,
            "periodStart":   p_start,
            "periodEnd":     p_end,
        })
        return _parse_generation_xml(r.text)

    start_ts = pd.Timestamp(start)
    end_ts   = pd.Timestamp(end)
    onshore_records:  list[dict] = []
    offshore_records: list[dict] = []
    current = start_ts

    while current < end_ts:
        chunk_end   = min(current + pd.DateOffset(months=1), end_ts)
        p_start_str = current.strftime("%Y%m%d%H%M")
        p_end_str   = chunk_end.strftime("%Y%m%d%H%M")

        for psr_type, store in (
            (ENTSO_PSR_WIND_ON,  onshore_records),
            (ENTSO_PSR_WIND_OFF, offshore_records),
        ):
            try:
                batch = _fetch_psr(psr_type, p_start_str, p_end_str)
                store.extend(batch)
                log.debug("DE wind %s %s-%s: %d points",
                          psr_type, p_start_str, p_end_str, len(batch))
            except requests.HTTPError as e:
                log.warning("DE wind %s %s-%s failed: %s",
                            psr_type, p_start_str, p_end_str, e)

        current = chunk_end

    if not onshore_records and not offshore_records:
        log.warning("DE wind: no data returned")
        return pd.DataFrame()

    def _to_series(records: list[dict], col: str) -> pd.Series:
        if not records:
            return pd.Series(dtype=float, name=col)
        df = pd.DataFrame(records)
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
        return (
            df.set_index("datetime_utc")["mw"]
            .resample("h").mean()
            .rename(col)
        )

    on  = _to_series(onshore_records,  "de_wind_onshore_mw")
    off = _to_series(offshore_records, "de_wind_offshore_mw")

    df = pd.concat([on, off], axis=1).sort_index()
    df["de_wind_mw"]      = df.sum(axis=1)
    df["fetch_timestamp"] = fetch_ts

    log.info("DE wind generation: %d hourly rows", len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 9. ENTSO-E — French DA price (A44)
# ─────────────────────────────────────────────────────────────────────────────

ENTSO_PRICE_NS = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}

def fetch_fr_da_price(
    api_key: str,
    start: str = SAMPLE_START,
    end: str = MAIN_END,
) -> pd.DataFrame:
    """
    Fetch French day-ahead electricity price from ENTSO-E (document A44).
    Used as Model B control — the FR DA clear at 08:00 transmits the
    outage signal into GB DA via IFA2 implicit coupling. Including this
    as a regressor isolates the residual GB DA mispricing in the
    08:00-11:00 window (primarily IFA1 channel).

    Implementation notes:
      - Document A44, in_Domain = out_Domain = FR bidding zone.
      - Plain XML response (no ZIP).
      - Different namespace: publicationdocument:7:3.
      - Price tag is price.amount (not quantity).
      - PT60M resolution — already hourly, no resampling needed.
      - Chunked by month.

    Returns hourly DataFrame with fr_da_price_eur_mwh and fetch_timestamp.
    """
    fetch_ts = _stamp()
    log.info("Fetching FR DA price from ENTSO-E (fetch_ts=%s)", fetch_ts)

    def _parse_price_xml(text: str) -> list[dict]:
        root    = ET.fromstring(text)
        records = []

        for ts in root.findall("ns:TimeSeries", ENTSO_PRICE_NS):
            for period in ts.findall("ns:Period", ENTSO_PRICE_NS):
                p_start = pd.Timestamp(
                    _xtext(period, "ns:timeInterval/ns:start", ENTSO_PRICE_NS), tz="UTC"
                )
                res_min = _resolution_minutes(
                    _xtext(period, "ns:resolution", ENTSO_PRICE_NS)
                )
                for pt in period.findall("ns:Point", ENTSO_PRICE_NS):
                    pos   = int(_xtext(pt, "ns:position",     ENTSO_PRICE_NS) or "1")
                    price = _xfloat(pt, "ns:price.amount",    ENTSO_PRICE_NS)
                    records.append({
                        "datetime_utc":       p_start + timedelta(minutes=res_min * (pos - 1)),
                        "fr_da_price_eur_mwh": price,
                    })
        return records

    start_ts = pd.Timestamp(start)
    end_ts   = pd.Timestamp(end)
    all_records: list[dict] = []
    current = start_ts

    while current < end_ts:
        chunk_end   = min(current + pd.DateOffset(months=1), end_ts)
        p_start_str = current.strftime("%Y%m%d%H%M")
        p_end_str   = chunk_end.strftime("%Y%m%d%H%M")

        try:
            r = _get(ENTSO_BASE, params={
                "securityToken": api_key,
                "documentType":  "A44",
                "in_Domain":     ENTSO_FR_ZONE,
                "out_Domain":    ENTSO_FR_ZONE,
                "periodStart":   p_start_str,
                "periodEnd":     p_end_str,
            })
            batch = _parse_price_xml(r.text)
            all_records.extend(batch)
            log.debug("FR DA price %s-%s: %d points", p_start_str, p_end_str, len(batch))
        except requests.HTTPError as e:
            log.warning("FR DA price %s-%s failed: %s", p_start_str, p_end_str, e)

        current = chunk_end

    if not all_records:
        log.warning("FR DA price: no data returned")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df = df.set_index("datetime_utc").sort_index()
    df["fetch_timestamp"] = fetch_ts

    log.info("FR DA price: %d hourly rows", len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 10. Elexon MID — half-hourly intraday settlement prices
# ─────────────────────────────────────────────────────────────────────────────

def fetch_elexon_mid_halfhourly(
    start: str = SAMPLE_START,
    end: str = MAIN_END,
) -> pd.DataFrame:
    """
    Fetch GB Market Index Data (MID) at half-hourly resolution from Elexon.

    Used as an intraday price proxy for Notebook 02b — EPEX GB hourly ID
    settlement prices are not publicly available post-Brexit. MID reflects
    continuous intraday trading cleared through N2EX/APXMIDP and is the
    closest freely available equivalent.

    Limitation (pre-registered): half-hourly volume-weighted settlement
    averages, not tick-level prices. Results stated at 1 MW and not
    extrapolated. The 16:00-17:00 test window = settlement periods 33-34.

    Returns half-hourly DataFrame indexed by datetime_utc with:
      - gb_id_price_gbp_mwh  (volume-weighted price across providers)
      - gb_id_volume_mwh     (total volume traded)
      - fetch_timestamp
    """
    fetch_ts = _stamp()
    log.info("Fetching Elexon MID half-hourly (fetch_ts=%s)", fetch_ts)

    frames  = []
    current = pd.Timestamp(start, tz="UTC")
    end_ts  = pd.Timestamp(end,   tz="UTC")
    errors  = 0

    while current < end_ts:
        batch_end = min(current + timedelta(days=7), end_ts)
        # Must build URL string directly — params dict encodes colons -> 400
        url = (
            f"{ELEXON_MID_URL}"
            f"?from={current.strftime('%Y-%m-%dT%H:%MZ')}"
            f"&to={batch_end.strftime('%Y-%m-%dT%H:%MZ')}"
        )
        try:
            r = SESSION.get(url, timeout=30)
            r.raise_for_status()
            rows = r.json().get("data", [])
            if rows:
                frames.append(pd.DataFrame(rows))
        except requests.HTTPError as e:
            errors += 1
            log.debug("Elexon MID HH batch %s failed: %s", current.date(), e)
        current = batch_end

    if errors:
        log.warning("Elexon MID HH: %d batch errors", errors)
    if not frames:
        log.warning("Elexon MID HH: no data returned")
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df = df[df["volume"] > 0]

    # Parse datetime from settlementDate + settlementPeriod
    # settlementPeriod 1 = 00:00-00:30, period N starts at (N-1)*30 min
    df["datetime_utc"] = (
        pd.to_datetime(df["settlementDate"], utc=True)
        + pd.to_timedelta((df["settlementPeriod"].astype(int) - 1) * 30, unit="min")
    )

    # Volume-weighted price per half-hour across both providers
    hh = (
        df.groupby("datetime_utc")
        .apply(
            lambda x: pd.Series({
                "gb_id_price_gbp_mwh": (x["price"] * x["volume"]).sum() / x["volume"].sum(),
                "gb_id_volume_mwh":     x["volume"].sum(),
            }),
            include_groups=False,
        )
        .reset_index()
    )
    hh["datetime_utc"] = pd.to_datetime(hh["datetime_utc"], utc=True)
    hh = hh.set_index("datetime_utc").sort_index()
    hh["fetch_timestamp"] = fetch_ts

    log.info("Elexon MID half-hourly: %d periods", len(hh))
    return hh
