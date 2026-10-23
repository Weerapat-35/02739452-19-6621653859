from __future__ import annotations
import json, logging, os, re, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser
import pandas as pd
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook

DATA_DIR = Path("/opt/airflow/data")
# Wikimedia asks scrapers to send a descriptive User-Agent with contact info. Set SCRAPER_CONTACT in docker-compose.yml.
SCRAPER_CONTACT = os.environ.get("SCRAPER_CONTACT", "student-project@example.com")
USER_AGENT = f"PremierLeagueStudentProject/1.0 (educational data integration; contact: {SCRAPER_CONTACT})"
REQUEST_DELAY = 1.0  # seconds between requests to the same site (polite crawling)
TIMEOUT = 25

def emit(name, source, records, error=None):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / name).write_text(json.dumps({
        "source": source, "fetched_at": datetime.now(timezone.utc).isoformat(),
        "error": error, "records": records or []
    }, ensure_ascii=False, default=str), encoding="utf-8")
    logging.info("%s: %s records; error=%s", source, len(records or []), error)

def reset_output(name):
    """Delete a source's previous output so a crashed task can never leave stale data for the merge step."""
    try:
        (DATA_DIR / name).unlink(missing_ok=True)
    except Exception as e:
        logging.warning("could not remove old %s: %s", name, e)

def get_session():
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json,text/html,*/*"})
    return s

def season_start_year():
    return datetime.now().year if datetime.now().month >= 7 else datetime.now().year - 1

def _as_int(v):
    """Return int for ints / digit strings, else None (bool is not a score)."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v.is_integer():
        return int(v)
    if isinstance(v, str) and v.strip().isdigit():
        return int(v.strip())
    return None

def _team_name(v):
    """Team may be a plain string, an object like {"name": ...}, or a list of those."""
    if isinstance(v, str):
        return v.strip() or None
    if isinstance(v, dict):
        return _team_name(v.get("name") or v.get("title") or v.get("team"))
    if isinstance(v, list):
        for item in v:
            name = _team_name(item)
            if name:
                return name
    return None

def _full_time(score):
    """Extract (home, away) full-time goals from the different score shapes football.json can have."""
    ft = None
    if isinstance(score, dict):
        ft = score.get("ft") or score.get("fulltime") or score.get("full_time")
    elif isinstance(score, list):
        if len(score) >= 2 and all(_as_int(x) is not None for x in score[:2]):
            ft = score
        else:
            for item in score:
                if isinstance(item, dict) and (item.get("ft") or item.get("fulltime")):
                    ft = item.get("ft") or item.get("fulltime")
                    break
    if isinstance(ft, dict):
        ft = [ft.get("home", ft.get("team1")), ft.get("away", ft.get("team2"))]
    if isinstance(ft, (list, tuple)) and len(ft) >= 2:
        h, a = _as_int(ft[0]), _as_int(ft[1])
        if h is not None and a is not None:
            return h, a
    return None, None

def _matches_from_payload(payload):
    """The matches may sit in payload['matches'], in payload['rounds'][i]['matches'], or be a bare list."""
    if isinstance(payload, dict):
        if isinstance(payload.get("matches"), list):
            return payload["matches"]
        out = []
        for rnd in payload.get("rounds") or []:
            if isinstance(rnd, dict) and isinstance(rnd.get("matches"), list):
                out.extend(rnd["matches"])
        return out
    if isinstance(payload, list):
        out = []
        for item in payload:
            if isinstance(item, dict) and isinstance(item.get("matches"), list):
                out.extend(item["matches"])
            else:
                out.append(item)
        return out
    return []

def parse_openfootball_payload(payload, season_label):
    """Parse one season file. One malformed match never drops the whole season: it is skipped and reported."""
    rows, skipped, first_err = [], 0, None
    items = _matches_from_payload(payload)
    for m in items:
        try:
            if not isinstance(m, dict):
                raise TypeError(f"match is {type(m).__name__}, expected object")
            home, away = _team_name(m.get("team1")), _team_name(m.get("team2"))
            if not home or not away:
                raise ValueError("missing team name")
            hs, aw = _full_time(m.get("score"))
            date = m.get("date")
            rows.append({
                "match_date": str(date)[:10] if date else None, "season": season_label,
                "home_team": home, "away_team": away,
                "home_score": hs, "away_score": aw,
                "status": "FINISHED" if hs is not None and aw is not None else "SCHEDULED",
                "source_name": "OpenFootball JSON", "source_match_id": "",
            })
        except Exception as e:
            skipped += 1
            if first_err is None:
                first_err = f"{type(e).__name__}: {e}; sample={json.dumps(m, ensure_ascii=False, default=str)[:160]}"
    return rows, skipped, len(items), first_err

def fetch_openfootball_json():
    """Public JSON datasets from OpenFootball (CC0); no account or API key required."""
    reset_output("openfootball.json")
    rows, errors = [], []
    current = season_start_year()
    # Include several seasons to ensure 100+ match rows where data is available.
    seasons = [f"{y}-{str(y+1)[-2:]}" for y in range(current-4, current+1)]
    s = get_session()
    for season_code in seasons:
        url = f"https://raw.githubusercontent.com/openfootball/football.json/master/{season_code}/en.1.json"
        try:
            r = s.get(url, timeout=TIMEOUT)
            if r.status_code == 404:
                logging.info("OpenFootball season not published: %s", season_code)
                continue
            r.raise_for_status()
            season_label = f"{season_code[:4]}/{2000+int(season_code[-2:])}"
            season_rows, skipped, total, first_err = parse_openfootball_payload(r.json(), season_label)
            rows.extend(season_rows)
            logging.info("OpenFootball %s: %s rows, %s skipped of %s", season_code, len(season_rows), skipped, total)
            if skipped:
                errors.append(f"{season_code}: skipped {skipped}/{total} matches (first: {first_err})")
        except Exception as e:
            errors.append(f"{season_code}: {type(e).__name__}: {e}")
            logging.warning("OpenFootball season failed %s: %s", season_code, e)
    emit("openfootball.json", "OpenFootball JSON API", rows, "; ".join(errors) if errors else None)

FOOTBALL_DATA_URL = "https://api.football-data.org/v4/competitions/PL/matches"
FOOTBALL_DATA_DELAY = 7.0   # free plan allows 10 requests/minute -> stay safely below it
FOOTBALL_DATA_SOURCE = "football-data.org API"

def parse_football_data_payload(payload, season_label):
    """Parse one football-data.org /competitions/PL/matches response. A malformed match is skipped and reported."""
    items = payload.get("matches") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise ValueError("response has no 'matches' list")
    rows, skipped, first_err = [], 0, None
    for m in items:
        try:
            if not isinstance(m, dict):
                raise TypeError(f"match is {type(m).__name__}, expected object")
            home = _team_name((m.get("homeTeam") or {}).get("name"))
            away = _team_name((m.get("awayTeam") or {}).get("name"))
            if not home or not away:
                raise ValueError("missing team name")
            hs = aw = None
            if m.get("status") in ("FINISHED", "AWARDED"):
                ft = (m.get("score") or {}).get("fullTime") or {}
                hs, aw = _as_int(ft.get("home")), _as_int(ft.get("away"))
                if hs is None or aw is None:
                    hs = aw = None
            date = m.get("utcDate")
            rows.append({
                "match_date": str(date)[:10] if date else None, "season": season_label,
                "home_team": home, "away_team": away, "home_score": hs, "away_score": aw,
                "status": "FINISHED" if hs is not None else "SCHEDULED",
                "source_name": FOOTBALL_DATA_SOURCE, "source_match_id": str(m.get("id") or ""),
            })
        except Exception as e:
            skipped += 1
            if first_err is None:
                first_err = f"{type(e).__name__}: {e}; sample={json.dumps(m, ensure_ascii=False, default=str)[:160]}"
    return rows, skipped, len(items), first_err

def fetch_football_data_org():
    """Third source: football-data.org REST API (free personal token from https://www.football-data.org/client/register).
    The token is read from the FOOTBALL_DATA_API_KEY environment variable and is never logged.
    Without a token the source is reported as failed on the dashboard and the other two sources still load."""
    reset_output("football_data.json")
    token = os.environ.get("FOOTBALL_DATA_API_KEY", "").strip()
    if not token:
        emit("football_data.json", FOOTBALL_DATA_SOURCE, [], "FOOTBALL_DATA_API_KEY is not set; source skipped")
        return
    rows, errors = [], []
    session = get_session()
    session.headers["X-Auth-Token"] = token
    current = season_start_year()
    for n, year in enumerate(range(current - 2, current + 1)):
        season_label = f"{year}/{year + 1}"
        if n:
            time.sleep(FOOTBALL_DATA_DELAY)
        try:
            r = session.get(FOOTBALL_DATA_URL, params={"season": year}, timeout=TIMEOUT)
            if r.status_code == 429:                       # rate limited: wait for the per-minute window, retry once
                logging.warning("football-data.org rate limit hit; waiting 65s")
                time.sleep(65)
                r = session.get(FOOTBALL_DATA_URL, params={"season": year}, timeout=TIMEOUT)
            if r.status_code in (400, 403, 404):
                errors.append(f"{season_label}: HTTP {r.status_code} (season not available on this plan or token invalid)")
                continue
            r.raise_for_status()
            season_rows, skipped, total, first_err = parse_football_data_payload(r.json(), season_label)
            rows.extend(season_rows)
            logging.info("football-data.org %s: %s rows, %s skipped of %s", season_label, len(season_rows), skipped, total)
            if skipped:
                errors.append(f"{season_label}: skipped {skipped}/{total} matches (first: {first_err})")
        except Exception as e:
            errors.append(f"{season_label}: {type(e).__name__}: {e}")
            logging.warning("football-data.org season failed %s: %s", season_label, type(e).__name__)
    emit("football_data.json", FOOTBALL_DATA_SOURCE, rows, "; ".join(errors) if errors else None)

class RobotsGuard:
    """Checks robots.txt for EVERY URL before it is requested (robots.txt is fetched once per host and cached).

    Rules (RFC 9309): 200 -> parse and obey; other 4xx (e.g. 404) -> no restrictions;
    401/403, 5xx or network errors -> treat as disallowed (conservative)."""
    def __init__(self, session):
        self.session = session
        self._parsers = {}

    def _parser_for(self, url):
        from urllib.parse import urlparse
        p = urlparse(url)
        origin = f"{p.scheme}://{p.netloc}"
        if origin in self._parsers:
            return self._parsers[origin]
        rp = RobotFileParser()
        try:
            r = self.session.get(origin + "/robots.txt", timeout=TIMEOUT)
            if r.status_code == 200:
                rp.parse(r.text.splitlines())
            elif r.status_code in (401, 403) or r.status_code >= 500:
                logging.warning("robots.txt for %s returned %s -> treat as disallowed", origin, r.status_code)
                rp.disallow_all = True
            else:
                logging.info("robots.txt for %s returned %s -> no restrictions", origin, r.status_code)
                rp.allow_all = True
        except Exception as e:
            logging.warning("robots.txt check failed for %s (%s) -> treat as disallowed", origin, e)
            rp.disallow_all = True
        self._parsers[origin] = rp
        return rp

    def allowed(self, url):
        return self._parser_for(url).can_fetch(USER_AGENT, url)

SCORE_RE = re.compile(r"^(\d{1,2})\s*[\u2013\u2014\u2212-]\s*(\d{1,2})$")

def clean_cell(text):
    text = str(text or "").replace("\xa0", " ")
    text = re.sub(r"\[[^\]]*\]", "", text)          # footnote markers such as [a] or [1]
    return re.sub(r"\s+", " ", text).strip()

def clean_team(text):
    text = clean_cell(text)
    return re.sub(r"\s*\((?:C|R|P|O|T|A)(?:,\s*[A-Z])*\)$", "", text).strip()   # (C) champions, (R) relegated ...

def find_results_matrix(soup):
    """Find the 'Home \\ Away' results cross-table on a Wikipedia season page."""
    best, best_n = None, 0
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 10:
            continue
        header = clean_cell(rows[0].get_text(" ")).casefold()
        n_scores = sum(1 for td in table.find_all("td") if SCORE_RE.match(clean_cell(td.get_text())))
        if "home" in header and "away" in header and n_scores > 0:
            return table
        if n_scores > best_n:
            best, best_n = table, n_scores
    return best if best_n >= 100 else None

def parse_results_matrix(table, season_label):
    """Parse the HTML table: row header = home team, columns = away teams (same order as the rows)."""
    parsed = []
    for tr in table.find_all("tr"):
        th = tr.find("th")
        tds = tr.find_all("td")
        if th is None or not tds:
            continue
        name = clean_team(th.get_text(" "))
        if name:
            parsed.append((name, [clean_cell(td.get_text(" ")) for td in tds]))
    n = len(parsed)
    if n < 10:
        raise ValueError(f"results matrix too small ({n} rows)")
    teams = [name for name, _ in parsed]
    out = []
    for i, (home, cells) in enumerate(parsed):
        if len(cells) != n:
            raise ValueError(f"row '{home}' has {len(cells)} cells, expected {n}")
        if SCORE_RE.match(cells[i]):                     # diagonal must be empty/dash if columns follow row order
            raise ValueError("diagonal cell contains a score; column order differs from row order")
        for j, cell in enumerate(cells):
            if i == j:
                continue
            m = SCORE_RE.match(cell)
            if not m:
                continue                                  # not played yet / postponed / blank
            out.append({
                "match_date": None, "season": season_label,
                "home_team": home, "away_team": teams[j],
                "home_score": int(m.group(1)), "away_score": int(m.group(2)),
                "status": "FINISHED", "source_name": "Wikipedia HTML scrape", "source_match_id": "",
            })
    return out

def scrape_wikipedia_results():
    """Required web-scraping task (Requests + BeautifulSoup, no Selenium).
    Scrapes the HTML results table of the last 3 Premier League season pages on Wikipedia.
    Only team names and scores are read: no personal data is collected. Wikipedia text is CC BY-SA 4.0."""
    reset_output("scraped.json")
    rows, errors = [], []
    session = get_session()
    guard = RobotsGuard(session)
    current = season_start_year()
    for y in range(current - 2, current + 1):
        season_label = f"{y}/{y + 1}"
        url = f"https://en.wikipedia.org/wiki/{y}\u2013{str(y + 1)[-2:]}_Premier_League"
        if not guard.allowed(url):
            errors.append(f"{season_label}: robots.txt disallows or could not be verified; skipped")
            continue
        try:
            time.sleep(REQUEST_DELAY)
            r = session.get(url, timeout=TIMEOUT)
            if r.status_code == 404:
                logging.info("Wikipedia page not found for %s", season_label)
                continue
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            table = find_results_matrix(soup)
            if table is None:
                errors.append(f"{season_label}: results table not found in HTML")
                continue
            season_rows = parse_results_matrix(table, season_label)
            logging.info("Wikipedia %s: %s matches scraped", season_label, len(season_rows))
            rows.extend(season_rows)
        except Exception as e:
            errors.append(f"{season_label}: {type(e).__name__}: {e}")
            logging.warning("Wikipedia scrape failed for %s: %s", season_label, e)
    if not rows and not errors:
        errors.append("No match rows obtained from HTML")
    emit("scraped.json", "Wikipedia HTML scraping", rows, "; ".join(errors) if errors else None)

def norm(value):
    return re.sub(r"\s+", " ", str(value or "").strip()).casefold()

# Different sources spell clubs differently ("Arsenal FC" vs "Arsenal", "Man United" vs "Manchester United").
# Map everything to one canonical name so match_key de-duplicates across sources and the dashboard shows one bar per club.
TEAM_ALIASES = {
    "man united": "Manchester United", "man utd": "Manchester United", "manchester utd": "Manchester United",
    "man city": "Manchester City",
    "newcastle": "Newcastle United", "newcastle utd": "Newcastle United",
    "tottenham": "Tottenham Hotspur", "spurs": "Tottenham Hotspur",
    "west ham": "West Ham United",
    "wolves": "Wolverhampton Wanderers", "wolverhampton": "Wolverhampton Wanderers",
    "nott'm forest": "Nottingham Forest", "nottm forest": "Nottingham Forest", "nottingham": "Nottingham Forest",
    "brighton": "Brighton & Hove Albion", "brighton and hove albion": "Brighton & Hove Albion",
    "leeds": "Leeds United", "leicester": "Leicester City", "norwich": "Norwich City",
    "ipswich": "Ipswich Town", "luton": "Luton Town", "sheffield utd": "Sheffield United",
    "west brom": "West Bromwich Albion", "cardiff": "Cardiff City", "swansea": "Swansea City",
    "stoke": "Stoke City", "hull": "Hull City", "huddersfield": "Huddersfield Town",
    "bournemouth": "AFC Bournemouth", "afc bournemouth": "AFC Bournemouth",
    "qpr": "Queens Park Rangers", "crystal palace": "Crystal Palace",
}

def canonical_team(name):
    s = re.sub(r"\s+", " ", str(name or "").strip())
    if not s:
        return s
    key = s.casefold()
    if key in TEAM_ALIASES:
        return TEAM_ALIASES[key]
    # strip club suffixes such as "Arsenal FC"
    stripped = re.sub(r"\s+(fc|afc)$", "", s, flags=re.I).strip()
    skey = stripped.casefold()
    return TEAM_ALIASES.get(skey, stripped)

def merge_validate():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    filenames = ["openfootball.json", "scraped.json", "football_data.json"]
    unique, statuses = {}, []
    for filename in filenames:
        path = DATA_DIR / filename
        if not path.exists():
            statuses.append({"source": filename, "status": "missing", "count": 0, "message": "Output missing"})
            continue
        p = json.loads(path.read_text(encoding="utf-8"))
        records, err = p.get("records", []), p.get("error")
        statuses.append({"source": p.get("source", filename),
                         "status": "partial" if err and records else ("failed" if err else ("success" if records else "empty")),
                         "count": len(records), "message": err or "OK"})
        for row in records:
            # match_date is optional (the Wikipedia results table has no dates); season + teams identify a PL match.
            if not row.get("season") or not row.get("home_team") or not row.get("away_team"):
                continue
            try:
                if row.get("home_score") is not None: row["home_score"] = int(row["home_score"])
                if row.get("away_score") is not None: row["away_score"] = int(row["away_score"])
                if (row.get("home_score") is not None and row["home_score"] < 0) or (row.get("away_score") is not None and row["away_score"] < 0):
                    continue
                if row.get("match_date"):
                    row["match_date"] = str(pd.to_datetime(row["match_date"], errors="raise").date())
                else:
                    row["match_date"] = None
            except Exception:
                continue
            row["home_team"] = canonical_team(row["home_team"])
            row["away_team"] = canonical_team(row["away_team"])
            if norm(row["home_team"]) == norm(row["away_team"]): continue
            # In a league season every (home, away) pair is played exactly once -> a date-independent key
            # de-duplicates the same match across sources even if their dates differ or are missing.
            key = "|".join([str(row["season"]), norm(row["home_team"]), norm(row["away_team"])])
            row["match_key"] = key
            old = unique.get(key)
            if old is None:
                unique[key] = row
                continue
            old_has = old.get("home_score") is not None and old.get("away_score") is not None
            new_has = row.get("home_score") is not None and row.get("away_score") is not None
            sources = {x for x in (old.get("source_name", "") + "," + row.get("source_name", "")).split(",") if x}
            merged = dict(old)
            if new_has and not old_has:
                merged.update({k: row.get(k) for k in ("home_score", "away_score", "status")})
            merged["match_date"] = old.get("match_date") or row.get("match_date")
            merged["source_match_id"] = old.get("source_match_id") or row.get("source_match_id")
            merged["source_name"] = ",".join(sorted(sources))
            unique[key] = merged
    rows = list(unique.values())
    passed = len(rows) > 0 and all(norm(r["home_team"]) != norm(r["away_team"]) for r in rows)
    (DATA_DIR / "merged.json").write_text(json.dumps(rows, ensure_ascii=False, default=str), encoding="utf-8")
    (DATA_DIR / "source_status.json").write_text(json.dumps(statuses, ensure_ascii=False), encoding="utf-8")
    (DATA_DIR / "quality.json").write_text(json.dumps({"passed": passed, "record_count": len(rows),
      "checks": {"has_valid_rows": len(rows) > 0, "no_negative_scores": all(
      (r.get("home_score") is None or r["home_score"] >= 0) and (r.get("away_score") is None or r["away_score"] >= 0) for r in rows)}}, ensure_ascii=False), encoding="utf-8")
    # Do not raise here: quality_branch routes to quality_failed, which records the failure in pipeline_runs.
    logging.info("Validated %s unique rows (quality passed=%s)", len(rows), passed)

def choose_quality_path():
    q = json.loads((DATA_DIR / "quality.json").read_text(encoding="utf-8"))
    return "load_postgres_upsert" if q.get("passed") else "quality_failed"

def record_load_failure(error):
    """Best effort: make a failed load visible in the dashboard's Pipeline Status table."""
    try:
        conn = PostgresHook(postgres_conn_id="football_db").get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO pipeline_runs(source_name,status,record_count,message) VALUES (%s,%s,%s,%s)",
                                ("load_postgres", "failed", 0, f"{type(error).__name__}: {error}"[:500]))
        finally:
            conn.close()
    except Exception:
        logging.exception("could not record load failure")

def load_postgres():
    try:
        _load_postgres()
    except Exception as e:
        record_load_failure(e)
        raise

def _load_postgres():
    rows = json.loads((DATA_DIR / "merged.json").read_text(encoding="utf-8"))
    hook = PostgresHook(postgres_conn_id="football_db")
    conn = hook.get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                for r in rows:
                    cur.execute("""
                      INSERT INTO matches(match_date,season,home_team,away_team,home_score,away_score,status,source_name,source_match_id,match_key,updated_at)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                      ON CONFLICT(match_key) DO UPDATE SET
                        match_date=COALESCE(EXCLUDED.match_date,matches.match_date),
                        home_score=COALESCE(EXCLUDED.home_score,matches.home_score),
                        away_score=COALESCE(EXCLUDED.away_score,matches.away_score),
                        status=CASE WHEN EXCLUDED.home_score IS NOT NULL AND EXCLUDED.away_score IS NOT NULL
                                    THEN EXCLUDED.status ELSE matches.status END,
                        source_name=(SELECT string_agg(DISTINCT s, ',' ORDER BY s)
                                     FROM unnest(string_to_array(matches.source_name || ',' || EXCLUDED.source_name, ',')) AS s
                                     WHERE s <> ''),
                        source_match_id=COALESCE(NULLIF(EXCLUDED.source_match_id,''),matches.source_match_id),
                        updated_at=NOW()
                    """, (r.get("match_date"),r.get("season"),r.get("home_team"),r.get("away_team"),
                          r.get("home_score"),r.get("away_score"),r.get("status"),r.get("source_name"),
                          r.get("source_match_id"),r["match_key"]))
                statuses = json.loads((DATA_DIR / "source_status.json").read_text(encoding="utf-8"))
                for s in statuses:
                    cur.execute("INSERT INTO pipeline_runs(source_name,status,record_count,message) VALUES (%s,%s,%s,%s)",
                                (s["source"],s["status"],s["count"],s["message"]))
    finally:
        conn.close()

def quality_failed():
    statuses = json.loads((DATA_DIR / "source_status.json").read_text(encoding="utf-8")) if (DATA_DIR / "source_status.json").exists() else []
    hook = PostgresHook(postgres_conn_id="football_db")
    conn = hook.get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO pipeline_runs(source_name,status,record_count,message) VALUES (%s,%s,%s,%s)",
                            ("data_quality","failed",0,"No valid rows; inspect source task logs"))
                for s in statuses:
                    cur.execute("INSERT INTO pipeline_runs(source_name,status,record_count,message) VALUES (%s,%s,%s,%s)",
                                (s["source"],s["status"],s["count"],s["message"]))
    finally:
        conn.close()

with DAG(
    dag_id="premier_league_pipeline",
    description="OpenFootball JSON + football-data.org REST API + robots.txt-checked HTML table scraping (Wikipedia)",
    start_date=datetime(2025,1,1), schedule="@daily", catchup=False, max_active_runs=1,
    tags=["premier-league","etl","postgres"],
) as dag:
    start = EmptyOperator(task_id="start")
    with TaskGroup(group_id="public_sources") as public_sources:
        a = PythonOperator(task_id="openfootball_json_api", python_callable=fetch_openfootball_json)
        b = PythonOperator(task_id="football_data_org_api", python_callable=fetch_football_data_org)
    with TaskGroup(group_id="web_scraping") as scraping:
        c = PythonOperator(task_id="wikipedia_html_scrape", python_callable=scrape_wikipedia_results)
    merge = PythonOperator(task_id="merge_deduplicate_quality", python_callable=merge_validate,
                            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    branch = BranchPythonOperator(task_id="quality_branch", python_callable=choose_quality_path)
    load = PythonOperator(task_id="load_postgres_upsert", python_callable=load_postgres)
    fail = PythonOperator(task_id="quality_failed", python_callable=quality_failed)
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    start >> [public_sources, scraping]
    [public_sources, scraping] >> merge >> branch
    branch >> [load, fail] >> end
