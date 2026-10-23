# Premier League Data Integration Dashboard

Docker Compose + Apache Airflow + PostgreSQL + Streamlit.

## Data sources (3)
1. **OpenFootball JSON** (public file, CC0, no key) - https://github.com/openfootball/football.json - last 5 seasons, with dates.
2. **football-data.org REST API** (free personal token) - `https://api.football-data.org/v4/competitions/PL/matches?season=<year>` - last 3 seasons.
   Needs the environment variable `FOOTBALL_DATA_API_KEY` (see "Setup"). Free plan limit is 10 requests/minute; the task waits 7 s between requests
   and retries once after a 429. Seasons the plan does not allow (HTTP 403) are reported on the dashboard and skipped.
   Without a token this source is shown as `failed` ("FOOTBALL_DATA_API_KEY is not set") and the other two still load.
3. **Wikipedia HTML scraping** (the required web-scraping source) - Requests + BeautifulSoup, **no Selenium**.
   The task opens `https://en.wikipedia.org/wiki/<YYYY>-<YY>_Premier_League` (last 3 seasons) and parses the HTML
   *results table* (home team = row header, away team = column, score = cell). Only team names and scores are read.

## Setup
1. Register for a free token at https://www.football-data.org/client/register.
2. Copy `.env.example` to `.env` (same folder as `docker-compose.yml`) and fill in `FOOTBALL_DATA_API_KEY` and your real e-mail in `SCRAPER_CONTACT`.
   Never commit `.env` or paste the token into the code.

## Compliance
- **robots.txt** (Wikipedia scraper): `RobotsGuard` downloads robots.txt and checks **every URL before it is requested** (cached per host).
  200 -> rules are obeyed; 404 -> no restrictions; 401/403/5xx/network error -> treated as disallowed and the page is skipped.
  The scraper waits 1 second between requests and sends a descriptive User-Agent with a contact e-mail (`SCRAPER_CONTACT`).
- **API terms**: football-data.org is used through its documented REST API with a personal token, within the free-plan rate limit.
- **PDPA**: no personal data is collected. Stored columns are only match date, season, team names, scores, status and source name.
  (Player, referee, fan or user data is never read or stored.)
- Wikipedia text/data is licensed CC BY-SA 4.0 - credit "Wikipedia" if you present the data.

## Run on Windows
1. Install and start Docker Desktop.
2. Extract the ZIP, create `.env` (see Setup) and open PowerShell in the extracted folder.
3. `docker compose down -v` (only if you ran an older version before), then `docker compose up --build -d`.
4. Airflow: http://localhost:8080 (admin / admin).
5. Dashboard: http://localhost:8501
6. In Airflow trigger `premier_league_pipeline` (it is also scheduled `@daily`); when `load_postgres_upsert` is green, refresh the dashboard.

PostgreSQL is exposed on host port 5433. The dashboard reads **directly from PostgreSQL** (tables `matches`, `pipeline_runs`).
On Linux, `./data` and `./airflow/logs` must be writable by UID 50000 (`sudo chown -R 50000:0 data airflow/logs`).

## How the pipeline works
`start` -> [`public_sources.openfootball_json_api`, `public_sources.football_data_org_api`, `web_scraping.wikipedia_html_scrape`]
-> `merge_deduplicate_quality` -> `quality_branch` -> `load_postgres_upsert` (or `quality_failed`) -> `end`

- Team names from all sources are mapped to one canonical name (`TEAM_ALIASES` in the DAG; club suffixes such as "FC"/"AFC" are stripped).
- A match is identified by `season | home team | away team` (each pair plays once per league season), so the same match from different sources is merged into one row;
  the date is kept when any source has it, and every contributing source is listed in `source_name`. If sources disagree on a score, the first source wins (OpenFootball, then Wikipedia, then football-data.org).
- Each source task deletes its previous output file before running, so a crashed task can never feed stale data into the merge.
- Quality checks: valid teams/season, no negative scores, home team != away team, at least one valid row.
- Load uses UPSERT on `match_key`, so reruns never duplicate rows. If the load itself fails, a `load_postgres` / `failed` row is written to `pipeline_runs`.
- A malformed record in a source is skipped and reported (not the whole season); source failures are isolated: a failing source is recorded in `pipeline_runs` (shown on the dashboard) and the others still load.

## Requirement check
| Requirement | How it is met |
|---|---|
| >= 3 data sources | OpenFootball JSON file, football-data.org REST API, Wikipedia HTML scraping |
| >= 1 web-scraping source, no Selenium | `wikipedia_html_scrape` (Requests + BeautifulSoup) |
| robots.txt + PDPA | per-URL robots check, no personal data stored |
| >= 100 records | one season page alone gives 380 matches; the pipeline normally loads well over 1,000 |
| Dashboard reads PostgreSQL directly | `dashboard/app.py` queries PostgreSQL via SQLAlchemy |

## Troubleshooting
- Old data / `airflow_db` missing after changing versions: `docker compose down -v`, then `docker compose up --build -d`.
- `football_data_org_api` shows "FOOTBALL_DATA_API_KEY is not set": create `.env`, then `docker compose up -d` again so the variable reaches the containers.
- `football_data_org_api` shows HTTP 403 for a season: that season is not included in your plan; the other seasons and sources still load.
- `wikipedia_html_scrape` shows "skipped": robots.txt could not be read or disallows the page; the other sources still load.
- A club appears twice on the dashboard: add its other spelling to `TEAM_ALIASES` in `airflow/dags/premier_league_pipeline.py`.
- Default passwords (`admin/admin`, `football123`) are for local coursework only.
- Tested with unit tests: OpenFootball parsing on real data, a Wikipedia-style results table, football-data.org parsing/fetching with mocked responses, and a 3-source merge
  (no duplicates, aliases resolved). **Not run end-to-end in Docker, and not run against the live football-data.org or Wikipedia pages** - if a source changes its format, check the Airflow task logs.
