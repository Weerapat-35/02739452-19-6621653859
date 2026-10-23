CREATE DATABASE airflow_db;
CREATE TABLE IF NOT EXISTS teams (
    team_id BIGSERIAL PRIMARY KEY,
    team_name TEXT NOT NULL UNIQUE,
    team_name_normalized TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS matches (
    match_id BIGSERIAL PRIMARY KEY,
    match_date DATE,
    season TEXT,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    status TEXT,
    source_name TEXT NOT NULL,
    source_match_id TEXT,
    match_key TEXT NOT NULL UNIQUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (home_score IS NULL OR home_score >= 0),
    CHECK (away_score IS NULL OR away_score >= 0),
    CHECK (home_team <> away_team)
);
CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date);
CREATE INDEX IF NOT EXISTS idx_matches_season ON matches(season);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    status TEXT NOT NULL,
    record_count INTEGER NOT NULL DEFAULT 0,
    message TEXT,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
