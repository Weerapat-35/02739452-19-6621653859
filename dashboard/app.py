import os
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Premier League Dashboard", page_icon="⚽", layout="wide")
st.title("⚽ Premier League Data Dashboard")
st.caption("ข้อมูลจาก Airflow → PostgreSQL | Dashboard อ่านข้อมูลจาก PostgreSQL โดยตรง")

@st.cache_data(ttl=30)
def get_data():
    url = os.environ["DATABASE_URL"]
    # Name the driver explicitly: newer SQLAlchemy defaults "postgresql://" to psycopg v3, but we install psycopg2.
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    engine = create_engine(url)
    conn = engine.connect()
    try:
        matches = pd.read_sql_query("""
            SELECT match_date, season, home_team, away_team, home_score, away_score,
                   status, source_name, updated_at
            FROM matches ORDER BY match_date DESC NULLS LAST
        """, conn)
        runs = pd.read_sql_query("""
            SELECT source_name, status, record_count, message, finished_at
            FROM pipeline_runs ORDER BY finished_at DESC LIMIT 100
        """, conn)
        return matches, runs
    finally:
        conn.close()
        engine.dispose()

try:
    matches, runs = get_data()
except Exception as exc:
    st.error(f"เชื่อมต่อ PostgreSQL ไม่สำเร็จ: {exc}")
    st.info("ตรวจสอบว่า docker compose up ทำงานครบ และ PostgreSQL healthy แล้ว")
    st.stop()

if matches.empty:
    st.warning("ยังไม่มีข้อมูลในฐานข้อมูล กรุณาเปิด Airflow แล้ว Trigger DAG: premier_league_pipeline")
    st.subheader("สถานะ Pipeline ล่าสุด")
    st.dataframe(runs, use_container_width=True, hide_index=True)
    st.stop()

matches["match_date"] = pd.to_datetime(matches["match_date"], errors="coerce")
c1, c2, c3, c4 = st.columns(4)
c1.metric("จำนวนแมตช์", f"{len(matches):,}")
c2.metric("จำนวนทีม", f"{len(set(matches['home_team'].dropna()) | set(matches['away_team'].dropna())):,}")
finished = matches.dropna(subset=["home_score", "away_score"])
c3.metric("แมตช์ที่มีผลแล้ว", f"{len(finished):,}")
c4.metric("จำนวนฤดูกาล", f"{matches['season'].nunique():,}")

st.sidebar.header("ตัวกรอง")
seasons = sorted(matches["season"].dropna().unique().tolist(), reverse=True)
# default to the newest season that actually has results (avoids an empty chart when the newest season is only fixtures)
finished_seasons = sorted(finished["season"].dropna().unique().tolist(), reverse=True)
default_seasons = finished_seasons[:1] or seasons[:1]
selected_seasons = st.sidebar.multiselect("ฤดูกาล", seasons, default=default_seasons)
teams = sorted(set(matches["home_team"].dropna()) | set(matches["away_team"].dropna()))
selected_team = st.sidebar.selectbox("ทีม", ["ทุกทีม"] + teams)
filtered = matches[matches["season"].isin(selected_seasons)] if selected_seasons else matches.iloc[0:0]
if selected_team != "ทุกทีม":
    filtered = filtered[(filtered["home_team"] == selected_team) | (filtered["away_team"] == selected_team)]

st.subheader("จำนวนประตูตามทีม")
goal_rows = []
only_team = None if selected_team == "ทุกทีม" else selected_team
for _, r in filtered.dropna(subset=["home_score", "away_score"]).iterrows():
    # with a team selected, count only that team's goals (not its opponents')
    if only_team is None or r["home_team"] == only_team:
        goal_rows.append({"ทีม": r["home_team"], "ฤดูกาล": r["season"], "ประตู": int(r["home_score"])})
    if only_team is None or r["away_team"] == only_team:
        goal_rows.append({"ทีม": r["away_team"], "ฤดูกาล": r["season"], "ประตู": int(r["away_score"])})
if goal_rows:
    goals_df = pd.DataFrame(goal_rows)
    if only_team:
        goals = goals_df.groupby("ฤดูกาล", as_index=False)["ประตู"].sum().sort_values("ฤดูกาล")
        fig = px.bar(goals, x="ฤดูกาล", y="ประตู", title=f"ประตูรวมของ {only_team} แยกตามฤดูกาล")
    else:
        goals = goals_df.groupby("ทีม", as_index=False)["ประตู"].sum().sort_values("ประตู", ascending=False)
        fig = px.bar(goals, x="ทีม", y="ประตู", title="ประตูรวมของแต่ละทีม")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("ไม่มีคะแนนที่สมบูรณ์สำหรับตัวกรองนี้")

st.subheader("ผลการแข่งขัน")
display = filtered.copy()
display["match_date"] = display["match_date"].dt.strftime("%Y-%m-%d")
st.dataframe(display[["match_date", "season", "home_team", "home_score", "away_score", "away_team", "status", "source_name"]],
             use_container_width=True, hide_index=True)

st.subheader("Pipeline Status")
if runs.empty:
    st.info("ยังไม่มีประวัติ pipeline")
else:
    st.dataframe(runs, use_container_width=True, hide_index=True)
if st.button("รีเฟรชข้อมูล"):
    st.cache_data.clear()
    st.rerun()
