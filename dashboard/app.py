"""Read-only Streamlit dashboard on top of the dbt Gold layer.

Connects as the least-privilege `dashboard_user` (member of `analytics_reader`,
SELECT on Gold only) inside read-only transactions.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import altair as alt
import pandas as pd
import psycopg
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

RAWG_ATTRIBUTION = "External game data provided by [RAWG](https://rawg.io/apidocs)."


def connection_settings() -> dict[str, Any]:
    user = os.environ.get("DASHBOARD_DB_USER", "dashboard_user")
    password = os.environ.get("DASHBOARD_DB_PASSWORD")
    if not password:
        raise RuntimeError(
            "Set DASHBOARD_DB_PASSWORD in .env (read-only dashboard login created by "
            "sql/006_access_roles.sql) before starting the dashboard."
        )
    return {
        "host": os.environ.get("DASHBOARD_DB_HOST", "localhost"),
        "port": int(os.environ.get("DASHBOARD_DB_PORT", "5433")),
        "dbname": os.environ.get("ANALYTICS_DB", "gaming_analytics"),
        "user": user,
        "password": password,
        "connect_timeout": 5,
        "options": "-c default_transaction_read_only=on -c statement_timeout=30000",
    }


@st.cache_data(ttl=300, show_spinner=False)
def fetch_frame(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    with psycopg.connect(**connection_settings()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            columns = [column.name for column in cursor.description]
            frame = pd.DataFrame(cursor.fetchall(), columns=columns)
    for column in frame.columns:
        if frame[column].dtype == "object" and frame[column].map(
            lambda value: isinstance(value, Decimal)
        ).any():
            frame[column] = pd.to_numeric(frame[column])
    return frame


def filtered_where(alias: str, start_day: date, end_day: date, game_id: int | None) -> tuple[str, tuple[Any, ...]]:
    conditions = [f"{alias}.date_day BETWEEN %s AND %s"]
    params: list[Any] = [start_day, end_day]
    if game_id is not None:
        conditions.append(f"{alias}.game_id = %s")
        params.append(game_id)
    return " AND ".join(conditions), tuple(params)


def load_overview(start_day: date, end_day: date, game_id: int | None, currency: str) -> dict[str, pd.DataFrame]:
    session_where, session_params = filtered_where("sessions", start_day, end_day, game_id)
    purchase_where, purchase_params = filtered_where("purchases", start_day, end_day, game_id)
    review_where, review_params = filtered_where("reviews", start_day, end_day, game_id)

    return {
        "kpis": fetch_frame(
            f"""
            SELECT COUNT(*) AS sessions,
                   COUNT(DISTINCT sessions.player_id) AS active_players,
                   COALESCE(AVG(sessions.duration_minutes), 0)::numeric(12, 2) AS avg_session_minutes
            FROM gold.fact_sessions AS sessions
            WHERE {session_where}
            """,
            session_params,
        ),
        "purchases": fetch_frame(
            f"""
            SELECT COUNT(*) AS purchases,
                   COALESCE(SUM(purchases.amount), 0)::numeric(14, 2) AS gross_amount,
                   COALESCE(SUM(purchases.amount_usd), 0)::numeric(14, 2) AS gross_amount_usd
            FROM gold.fact_purchases AS purchases
            WHERE {purchase_where} AND purchases.currency = %s
            """,
            purchase_params + (currency,),
        ),
        "purchases_usd": fetch_frame(
            f"""
            SELECT COALESCE(SUM(purchases.amount_usd), 0)::numeric(14, 2) AS revenue_usd,
                   COUNT(DISTINCT purchases.player_id) AS paying_players
            FROM gold.fact_purchases AS purchases
            WHERE {purchase_where}
            """,
            purchase_params,
        ),
        "reviews": fetch_frame(
            f"""
            SELECT COUNT(*) AS reviews,
                   COALESCE(AVG(reviews.rating), 0)::numeric(6, 2) AS avg_rating
            FROM gold.fact_reviews AS reviews
            WHERE {review_where}
            """,
            review_params,
        ),
        "sessions_by_day": fetch_frame(
            f"""
            SELECT sessions.date_day, COUNT(*) AS sessions
            FROM gold.fact_sessions AS sessions
            WHERE {session_where}
            GROUP BY sessions.date_day
            ORDER BY sessions.date_day
            """,
            session_params,
        ),
        "revenue_by_day": fetch_frame(
            f"""
            SELECT purchases.date_day,
                   SUM(purchases.amount)::numeric(14, 2) AS gross_amount
            FROM gold.fact_purchases AS purchases
            WHERE {purchase_where} AND purchases.currency = %s
            GROUP BY purchases.date_day
            ORDER BY purchases.date_day
            """,
            purchase_params + (currency,),
        ),
        "top_games": fetch_frame(
            f"""
            SELECT games.game_name, COUNT(*) AS sessions
            FROM gold.fact_sessions AS sessions
            JOIN gold.dim_games AS games USING (game_id)
            WHERE {session_where}
            GROUP BY games.game_name
            ORDER BY sessions DESC, games.game_name
            LIMIT 10
            """,
            session_params,
        ),
        "platforms": fetch_frame(
            f"""
            SELECT platforms.platform_name, COUNT(*) AS sessions
            FROM gold.fact_sessions AS sessions
            JOIN gold.dim_platforms AS platforms USING (platform_id)
            WHERE {session_where}
            GROUP BY platforms.platform_name
            ORDER BY sessions DESC, platforms.platform_name
            LIMIT 10
            """,
            session_params,
        ),
        "game_ratings": fetch_frame(
            f"""
            SELECT games.game_name,
                   COUNT(*) AS reviews,
                   AVG(reviews.rating)::numeric(6, 2) AS avg_rating
            FROM gold.fact_reviews AS reviews
            JOIN gold.dim_games AS games USING (game_id)
            WHERE {review_where}
            GROUP BY games.game_name
            ORDER BY reviews DESC, games.game_name
            LIMIT 10
            """,
            review_params,
        ),
    }


DAILY_KPIS_SQL = """
    SELECT date_day, daily_active_players, new_players, revenue_usd, arpdau_usd, arppu_usd
    FROM gold.mart_daily_kpis
    WHERE date_day BETWEEN %s AND %s
    ORDER BY date_day
"""

TARGETS_SQL = """
    SELECT game_id, game_name, genre,
           target_revenue, actual_revenue_usd, revenue_attainment,
           target_players, actual_players, players_attainment,
           target_sessions, actual_sessions, sessions_attainment,
           target_rating, actual_rating, rating_gap
    FROM gold.mart_game_target_vs_actual
    ORDER BY revenue_attainment DESC NULLS LAST, game_id
"""

CAMPAIGNS_SQL = """
    SELECT campaign_id, campaign, game_name, start_date, end_date, campaign_days,
           budget, actual_spend, budget_spend_ratio,
           campaign_revenue_usd, baseline_revenue_usd, revenue_uplift_usd, uplift_roi,
           is_finished
    FROM gold.mart_campaign_performance
    ORDER BY start_date DESC, campaign_id
"""

RETENTION_SQL = """
    SELECT cohort_month, months_since_first_session, cohort_players, retention_rate
    FROM gold.mart_player_retention
    WHERE months_since_first_session <= %s
    ORDER BY cohort_month, months_since_first_session
"""

GENRE_SQL = """
    SELECT genre, our_games, our_avg_price, our_reviews, our_avg_rating,
           rawg_games, rawg_avg_rating, rawg_avg_metacritic, rawg_avg_playtime_hours,
           rating_gap_vs_rawg
    FROM gold.mart_genre_benchmark
    ORDER BY genre
"""


HEALTH_SQL = {
    "quality": """
        SELECT check_day, status, passed_count, warning_count, failed_count
        FROM gold.ops_quality_daily ORDER BY check_day DESC LIMIT 60
    """,
    "relations": """
        SELECT relation, row_count, column_count, freshness_status, volume_status,
               volume_change_percent, last_ingested_at
        FROM gold.ops_relation_health
        ORDER BY (freshness_status = 'fail' OR volume_status = 'fail') DESC,
                 (freshness_status = 'warn' OR volume_status = 'warn') DESC, relation
    """,
    "dbt": """
        SELECT run_day, resource_type, passed, warned, failed, skipped, failing_rows
        FROM gold.ops_dbt_results_daily ORDER BY run_day DESC LIMIT 200
    """,
    "tasks": """
        SELECT dag_id, task_id, runs, failures, avg_seconds, max_seconds
        FROM gold.ops_task_daily
        WHERE run_day = (SELECT max(run_day) FROM gold.ops_task_daily)
        ORDER BY avg_seconds DESC NULLS LAST
    """,
    "alerts": """
        SELECT fired_at, severity, check_name, object, detail, source, sent
        FROM gold.ops_alerts ORDER BY fired_at DESC LIMIT 100
    """,
}


def render_pipeline_health() -> None:
    st.caption(
        "Operational metadata from the monitoring history (up to the previous pipeline run): "
        "quality checks, dbt results, Airflow task durations and alerts."
    )
    data = {name: fetch_frame(sql) for name, sql in HEALTH_SQL.items()}
    quality, relations, dbt_results, tasks, alerts = (
        data["quality"], data["relations"], data["dbt"], data["tasks"], data["alerts"]
    )
    if quality.empty and alerts.empty and dbt_results.empty:
        st.info("No monitoring history yet. It appears after the first daily pipeline run.")
        return

    latest = quality.iloc[0] if not quality.empty else None
    icon = {"pass": "✅", "warn": "🟡", "fail": "🔴"}
    columns = st.columns(4)
    columns[0].metric(
        "Last quality check",
        f"{icon.get(latest['status'], '')} {latest['status']}" if latest is not None else "n/a",
        help=str(latest["check_day"]) if latest is not None else None,
    )
    failing = relations[(relations["freshness_status"] == "fail") | (relations["volume_status"] == "fail")]
    columns[1].metric("Failing relations", f"{len(failing):,}", help=f"of {len(relations):,} monitored")
    recent = alerts[pd.to_datetime(alerts["fired_at"], utc=True) >= pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)]
    columns[2].metric("Critical alerts (7 days)", f"{int((recent['severity'] == 'critical').sum()):,}")
    tests = dbt_results[dbt_results["resource_type"] == "test"]
    last_tests = tests[tests["run_day"] == tests["run_day"].max()] if not tests.empty else tests
    columns[3].metric(
        "dbt tests failing (last run)",
        f"{int(last_tests['failed'].sum()):,}" if not last_tests.empty else "n/a",
        help=f"{int(last_tests['warned'].sum()):,} warning" if not last_tests.empty else None,
    )

    left, right = st.columns(2)
    with left:
        st.subheader("Quality checks per day")
        if not quality.empty:
            st.line_chart(quality.sort_values("check_day"), x="check_day", y=["warning_count", "failed_count"])
    with right:
        st.subheader("dbt tests per day")
        if not tests.empty:
            st.line_chart(tests.sort_values("run_day"), x="run_day", y=["passed", "warned", "failed"])

    st.subheader("Task durations (latest day)")
    if not tasks.empty:
        tasks = tasks.assign(task=tasks["dag_id"] + " · " + tasks["task_id"])
        st.bar_chart(tasks.head(15), x="task", y="avg_seconds")
    st.subheader("Relations needing attention")
    attention = relations[(relations["freshness_status"].isin(["fail", "warn"]))
                          | (relations["volume_status"].isin(["fail", "warn"]))]
    st.dataframe(attention if not attention.empty else relations.head(0), hide_index=True)
    st.subheader("Recent alerts")
    st.dataframe(alerts, hide_index=True)


def render_overview(start_day: date, end_day: date, game_id: int | None, currency: str) -> None:
    data = load_overview(start_day, end_day, game_id, currency)
    kpis = data["kpis"].iloc[0]
    purchases = data["purchases"].iloc[0]
    purchases_usd = data["purchases_usd"].iloc[0]
    reviews = data["reviews"].iloc[0]

    metric_columns = st.columns(5)
    metric_columns[0].metric("Sessions", f"{int(kpis['sessions']):,}")
    metric_columns[1].metric("Active players", f"{int(kpis['active_players']):,}")
    metric_columns[2].metric("Avg. session", f"{float(kpis['avg_session_minutes']):,.1f} min")
    metric_columns[3].metric(
        f"Purchase value ({currency})",
        f"{float(purchases['gross_amount']):,.2f}",
        help=f"{int(purchases['purchases']):,} purchases in {currency}, original currency.",
    )
    metric_columns[4].metric(
        "Revenue, all currencies (USD)",
        f"{float(purchases_usd['revenue_usd']):,.2f}",
        help="Converted with the illustrative fx_rates_to_usd reference table.",
    )

    engagement_column, purchase_column = st.columns(2)
    with engagement_column:
        st.subheader("Daily sessions")
        st.line_chart(data["sessions_by_day"], x="date_day", y="sessions")
    with purchase_column:
        st.subheader(f"Daily purchase value ({currency})")
        st.line_chart(data["revenue_by_day"], x="date_day", y="gross_amount")

    game_column, platform_column = st.columns(2)
    with game_column:
        st.subheader("Most-played games")
        st.bar_chart(data["top_games"], x="game_name", y="sessions")
    with platform_column:
        st.subheader("Sessions by platform")
        st.bar_chart(data["platforms"], x="platform_name", y="sessions")

    st.subheader("Player feedback")
    feedback_columns = st.columns(2)
    feedback_columns[0].metric("Reviews", f"{int(reviews['reviews']):,}")
    feedback_columns[1].metric("Average rating", f"{float(reviews['avg_rating']):,.2f} / 5")
    st.dataframe(data["game_ratings"], hide_index=True)


def render_kpis(start_day: date, end_day: date) -> None:
    st.caption("All games. Revenue in USD via the illustrative FX reference table (mart_daily_kpis).")
    daily = fetch_frame(DAILY_KPIS_SQL, (start_day, end_day))
    if daily.empty:
        st.info("No daily KPI rows for the selected range.")
        return
    columns = st.columns(4)
    columns[0].metric("Avg. DAU", f"{daily['daily_active_players'].mean():,.0f}")
    columns[1].metric("New players", f"{int(daily['new_players'].sum()):,}")
    columns[2].metric("Revenue (USD)", f"{daily['revenue_usd'].sum():,.2f}")
    paying_days = daily["arppu_usd"].dropna()
    columns[3].metric("Avg. ARPPU (USD)", f"{paying_days.mean():,.2f}" if not paying_days.empty else "n/a")
    left, right = st.columns(2)
    with left:
        st.subheader("Daily active players")
        st.line_chart(daily, x="date_day", y=["daily_active_players", "new_players"])
    with right:
        st.subheader("ARPDAU (USD)")
        st.line_chart(daily, x="date_day", y="arpdau_usd")


def render_targets() -> None:
    st.caption(
        "Actuals against game_targets.xlsx. The workbook has no target period, so all "
        "warehouse activity is compared with each target."
    )
    targets = fetch_frame(TARGETS_SQL)
    if targets.empty:
        st.info("No game targets loaded yet.")
        return
    columns = st.columns(3)
    columns[0].metric("Games with targets", f"{len(targets):,}")
    columns[1].metric("Revenue target met", f"{int((targets['revenue_attainment'] >= 1).sum()):,}")
    columns[2].metric("Rating target met", f"{int((targets['rating_gap'] >= 0).sum()):,}")
    display = targets.copy()
    for column in ("revenue_attainment", "players_attainment", "sessions_attainment"):
        display[column] = display[column] * 100
    st.dataframe(
        display,
        hide_index=True,
        column_config={
            "revenue_attainment": st.column_config.ProgressColumn("Revenue attainment", format="%.1f%%", min_value=0, max_value=100),
            "players_attainment": st.column_config.ProgressColumn("Players attainment", format="%.1f%%", min_value=0, max_value=100),
            "sessions_attainment": st.column_config.ProgressColumn("Sessions attainment", format="%.1f%%", min_value=0, max_value=100),
        },
    )


def render_campaigns() -> None:
    st.caption(
        "Revenue of the promoted game during each campaign versus an equally long window "
        "just before it. Uplift is correlational, not causal."
    )
    campaigns = fetch_frame(CAMPAIGNS_SQL)
    if campaigns.empty:
        st.info("No marketing campaigns loaded yet.")
        return
    columns = st.columns(4)
    columns[0].metric("Campaigns", f"{len(campaigns):,}")
    columns[1].metric("Budget", f"{campaigns['budget'].sum():,.0f}")
    columns[2].metric("Actual spend", f"{campaigns['actual_spend'].sum():,.0f}")
    columns[3].metric("Revenue uplift (USD)", f"{campaigns['revenue_uplift_usd'].sum():,.2f}")
    st.subheader("Budget utilisation")
    st.bar_chart(campaigns, x="campaign", y=["budget", "actual_spend"], stack=False)
    display = campaigns.copy()
    display["budget_spend_ratio"] = display["budget_spend_ratio"] * 100
    st.dataframe(
        display,
        hide_index=True,
        column_config={
            "budget_spend_ratio": st.column_config.ProgressColumn("Spend / budget", format="%.0f%%", min_value=0, max_value=120),
        },
    )


def render_retention() -> None:
    st.caption("Monthly cohorts by first session; share of each cohort active N months later.")
    max_months = st.slider("Months after first session", min_value=3, max_value=24, value=12)
    retention = fetch_frame(RETENTION_SQL, (max_months,))
    if retention.empty:
        st.info("No retention data yet.")
        return
    retention["cohort"] = pd.to_datetime(retention["cohort_month"]).dt.strftime("%Y-%m")
    heatmap = (
        alt.Chart(retention)
        .mark_rect()
        .encode(
            x=alt.X("months_since_first_session:O", title="Months since first session"),
            y=alt.Y("cohort:O", title="Cohort", sort="descending"),
            color=alt.Color("retention_rate:Q", title="Retention", scale=alt.Scale(scheme="blues", domain=[0, 1])),
            tooltip=[
                alt.Tooltip("cohort:O", title="Cohort"),
                alt.Tooltip("months_since_first_session:O", title="Month"),
                alt.Tooltip("cohort_players:Q", title="Cohort size", format=","),
                alt.Tooltip("retention_rate:Q", title="Retention", format=".1%"),
            ],
        )
    )
    st.altair_chart(heatmap, use_container_width=True)


def render_genre_benchmark() -> None:
    st.caption("Our catalogue compared with the external RAWG catalogue, by genre.")
    genres = fetch_frame(GENRE_SQL)
    if genres.empty:
        st.info("No genre benchmark yet. Run the RAWG ingestion and dbt build.")
        return
    chart_data = genres.melt(
        id_vars="genre",
        value_vars=["our_avg_rating", "rawg_avg_rating"],
        var_name="source",
        value_name="avg_rating",
    ).replace({"source": {"our_avg_rating": "Our players", "rawg_avg_rating": "RAWG"}})
    chart = (
        alt.Chart(chart_data)
        .mark_bar()
        .encode(
            x=alt.X("genre:N", title=None),
            xOffset="source:N",
            y=alt.Y("avg_rating:Q", title="Average rating (0-5)"),
            color=alt.Color("source:N", title=None),
            tooltip=["genre", "source", alt.Tooltip("avg_rating:Q", format=".2f")],
        )
    )
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(genres, hide_index=True)
    st.markdown(RAWG_ATTRIBUTION)


st.set_page_config(page_title="Gaming Analytics", page_icon="🎮", layout="wide")
st.title("Gaming Analytics")
st.caption("Engagement, monetisation, targets and player feedback from the dbt Gold layer.")

try:
    date_bounds = fetch_frame(
        """
        SELECT MIN(date_day) AS first_day, MAX(date_day) AS last_day
        FROM (
            SELECT date_day FROM gold.fact_sessions
            UNION ALL
            SELECT date_day FROM gold.fact_purchases
            UNION ALL
            SELECT date_day FROM gold.fact_reviews
        ) AS activity
        """
    )
    games = fetch_frame("SELECT game_id, game_name FROM gold.dim_games ORDER BY game_name")
    currencies = fetch_frame("SELECT DISTINCT currency FROM gold.fact_purchases ORDER BY currency")
except (psycopg.Error, RuntimeError, ValueError) as exc:
    st.error(f"Could not load dashboard filters from the analytics database: {exc}")
    st.stop()

if date_bounds.empty or pd.isna(date_bounds.loc[0, "first_day"]):
    st.warning("No dbt Gold data is available yet. Run the analytics pipeline first.")
    st.stop()

first_day = date_bounds.loc[0, "first_day"]
last_day = date_bounds.loc[0, "last_day"]
default_start = max(first_day, last_day - timedelta(days=89))

with st.sidebar:
    st.header("Filters")
    selected_range = st.date_input(
        "Activity date range",
        value=(default_start, last_day),
        min_value=first_day,
        max_value=last_day,
    )
    game_options: dict[str, int | None] = {"All games": None}
    game_options.update(
        {f"{row.game_name} (#{row.game_id})": int(row.game_id) for row in games.itertuples(index=False)}
    )
    selected_game = st.selectbox("Game (Overview tab)", list(game_options))
    available_currencies = currencies["currency"].tolist() or ["USD"]
    selected_currency = st.selectbox("Purchase currency (Overview tab)", available_currencies)
    st.caption("Original-currency values are never added together; USD figures use reference FX rates.")
    if st.button("Refresh data"):
        fetch_frame.clear()
        st.rerun()

if isinstance(selected_range, (tuple, list)):
    start_day = selected_range[0]
    end_day = selected_range[-1]
else:
    start_day = end_day = selected_range

overview_tab, kpi_tab, target_tab, campaign_tab, retention_tab, genre_tab, health_tab = st.tabs(
    ["Overview", "Daily KPIs", "Targets vs actual", "Campaigns", "Retention", "Genre benchmark (RAWG)",
     "Pipeline health"]
)
try:
    with overview_tab:
        render_overview(start_day, end_day, game_options[selected_game], selected_currency)
    with kpi_tab:
        render_kpis(start_day, end_day)
    with target_tab:
        render_targets()
    with campaign_tab:
        render_campaigns()
    with retention_tab:
        render_retention()
    with genre_tab:
        render_genre_benchmark()
    with health_tab:
        render_pipeline_health()
except psycopg.Error as exc:
    st.error(f"Could not query the analytics database: {exc}")
    st.stop()
