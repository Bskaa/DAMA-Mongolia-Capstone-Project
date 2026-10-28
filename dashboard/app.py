from __future__ import annotations

import os
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import pandas as pd
import psycopg
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


def fetch_frame(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("Set POSTGRES_PASSWORD in the local .env before starting the dashboard.")

    with psycopg.connect(
        host=os.environ.get("DASHBOARD_DB_HOST", "localhost"),
        port=int(os.environ.get("DASHBOARD_DB_PORT", "5433")),
        dbname=os.environ.get("ANALYTICS_DB", "gaming_analytics"),
        user=os.environ.get("POSTGRES_USER", "gaming"),
        password=password,
        connect_timeout=5,
        options="-c default_transaction_read_only=on",
    ) as connection:
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


def load_dashboard_data(start_day: date, end_day: date, game_id: int | None, currency: str) -> dict[str, pd.DataFrame]:
    session_where, session_params = filtered_where("sessions", start_day, end_day, game_id)
    purchase_where, purchase_params = filtered_where("purchases", start_day, end_day, game_id)
    review_where, review_params = filtered_where("reviews", start_day, end_day, game_id)

    return {
        "kpis": fetch_frame(
            f"""
            SELECT COUNT(*) AS sessions,
                   COUNT(DISTINCT sessions.player_id) AS active_players,
                   COALESCE(AVG(sessions.duration_minutes), 0)::numeric(12, 2) AS avg_session_minutes
            FROM staging_core.fact_sessions AS sessions
            WHERE {session_where}
            """,
            session_params,
        ),
        "purchases": fetch_frame(
            f"""
            SELECT COUNT(*) AS purchases,
                   COALESCE(SUM(purchases.amount), 0)::numeric(14, 2) AS gross_amount
            FROM staging_core.fact_purchases AS purchases
            WHERE {purchase_where} AND purchases.currency = %s
            """,
            purchase_params + (currency,),
        ),
        "reviews": fetch_frame(
            f"""
            SELECT COUNT(*) AS reviews,
                   COALESCE(AVG(reviews.rating), 0)::numeric(6, 2) AS avg_rating
            FROM staging_core.fact_reviews AS reviews
            WHERE {review_where}
            """,
            review_params,
        ),
        "sessions_by_day": fetch_frame(
            f"""
            SELECT sessions.date_day, COUNT(*) AS sessions
            FROM staging_core.fact_sessions AS sessions
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
            FROM staging_core.fact_purchases AS purchases
            WHERE {purchase_where} AND purchases.currency = %s
            GROUP BY purchases.date_day
            ORDER BY purchases.date_day
            """,
            purchase_params + (currency,),
        ),
        "top_games": fetch_frame(
            f"""
            SELECT games.game_name, COUNT(*) AS sessions
            FROM staging_core.fact_sessions AS sessions
            JOIN staging_core.dim_games AS games USING (game_id)
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
            FROM staging_core.fact_sessions AS sessions
            JOIN staging_core.dim_platforms AS platforms USING (platform_id)
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
            FROM staging_core.fact_reviews AS reviews
            JOIN staging_core.dim_games AS games USING (game_id)
            WHERE {review_where}
            GROUP BY games.game_name
            ORDER BY reviews DESC, games.game_name
            LIMIT 10
            """,
            review_params,
        ),
    }


st.set_page_config(page_title="Gaming Analytics", page_icon="🎮", layout="wide")
st.title("Gaming Analytics")
st.caption("Engagement, purchase value, and player feedback from the dbt core models.")

try:
    date_bounds = fetch_frame(
        """
        SELECT MIN(date_day) AS first_day, MAX(date_day) AS last_day
        FROM (
            SELECT date_day FROM staging_core.fact_sessions
            UNION ALL
            SELECT date_day FROM staging_core.fact_purchases
            UNION ALL
            SELECT date_day FROM staging_core.fact_reviews
        ) AS activity
        """
    )
    games = fetch_frame(
        "SELECT game_id, game_name FROM staging_core.dim_games ORDER BY game_name"
    )
    currencies = fetch_frame(
        "SELECT DISTINCT currency FROM staging_core.fact_purchases ORDER BY currency"
    )
except (psycopg.Error, RuntimeError, ValueError) as exc:
    st.error(f"Could not load dashboard filters from the analytics database: {exc}")
    st.stop()

if date_bounds.empty or pd.isna(date_bounds.loc[0, "first_day"]):
    st.warning("No dbt core data is available yet. Run the analytics pipeline first.")
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
        {
            f"{row.game_name} (#{row.game_id})": int(row.game_id)
            for row in games.itertuples(index=False)
        }
    )
    selected_game = st.selectbox("Game", list(game_options))
    available_currencies = currencies["currency"].tolist()
    selected_currency = st.selectbox("Purchase currency", available_currencies)
    st.caption("Purchase values are kept in their original currency; currencies are never added together.")
    if st.button("Refresh data"):
        st.rerun()

if isinstance(selected_range, (tuple, list)):
    start_day = selected_range[0]
    end_day = selected_range[-1]
else:
    start_day = end_day = selected_range

try:
    data = load_dashboard_data(
        start_day,
        end_day,
        game_options[selected_game],
        selected_currency,
    )
except psycopg.Error as exc:
    st.error(f"Could not query the analytics database: {exc}")
    st.stop()

kpis = data["kpis"].iloc[0]
purchases = data["purchases"].iloc[0]
reviews = data["reviews"].iloc[0]

metric_columns = st.columns(4)
metric_columns[0].metric("Sessions", f"{int(kpis['sessions']):,}")
metric_columns[1].metric("Active players", f"{int(kpis['active_players']):,}")
metric_columns[2].metric("Avg. session", f"{float(kpis['avg_session_minutes']):,.1f} min")
metric_columns[3].metric(
    f"Purchase value ({selected_currency})",
    f"{float(purchases['gross_amount']):,.2f}",
    help=f"{int(purchases['purchases']):,} purchases in the selected currency.",
)

engagement_column, purchase_column = st.columns(2)
with engagement_column:
    st.subheader("Daily sessions")
    st.line_chart(data["sessions_by_day"], x="date_day", y="sessions")
with purchase_column:
    st.subheader(f"Daily purchase value ({selected_currency})")
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
