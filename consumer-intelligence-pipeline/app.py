from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


st.set_page_config(
    page_title="Consumer Intelligence Platform",
    page_icon="CI",
    layout="wide",
    initial_sidebar_state="expanded",
)


PROJECT_ROOT = Path(__file__).resolve().parent


def get_database_url() -> str:
    """Read the PostgreSQL connection string from Streamlit secrets."""
    try:
        return st.secrets["DATABASE_URL"]
    except Exception as exc:
        raise KeyError("Missing DATABASE_URL in Streamlit secrets.") from exc


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    """Create and reuse a SQLAlchemy engine for the PostgreSQL warehouse."""
    return create_engine(get_database_url(), pool_pre_ping=True)


def apply_theme() -> None:
    st.markdown(
        """
        <style>
            .stApp {
                background:
                    radial-gradient(circle at top left, rgba(18, 83, 137, 0.12), transparent 28%),
                    linear-gradient(180deg, #f4f7fb 0%, #eef3f8 100%);
            }
            section[data-testid="stSidebar"] {
                background: linear-gradient(180deg, #0f172a 0%, #172554 100%);
                border-right: 1px solid rgba(255, 255, 255, 0.08);
            }
            section[data-testid="stSidebar"] * {
                color: #f8fafc;
            }
            .block-container {
                padding-top: 2rem;
                padding-bottom: 2rem;
            }
            .hero-card {
                padding: 1.5rem 1.75rem;
                border-radius: 20px;
                background: linear-gradient(135deg, #0f172a 0%, #1d4ed8 100%);
                color: #ffffff;
                box-shadow: 0 18px 40px rgba(15, 23, 42, 0.18);
                margin-bottom: 1.5rem;
            }
            .hero-title {
                font-size: 2rem;
                font-weight: 700;
                margin-bottom: 0.35rem;
                letter-spacing: -0.02em;
            }
            .hero-subtitle {
                font-size: 1rem;
                color: rgba(255, 255, 255, 0.82);
                margin-bottom: 0;
            }
            .section-card {
                background: rgba(255, 255, 255, 0.78);
                border: 1px solid rgba(148, 163, 184, 0.18);
                border-radius: 18px;
                padding: 1rem 1.25rem;
                box-shadow: 0 14px 30px rgba(15, 23, 42, 0.06);
            }
            .insight-card {
                background: #ffffff;
                border-left: 4px solid #2563eb;
                border-radius: 14px;
                padding: 1rem 1.1rem;
                box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05);
                min-height: 132px;
            }
            .insight-label {
                color: #64748b;
                font-size: 0.78rem;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                margin-bottom: 0.35rem;
            }
            .insight-value {
                color: #0f172a;
                font-size: 1.4rem;
                font-weight: 700;
                margin-bottom: 0.45rem;
            }
            .insight-copy {
                color: #334155;
                font-size: 0.95rem;
                margin-bottom: 0;
            }
            div[data-testid="stMetric"] {
                background: rgba(255, 255, 255, 0.88);
                border: 1px solid rgba(148, 163, 184, 0.18);
                padding: 1rem;
                border-radius: 18px;
                box-shadow: 0 14px 28px rgba(15, 23, 42, 0.06);
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def page_header(title: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="hero-card">
            <div class="hero-title">{title}</div>
            <p class="hero-subtitle">{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    """Execute a cached SQL query against the PostgreSQL warehouse."""
    with get_engine().connect() as connection:
        return pd.read_sql_query(text(query), connection)


def run_pipeline(product_category: str) -> None:
    """Run the scraper, NLP processor, and database loader sequentially."""
    os.makedirs(PROJECT_ROOT / "data" / "raw", exist_ok=True)
    os.makedirs(PROJECT_ROOT / "data" / "processed", exist_ok=True)

    commands = [
        [sys.executable, "src/scraper.py"],
        [sys.executable, "src/nlp_processor.py"],
        [sys.executable, "src/db_connector.py"],
    ]
    env = os.environ.copy()
    env["PRODUCT_CATEGORY"] = product_category.strip()

    with st.spinner("Extracting and analyzing data..."):
        for command in commands:
            completed = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
            if completed.stdout.strip():
                st.session_state["latest_pipeline_log"] = completed.stdout.strip()

    st.cache_data.clear()
    st.rerun()


def load_executive_kpis() -> pd.Series:
    query = """
        SELECT
            (SELECT COUNT(*) FROM dim_product) AS total_products,
            (SELECT COUNT(*) FROM fact_reviews) AS total_reviews,
            (SELECT COUNT(*) FROM dim_topic) AS total_topics,
            (SELECT ROUND(AVG(star_rating)::numeric, 2) FROM fact_reviews) AS average_rating,
            (SELECT ROUND(AVG(topic_confidence)::numeric, 4) FROM fact_reviews) AS average_topic_confidence
    """
    return run_query(query).iloc[0]


def load_topic_distribution() -> pd.DataFrame:
    query = """
        SELECT
            t.topic_name AS topic_label,
            COUNT(*) AS review_count,
            ROUND(AVG(f.topic_confidence)::numeric, 4) AS avg_confidence,
            ROUND(AVG(f.star_rating)::numeric, 2) AS avg_rating
        FROM fact_reviews AS f
        INNER JOIN dim_topic AS t
            ON f.topic_key = t.topic_key
        GROUP BY t.topic_name
        ORDER BY review_count DESC, topic_label
    """
    return run_query(query)


def load_pricing_snapshot() -> pd.DataFrame:
    query = """
        SELECT
            p.product_name AS product_name,
            p.current_price AS current_price,
            ROUND(AVG(f.star_rating)::numeric, 2) AS avg_rating,
            COUNT(f.review_key) AS review_count,
            MAX(t.topic_name) AS topic_label
        FROM dim_product AS p
        LEFT JOIN fact_reviews AS f
            ON p.product_key = f.product_key
        LEFT JOIN dim_topic AS t
            ON f.topic_key = t.topic_key
        GROUP BY p.product_key, p.product_name, p.current_price
        ORDER BY current_price DESC, product_name
    """
    return run_query(query)


def load_last_refresh() -> str:
    query = """
        SELECT COALESCE(MAX(load_timestamp)::text, 'Unavailable') AS last_refresh
        FROM fact_reviews
    """
    return str(run_query(query).iloc[0]["last_refresh"])


def render_insight_card(label: str, value: str, body: str) -> None:
    st.markdown(
        f"""
        <div class="insight-card">
            <div class="insight-label">{label}</div>
            <div class="insight-value">{value}</div>
            <p class="insight-copy">{body}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def sidebar() -> str:
    with st.sidebar:
        st.markdown("## Consumer Intelligence")
        st.caption("Portfolio demo powered by a cloud-hosted PostgreSQL star schema")
        st.divider()

        page = st.radio(
            "Navigation",
            ["Executive Summary", "Consumer Sentiment", "Pricing Intelligence"],
            label_visibility="visible",
        )

        st.divider()
        with st.form("pipeline_runner_form"):
            product_category = st.text_input("Enter Product Category", value="wireless earbuds")
            submitted = st.form_submit_button("Run Intelligence Pipeline")

        if submitted:
            try:
                run_pipeline(product_category)
            except subprocess.CalledProcessError as exc:
                exc = exc.stderr.strip() or exc.stdout.strip() or str(exc)
                st.error(f"Pipeline failed: {exc}")
            except Exception as exc:
                st.error(f"Pipeline failed: {exc}")

        st.divider()
        st.markdown("### Warehouse Status")
        st.caption("Database: `PostgreSQL via st.secrets[\"DATABASE_URL\"]`")
        st.caption(f"Last refresh: `{load_last_refresh()}`")
        st.caption("Source tables: `dim_product`, `dim_topic`, `fact_reviews`")

    return page


def render_executive_summary() -> None:
    kpis = load_executive_kpis()
    topic_df = load_topic_distribution()
    pricing_df = load_pricing_snapshot()

    page_header(
        "Executive Summary",
        "A live leadership view of product coverage, review volume, topic mix, and pricing signals from the retail intelligence warehouse.",
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Products In Catalog", f"{int(kpis['total_products']):,}")
    col2.metric("Reviews In Warehouse", f"{int(kpis['total_reviews']):,}")
    col3.metric("Tracked Topics", f"{int(kpis['total_topics']):,}")
    col4.metric("Avg Topic Confidence", f"{float(kpis['average_topic_confidence']) * 100:.1f}%")

    st.write("")
    left, right = st.columns([1.4, 1])

    with left:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("Live Topic Mix")
        st.write(
            "This view aggregates the fact table by topic label so portfolio reviewers can see that the frontend is now reading directly from the star schema instead of using mock objects."
        )

        chart_df = topic_df.sort_values("review_count", ascending=True)
        fig = px.bar(
            chart_df,
            x="review_count",
            y="topic_label",
            color="avg_confidence",
            orientation="h",
            color_continuous_scale="Blues",
            text="review_count",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            coloraxis_colorbar_title="Confidence",
            xaxis_title="Reviews",
            yaxis_title="",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.subheader("Strategic Signals")

        top_topic = topic_df.iloc[0]
        most_confident_topic = topic_df.sort_values("avg_confidence", ascending=False).iloc[0]
        priciest_product = pricing_df.iloc[0]

        render_insight_card(
            "Most Discussed Topic",
            str(top_topic["topic_label"]),
            f"{int(top_topic['review_count'])} reviews mapped to this theme in fact_reviews.",
        )
        st.write("")
        render_insight_card(
            "Highest Model Confidence",
            str(most_confident_topic["topic_label"]),
            f"Average topic confidence is {float(most_confident_topic['avg_confidence']) * 100:.1f}% across assigned reviews.",
        )
        st.write("")
        render_insight_card(
            "Highest Current Price",
            f"{str(priciest_product['product_name'])} (${float(priciest_product['current_price']):.2f})",
            "This is sourced directly from dim_product and reflects the latest price snapshot currently stored in the warehouse.",
        )


def render_consumer_sentiment() -> None:
    topic_df = load_topic_distribution()

    page_header(
        "Consumer Sentiment",
        "Live NLP topic aggregation from the PostgreSQL warehouse, built by joining fact_reviews and dim_topic.",
    )

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Topic Distribution")

    fig = px.bar(
        topic_df,
        x="topic_label",
        y="review_count",
        color="topic_label",
        text="review_count",
        custom_data=["avg_confidence", "avg_rating"],
        color_discrete_sequence=["#1d4ed8", "#0f766e", "#f97316", "#dc2626", "#7c3aed"],
    )
    fig.update_traces(
        textposition="outside",
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Reviews: %{y}<br>"
            "Avg confidence: %{customdata[0]:.2%}<br>"
            "Avg rating: %{customdata[1]:.2f}<extra></extra>"
        ),
    )
    fig.update_layout(
        height=440,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        xaxis_title="Topic Label",
        yaxis_title="Review Count",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.write("")
    col1, col2, col3 = st.columns(3)

    most_discussed = topic_df.iloc[0]
    highest_confidence = topic_df.sort_values("avg_confidence", ascending=False).iloc[0]
    highest_rated = topic_df.sort_values("avg_rating", ascending=False).iloc[0]

    col1.metric("Most Discussed Topic", str(most_discussed["topic_label"]), f"{int(most_discussed['review_count'])} reviews")
    col2.metric(
        "Highest Avg Confidence",
        str(highest_confidence["topic_label"]),
        f"{float(highest_confidence['avg_confidence']) * 100:.1f}%",
    )
    col3.metric(
        "Highest Avg Rating",
        str(highest_rated["topic_label"]),
        f"{float(highest_rated['avg_rating']):.2f} stars",
    )

    st.dataframe(topic_df, use_container_width=True, hide_index=True)


def render_pricing_intelligence() -> None:
    pricing_df = load_pricing_snapshot()

    page_header(
        "Pricing Intelligence",
        "Live pricing snapshot from dim_product. A historical trend line can be added later by introducing a dedicated price history fact table.",
    )

    col1, col2, col3 = st.columns(3)
    col1.metric("Average Listed Price", f"${pricing_df['current_price'].mean():.2f}")
    col2.metric("Highest Listed Price", f"${pricing_df['current_price'].max():.2f}")
    col3.metric("Lowest Listed Price", f"${pricing_df['current_price'].min():.2f}")

    st.write("")
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Current Product Price Snapshot")

    fig = px.bar(
        pricing_df.sort_values("current_price", ascending=False),
        x="product_name",
        y="current_price",
        color="avg_rating",
        color_continuous_scale="Tealgrn",
        text="current_price",
        custom_data=["topic_label", "review_count"],
    )
    fig.update_traces(
        texttemplate="$%{y:.2f}",
        textposition="outside",
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Current price: $%{y:.2f}<br>"
            "Avg rating: %{marker.color:.2f}<br>"
            "Topic label: %{customdata[0]}<br>"
            "Reviews: %{customdata[1]}<extra></extra>"
        ),
    )
    fig.update_layout(
        height=460,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        coloraxis_colorbar_title="Avg Rating",
        xaxis_title="Product",
        yaxis_title="Current Price ($)",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.dataframe(pricing_df, use_container_width=True, hide_index=True)


def main() -> None:
    apply_theme()

    try:
        page = sidebar()

        if page == "Executive Summary":
            render_executive_summary()
        elif page == "Consumer Sentiment":
            render_consumer_sentiment()
        else:
            render_pricing_intelligence()
    except (KeyError, SQLAlchemyError) as exc:
        st.error(str(exc))
        st.info(
            "Configure `DATABASE_URL` in Streamlit secrets and run the intelligence pipeline to load the PostgreSQL warehouse."
        )
    except Exception as exc:
        st.error(f"Unable to load dashboard data: {exc}")


if __name__ == "__main__":
    main()
