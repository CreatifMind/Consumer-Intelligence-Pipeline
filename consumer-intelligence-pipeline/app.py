from __future__ import annotations

# Standard library imports for file paths and the local SQLite connection.
import sqlite3
from pathlib import Path

# Third-party imports for data access, visualization, and the Streamlit UI.
try:
    import pandas as pd  # type: ignore
except ImportError:
    pd = None

try:
    import plotly.express as px  # type: ignore
except ImportError:
    px = None

try:
    import streamlit as st  # type: ignore
except ImportError:
    st = None


# Keep the original page configuration so the app still feels like the same product.
if st is not None:
    st.set_page_config(
    page_title="Consumer Intelligence Platform",
    page_icon="CI",
    layout="wide",
    initial_sidebar_state="expanded",
)


# Resolve the database path relative to the project root so the app works regardless of
# where it is launched from inside the repository.
PROJECT_ROOT = Path(__file__).resolve().parent
DATABASE_PATH = PROJECT_ROOT / "retail_intelligence.db"


def apply_theme() -> None:
    """Apply the original visual theme so the production app keeps the polished look."""
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
    """Render a consistent header banner across all pages."""
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
    """
    Execute a cached SQL query against the local SQLite warehouse.

    Streamlit caches the returned DataFrame, which prevents repeat reads every time
    the user changes pages or interacts with the app.
    """
    if not DATABASE_PATH.exists():
        raise FileNotFoundError(
            f"SQLite database not found at {DATABASE_PATH}. Run src/db_connector.py first."
        )

    with sqlite3.connect(DATABASE_PATH) as connection:
        return pd.read_sql_query(query, connection)


def load_executive_kpis() -> pd.Series:
    """Load the KPI metrics used on the Executive Summary page."""
    query = """
        SELECT
            (SELECT COUNT(*) FROM Dim_Product) AS total_products,
            (SELECT COUNT(*) FROM Fact_Reviews) AS total_reviews,
            (SELECT COUNT(*) FROM Dim_Topic) AS total_topics,
            (SELECT ROUND(AVG(Star_Rating), 2) FROM Fact_Reviews) AS average_rating,
            (SELECT ROUND(AVG(Topic_Confidence), 4) FROM Fact_Reviews) AS average_topic_confidence
    """
    return run_query(query).iloc[0]


def load_topic_distribution() -> pd.DataFrame:
    """
    Load the live sentiment/topic breakdown by joining the fact table with the topic dimension.

    This is the required SQL aggregation for the Consumer Sentiment page.
    """
    query = """
        SELECT
            t.Topic_Name AS Topic_Label,
            COUNT(*) AS Review_Count,
            ROUND(AVG(f.Topic_Confidence), 4) AS Avg_Confidence,
            ROUND(AVG(f.Star_Rating), 2) AS Avg_Rating
        FROM Fact_Reviews AS f
        INNER JOIN Dim_Topic AS t
            ON f.Topic_Key = t.Topic_Key
        GROUP BY t.Topic_Name
        ORDER BY Review_Count DESC, t.Topic_Name
    """
    return run_query(query)


def load_pricing_snapshot() -> "pd.DataFrame":
    """
    Load a live pricing snapshot from the warehouse.

    The current star schema stores the latest known product price in Dim_Product, so this
    page presents a current-state pricing view instead of a historical trend line.
    """
    query = """
        SELECT
            p.Product_Name,
            p.Current_Price,
            ROUND(AVG(f.Star_Rating), 2) AS Avg_Rating,
            COUNT(f.Review_Key) AS Review_Count,
            MAX(t.Topic_Name) AS Topic_Label
        FROM Dim_Product AS p
        LEFT JOIN Fact_Reviews AS f
            ON p.Product_Key = f.Product_Key
        LEFT JOIN Dim_Topic AS t
            ON f.Topic_Key = t.Topic_Key
        GROUP BY p.Product_Key, p.Product_Name, p.Current_Price
        ORDER BY p.Current_Price DESC, p.Product_Name
    """
    return run_query(query)


def load_last_refresh() -> str:
    """Return the latest warehouse load timestamp to show freshness in the UI."""
    query = """
        SELECT COALESCE(MAX(Load_Timestamp), 'Unavailable') AS last_refresh
        FROM Fact_Reviews
    """
    return str(run_query(query).iloc[0]["last_refresh"])


def render_insight_card(label: str, value: str, body: str) -> None:
    """Render a reusable insight card on the right-hand side of dashboard pages."""
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
    """Render the original sidebar navigation, now backed by live warehouse metadata."""
    with st.sidebar:
        st.markdown("## Consumer Intelligence")
        st.caption("Portfolio demo powered by a local SQLite star schema")
        st.divider()

        page = st.radio(
            "Navigation",
            ["Executive Summary", "Consumer Sentiment", "Pricing Intelligence"],
            label_visibility="visible",
        )

        st.divider()
        st.markdown("### Warehouse Status")
        st.caption(f"Database: `{DATABASE_PATH.name}`")
        st.caption(f"Last refresh: `{load_last_refresh()}`")
        st.caption("Source tables: `Dim_Product`, `Dim_Topic`, `Fact_Reviews`")

    return page


def render_executive_summary() -> None:
    """Render the executive overview with live KPIs and topic highlights."""
    kpis = load_executive_kpis()
    topic_df = load_topic_distribution()
    pricing_df = load_pricing_snapshot()

    page_header(
        "Executive Summary",
        "A live leadership view of product coverage, review volume, topic mix, and pricing signals from the local retail intelligence warehouse.",
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

        chart_df = topic_df.sort_values("Review_Count", ascending=True)
        fig = px.bar(
            chart_df,
            x="Review_Count",
            y="Topic_Label",
            color="Avg_Confidence",
            orientation="h",
            color_continuous_scale="Blues",
            text="Review_Count",
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
        most_confident_topic = topic_df.sort_values("Avg_Confidence", ascending=False).iloc[0]
        priciest_product = pricing_df.iloc[0]

        render_insight_card(
            "Most Discussed Topic",
            str(top_topic["Topic_Label"]),
            f"{int(top_topic['Review_Count'])} reviews mapped to this theme in Fact_Reviews.",
        )
        st.write("")
        render_insight_card(
            "Highest Model Confidence",
            str(most_confident_topic["Topic_Label"]),
            f"Average topic confidence is {float(most_confident_topic['Avg_Confidence']) * 100:.1f}% across assigned reviews.",
        )
        st.write("")
        render_insight_card(
            "Highest Current Price",
            f"{str(priciest_product['Product_Name'])} (${float(priciest_product['Current_Price']):.2f})",
            "This is sourced directly from Dim_Product and reflects the latest price snapshot currently stored in the warehouse.",
        )


def render_consumer_sentiment() -> None:
    """Render the live topic distribution page backed by the warehouse join query."""
    topic_df = load_topic_distribution()

    page_header(
        "Consumer Sentiment",
        "Live NLP topic aggregation from the SQLite warehouse, built by joining Fact_Reviews and Dim_Topic.",
    )

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Topic Distribution")

    fig = px.bar(
        topic_df,
        x="Topic_Label",
        y="Review_Count",
        color="Topic_Label",
        text="Review_Count",
        custom_data=["Avg_Confidence", "Avg_Rating"],
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
    highest_confidence = topic_df.sort_values("Avg_Confidence", ascending=False).iloc[0]
    highest_rated = topic_df.sort_values("Avg_Rating", ascending=False).iloc[0]

    col1.metric("Most Discussed Topic", str(most_discussed["Topic_Label"]), f"{int(most_discussed['Review_Count'])} reviews")
    col2.metric(
        "Highest Avg Confidence",
        str(highest_confidence["Topic_Label"]),
        f"{float(highest_confidence['Avg_Confidence']) * 100:.1f}%",
    )
    col3.metric(
        "Highest Avg Rating",
        str(highest_rated["Topic_Label"]),
        f"{float(highest_rated['Avg_Rating']):.2f} stars",
    )

    st.dataframe(topic_df, use_container_width=True, hide_index=True)


def render_pricing_intelligence() -> None:
    """Render a live pricing snapshot from the product dimension."""
    pricing_df = load_pricing_snapshot()

    page_header(
        "Pricing Intelligence",
        "Live pricing snapshot from Dim_Product. A historical trend line can be added later by introducing a dedicated price history fact table.",
    )

    col1, col2, col3 = st.columns(3)
    col1.metric("Average Listed Price", f"${pricing_df['Current_Price'].mean():.2f}")
    col2.metric("Highest Listed Price", f"${pricing_df['Current_Price'].max():.2f}")
    col3.metric("Lowest Listed Price", f"${pricing_df['Current_Price'].min():.2f}")

    st.write("")
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Current Product Price Snapshot")

    fig = px.bar(
        pricing_df.sort_values("Current_Price", ascending=False),
        x="Product_Name",
        y="Current_Price",
        color="Avg_Rating",
        color_continuous_scale="Tealgrn",
        text="Current_Price",
        custom_data=["Topic_Label", "Review_Count"],
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
    """Application entry point."""
    apply_theme()

    try:
        page = sidebar()

        if page == "Executive Summary":
            render_executive_summary()
        elif page == "Consumer Sentiment":
            render_consumer_sentiment()
        else:
            render_pricing_intelligence()
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.info("Run `python3 src/db_connector.py` from the project root to build and load the warehouse.")
    except Exception as exc:
        st.error(f"Unable to load dashboard data: {exc}")


if __name__ == "__main__":
    main()
