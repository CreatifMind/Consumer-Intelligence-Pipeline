from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# ==========================================
# 1. CONFIG & THEME
# ==========================================
st.set_page_config(
    page_title="Consumer Intelligence Platform",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

def apply_theme() -> None:
    st.markdown(
        """
        <style>
            .stApp { background: radial-gradient(circle at top left, rgba(18, 83, 137, 0.12), transparent 28%), linear-gradient(180deg, #f4f7fb 0%, #eef3f8 100%); }
            section[data-testid="stSidebar"] { background: linear-gradient(180deg, #0f172a 0%, #172554 100%); border-right: 1px solid rgba(255, 255, 255, 0.08); }
            section[data-testid="stSidebar"] * { color: #f8fafc; }
            .block-container { padding-top: 2rem; padding-bottom: 2rem; }
            .hero-card { padding: 1.5rem 1.75rem; border-radius: 20px; background: linear-gradient(135deg, #0f172a 0%, #1d4ed8 100%); color: #ffffff; box-shadow: 0 18px 40px rgba(15, 23, 42, 0.18); margin-bottom: 1.5rem; }
            .hero-title { font-size: 2rem; font-weight: 700; margin-bottom: 0.35rem; letter-spacing: -0.02em; }
            .hero-subtitle { font-size: 1rem; color: rgba(255, 255, 255, 0.82); margin-bottom: 0; }
            .section-card { background: rgba(255, 255, 255, 0.78); border: 1px solid rgba(148, 163, 184, 0.18); border-radius: 18px; padding: 1rem 1.25rem; box-shadow: 0 14px 30px rgba(15, 23, 42, 0.06); }
            .insight-card { background: #ffffff; border-left: 4px solid #2563eb; border-radius: 14px; padding: 1rem 1.1rem; box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05); min-height: 132px; }
            .insight-label { color: #64748b; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.35rem; }
            .insight-value { color: #0f172a; font-size: 1.4rem; font-weight: 700; margin-bottom: 0.45rem; }
            .insight-copy { color: #334155; font-size: 0.95rem; margin-bottom: 0; }
            div[data-testid="stMetric"] { background: rgba(255, 255, 255, 0.88); border: 1px solid rgba(148, 163, 184, 0.18); padding: 1rem; border-radius: 18px; box-shadow: 0 14px 28px rgba(15, 23, 42, 0.06); }
        </style>
        """,
        unsafe_allow_html=True,
    )

def page_header(title: str, subtitle: str) -> None:
    st.markdown(f'<div class="hero-card"><div class="hero-title">{title}</div><p class="hero-subtitle">{subtitle}</p></div>', unsafe_allow_html=True)

def render_insight_card(label: str, value: str, body: str) -> None:
    st.markdown(f'<div class="insight-card"><div class="insight-label">{label}</div><div class="insight-value">{value}</div><p class="insight-copy">{body}</p></div>', unsafe_allow_html=True)

# ==========================================
# 2. DATABASE & QUERIES (PostgreSQL)
# ==========================================
def get_engine():
    return create_engine(st.secrets["DATABASE_URL"], pool_pre_ping=True)

@st.cache_data(show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)

def check_if_empty() -> bool:
    try:
        df = run_query("SELECT COUNT(*) as count FROM fact_reviews")
        return df.iloc[0]["count"] == 0
    except:
        return True # If table doesn't exist yet, it's empty

def load_executive_kpis() -> pd.Series:
    query = """
        SELECT
            (SELECT COUNT(*) FROM dim_product) AS total_products,
            (SELECT COUNT(*) FROM fact_reviews) AS total_reviews,
            (SELECT COUNT(*) FROM dim_topic) AS total_topics,
            (SELECT ROUND(CAST(AVG(topic_confidence) AS numeric), 4) FROM fact_reviews) AS average_topic_confidence
    """
    return run_query(query).iloc[0]

def load_topic_distribution() -> pd.DataFrame:
    query = """
        SELECT
            t.topic_name AS "Topic_Label",
            COUNT(*) AS "Review_Count",
            ROUND(CAST(AVG(f.topic_confidence) AS numeric), 4) AS "Avg_Confidence",
            ROUND(CAST(AVG(f.star_rating) AS numeric), 2) AS "Avg_Rating"
        FROM fact_reviews AS f
        INNER JOIN dim_topic AS t ON f.topic_key = t.topic_key
        GROUP BY t.topic_name
        ORDER BY "Review_Count" DESC, t.topic_name
    """
    return run_query(query)

def load_pricing_snapshot() -> pd.DataFrame:
    query = """
        SELECT
            p.product_name AS "Product_Name",
            p.current_price AS "Current_Price",
            ROUND(CAST(AVG(f.star_rating) AS numeric), 2) AS "Avg_Rating",
            COUNT(f.review_key) AS "Review_Count",
            MAX(t.topic_name) AS "Topic_Label"
        FROM dim_product AS p
        LEFT JOIN fact_reviews AS f ON p.product_key = f.product_key
        LEFT JOIN dim_topic AS t ON f.topic_key = t.topic_key
        GROUP BY p.product_key, p.product_name, p.current_price
        ORDER BY p.current_price DESC, p.product_name
    """
    return run_query(query)

# ==========================================
# 3. SIDEBAR & PIPELINE ORCHESTRATOR
# ==========================================
def render_sidebar() -> str:
    with st.sidebar:
        st.markdown("## Consumer Intelligence")
        st.caption("End-to-End Cloud Data Pipeline")
        st.divider()

        page = st.radio("Navigation", ["Executive Summary", "Consumer Sentiment", "Pricing Intelligence"])
        st.divider()

        st.markdown("### 🚀 Run Product Test")
        with st.form("pipeline_form"):
            url_input = st.text_input("Enter Retail URL", placeholder="https://books.toscrape.com/...")
            submit = st.form_submit_button("Start Analysis")

            if submit:
                if url_input:
                    with st.spinner("🚀 Product Intelligence Engine Started..."):
                        root_path = os.path.dirname(__file__)
                        scraper_path = os.path.join(root_path, "src", "scraper.py")
                        nlp_path = os.path.join(root_path, "src", "nlp_processor.py")
                        db_path = os.path.join(root_path, "src", "db_connector.py")

                        # Ensure directories exist
                        os.makedirs(os.path.join(root_path, "data", "raw"), exist_ok=True)
                        os.makedirs(os.path.join(root_path, "data", "processed"), exist_ok=True)

                        # Step 1: Scraper
                        st.text("Step 1: Extracting Live Data...")
                        scrape_result = subprocess.run([sys.executable, scraper_path, url_input], capture_output=True, text=True)
                        if scrape_result.returncode != 0:
                            st.error(f"Scraper Error:\n{scrape_result.stderr}\n{scrape_result.stdout}")
                            st.stop()

                        # Step 2: NLP
                        st.text("Step 2: Analyzing Sentiment...")
                        nlp_result = subprocess.run([sys.executable, nlp_path], capture_output=True, text=True)
                        if nlp_result.returncode != 0:
                            st.error(f"NLP Error:\n{nlp_result.stderr}\n{nlp_result.stdout}")
                            st.stop()

                        # Step 3: DB
                        st.text("Step 3: Syncing to Warehouse...")
                        db_result = subprocess.run([sys.executable, db_path], capture_output=True, text=True)
                        if db_result.returncode != 0:
                            st.error(f"Database Error:\n{db_result.stderr}\n{db_result.stdout}")
                            st.stop()

                        st.success("Analysis Complete!")
                        st.balloons()
                        st.cache_data.clear()
                        st.rerun()
                else:
                    st.warning("Please enter a valid URL first!")
    return page

# ==========================================
# 4. PAGE RENDERS
# ==========================================
def render_executive_summary() -> None:
    kpis = load_executive_kpis()
    topic_df = load_topic_distribution()
    pricing_df = load_pricing_snapshot()

    page_header("Executive Summary", "Live leadership view of product coverage, review volume, topic mix, and pricing signals.")

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
        chart_df = topic_df.sort_values("Review_Count", ascending=True)
        if not chart_df.empty:
            fig = px.bar(chart_df, x="Review_Count", y="Topic_Label", color="Avg_Confidence", orientation="h", color_continuous_scale="Blues", text="Review_Count")
            fig.update_traces(textposition="outside")
            fig.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", xaxis_title="Reviews", yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.subheader("Strategic Signals")
        if not topic_df.empty and not pricing_df.empty:
            top_topic = topic_df.iloc[0]
            most_confident_topic = topic_df.sort_values("Avg_Confidence", ascending=False).iloc[0]
            priciest_product = pricing_df.iloc[0]

            render_insight_card("Most Discussed Topic", str(top_topic["Topic_Label"]), f"{int(top_topic['Review_Count'])} reviews mapped to this theme.")
            st.write("")
            render_insight_card("Highest Model Confidence", str(most_confident_topic["Topic_Label"]), f"Avg confidence is {float(most_confident_topic['Avg_Confidence']) * 100:.1f}%.")
            st.write("")
            render_insight_card("Highest Current Price", f"{str(priciest_product['Product_Name'])} (${float(priciest_product['Current_Price']):.2f})", "Sourced from dim_product.")

def render_consumer_sentiment() -> None:
    topic_df = load_topic_distribution()
    page_header("Consumer Sentiment", "Live NLP topic aggregation from the PostgreSQL warehouse.")

    if not topic_df.empty:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("Topic Distribution")
        fig = px.bar(topic_df, x="Topic_Label", y="Review_Count", color="Topic_Label", text="Review_Count", custom_data=["Avg_Confidence", "Avg_Rating"], color_discrete_sequence=["#1d4ed8", "#0f766e", "#f97316", "#dc2626", "#7c3aed"])
        fig.update_traces(textposition="outside", hovertemplate="<b>%{x}</b><br>Reviews: %{y}<br>Avg confidence: %{customdata[0]:.2%}<br>Avg rating: %{customdata[1]:.2f}<extra></extra>")
        fig.update_layout(height=440, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        st.write("")
        col1, col2, col3 = st.columns(3)
        most_discussed = topic_df.iloc[0]
        highest_confidence = topic_df.sort_values("Avg_Confidence", ascending=False).iloc[0]
        highest_rated = topic_df.sort_values("Avg_Rating", ascending=False).iloc[0]

        col1.metric("Most Discussed Topic", str(most_discussed["Topic_Label"]), f"{int(most_discussed['Review_Count'])} reviews")
        col2.metric("Highest Avg Confidence", str(highest_confidence["Topic_Label"]), f"{float(highest_confidence['Avg_Confidence']) * 100:.1f}%")
        col3.metric("Highest Avg Rating", str(highest_rated["Topic_Label"]), f"{float(highest_rated['Avg_Rating']):.2f} stars")
        st.dataframe(topic_df, use_container_width=True, hide_index=True)

def render_pricing_intelligence() -> None:
    pricing_df = load_pricing_snapshot()
    page_header("Pricing Intelligence", "Live pricing snapshot from dim_product.")

    if not pricing_df.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Average Listed Price", f"${pricing_df['Current_Price'].mean():.2f}")
        col2.metric("Highest Listed Price", f"${pricing_df['Current_Price'].max():.2f}")
        col3.metric("Lowest Listed Price", f"${pricing_df['Current_Price'].min():.2f}")

        st.write("")
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("Current Product Price Snapshot")

        fig = px.bar(pricing_df.sort_values("Current_Price", ascending=False), x="Product_Name", y="Current_Price", color="Avg_Rating", color_continuous_scale="Tealgrn", text="Current_Price", custom_data=["Topic_Label", "Review_Count"])
        fig.update_traces(texttemplate="$%{y:.2f}", textposition="outside", hovertemplate="<b>%{x}</b><br>Price: $%{y:.2f}<br>Avg rating: %{marker.color:.2f}<br>Topic: %{customdata[0]}<br>Reviews: %{customdata[1]}<extra></extra>")
        fig.update_layout(height=460, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", coloraxis_colorbar_title="Avg Rating")
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        st.dataframe(pricing_df, use_container_width=True, hide_index=True)

# ==========================================
# 5. MAIN APP LOOP
# ==========================================
def main() -> None:
    apply_theme()
    selected_page = render_sidebar()

    try:
        # Zero-State Logic
        if check_if_empty():
            st.info("👋 Welcome to the Consumer Intelligence Platform! The database is currently empty. Please enter a product URL in the sidebar to run your first analysis.")
        else:
            if selected_page == "Executive Summary":
                render_executive_summary()
            elif selected_page == "Consumer Sentiment":
                render_consumer_sentiment()
            else:
                render_pricing_intelligence()
    except Exception as e:
        st.error(f"Dashboard Error: {e}")

if __name__ == "__main__":
    main()
    