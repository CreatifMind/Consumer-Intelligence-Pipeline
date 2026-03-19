from __future__ import annotations

import os
import sys
import subprocess
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
            section[data-testid="stSidebar"] div[data-baseweb="input"] input {
                color: #0f172a !important;
                -webkit-text-fill-color: #0f172a !important;
                background: #ffffff !important;
                border-radius: 12px;
            }
            section[data-testid="stSidebar"] div[data-baseweb="input"] input::placeholder {
                color: #94a3b8 !important;
                opacity: 1;
            }
            section[data-testid="stSidebar"] div.stButton > button,
            section[data-testid="stSidebar"] div[data-testid="stFormSubmitButton"] > button {
                width: 100%;
                border-radius: 12px;
                border: 1px solid rgba(148, 163, 184, 0.32);
                background: rgba(255, 255, 255, 0.94);
                color: #0f172a !important;
                font-weight: 600;
            }
            section[data-testid="stSidebar"] button,
            section[data-testid="stSidebar"] button *,
            section[data-testid="stSidebar"] button p,
            section[data-testid="stSidebar"] button span,
            section[data-testid="stSidebar"] button div {
                color: #0f172a !important;
                -webkit-text-fill-color: #0f172a !important;
            }
            section[data-testid="stSidebar"] div.stButton > button[kind="primary"],
            section[data-testid="stSidebar"] div[data-testid="stFormSubmitButton"] > button[kind="primary"] {
                background: linear-gradient(135deg, #dbeafe 0%, #bfdbfe 100%);
                color: #0f172a !important;
                border-color: rgba(59, 130, 246, 0.45);
                box-shadow: 0 10px 22px rgba(15, 23, 42, 0.16);
            }
            section[data-testid="stSidebar"] div.stButton > button:hover,
            section[data-testid="stSidebar"] div[data-testid="stFormSubmitButton"] > button:hover {
                border-color: rgba(59, 130, 246, 0.48);
                color: #0f172a !important;
            }
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
            .empty-state {
                max-width: 860px;
                margin: 5rem auto 0 auto;
                padding: 2.25rem 2.4rem;
                border-radius: 22px;
                background: rgba(255, 255, 255, 0.92);
                box-shadow: 0 18px 36px rgba(15, 23, 42, 0.08);
            }
            .home-copy {
                color: #334155;
                font-size: 1rem;
                line-height: 1.75;
            }
            .architecture-step {
                background: rgba(255, 255, 255, 0.9);
                border: 1px solid rgba(148, 163, 184, 0.16);
                border-radius: 18px;
                padding: 1.15rem 1.2rem;
                box-shadow: 0 10px 22px rgba(15, 23, 42, 0.05);
                min-height: 210px;
            }
            .architecture-step h4 {
                margin-bottom: 0.55rem;
                color: #0f172a;
            }
            .architecture-step p {
                color: #334155;
                margin-bottom: 0;
                line-height: 1.7;
            }
            .sidebar-title {
                color: #f8fafc;
                font-size: 1.28rem;
                font-weight: 700;
                margin-bottom: 0.08rem;
                line-height: 1.15;
            }
            .sidebar-subtitle {
                color: rgba(248, 250, 252, 0.72);
                font-size: 0.83rem;
                margin-bottom: 0.45rem;
                line-height: 1.35;
            }
            .sidebar-rule {
                border: 0;
                border-top: 1px solid rgba(255, 255, 255, 0.12);
                margin: 0.45rem 0 0.6rem 0;
            }
            .sidebar-section-heading {
                color: #e2e8f0;
                font-size: 0.94rem;
                font-weight: 700;
                margin: 0.1rem 0 0.35rem 0;
                line-height: 1.2;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

def page_header(title: str, subtitle: str) -> None:
    st.markdown(f'<div class="hero-card"><div class="hero-title">{title}</div><p class="hero-subtitle">{subtitle}</p></div>', unsafe_allow_html=True)

def render_insight_card(label: str, value: str, body: str) -> None:
    st.markdown(f'<div class="insight-card"><div class="insight-label">{label}</div><div class="insight-value">{value}</div><p class="insight-copy">{body}</p></div>', unsafe_allow_html=True)


PAGES = [
    "Home",
    "Executive Summary",
    "Consumer Sentiment",
    "Pricing Intelligence",
]

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
            t.topic_name AS "Topic Label",
            COUNT(*) AS "Review Count",
            ROUND(CAST(AVG(f.topic_confidence) AS numeric), 4) AS "Average Confidence",
            ROUND(CAST(AVG(f.star_rating) AS numeric), 2) AS "Average Rating"
        FROM fact_reviews AS f
        INNER JOIN dim_topic AS t ON f.topic_key = t.topic_key
        GROUP BY t.topic_name
        ORDER BY "Review Count" DESC, t.topic_name
    """
    return run_query(query)

def load_pricing_snapshot() -> pd.DataFrame:
    query = """
        SELECT
            p.product_name AS "Product Name",
            p.current_price AS "Current Price",
            ROUND(CAST(AVG(f.star_rating) AS numeric), 2) AS "Average Rating",
            COUNT(f.review_key) AS "Review Count",
            MAX(t.topic_name) AS "Topic Label"
        FROM dim_product AS p
        LEFT JOIN fact_reviews AS f ON p.product_key = f.product_key
        LEFT JOIN dim_topic AS t ON f.topic_key = t.topic_key
        GROUP BY p.product_key, p.product_name, p.current_price
        ORDER BY p.current_price DESC, p.product_name
    """
    return run_query(query)


def load_export_results() -> pd.DataFrame:
    query = """
        SELECT
            p.product_name AS "Product Name",
            p.current_price AS "Current Price",
            f.review_text AS "Review Text",
            f.star_rating AS "Star Rating",
            t.topic_name AS "Topic Label",
            ROUND(CAST(f.topic_confidence AS numeric), 4) AS "Topic Confidence",
            f.load_timestamp AS "Load Timestamp"
        FROM fact_reviews AS f
        INNER JOIN dim_product AS p ON f.product_key = p.product_key
        INNER JOIN dim_topic AS t ON f.topic_key = t.topic_key
        ORDER BY f.load_timestamp DESC, p.product_name
    """
    return run_query(query)


def dashboard_view_is_cleared() -> bool:
    return st.session_state.get("dashboard_cleared", False)

# ==========================================
# 3. SIDEBAR & PIPELINE ORCHESTRATOR
# ==========================================
def initialize_page_state() -> None:
    if "selected_page" not in st.session_state:
        st.session_state["selected_page"] = "Home"
    if "pipeline_url_input" not in st.session_state:
        st.session_state["pipeline_url_input"] = ""
    if "dashboard_cleared" not in st.session_state:
        st.session_state["dashboard_cleared"] = False


def render_navigation_buttons() -> str:
    for page_name in PAGES:
        is_active = st.session_state["selected_page"] == page_name
        if st.button(page_name, key=f"nav_{page_name.lower().replace(' ', '_')}", use_container_width=True, type="primary" if is_active else "secondary"):
            st.session_state["selected_page"] = page_name
            st.rerun()

    return st.session_state["selected_page"]

def render_sidebar() -> str:
    # --- CALLBACK FUNCTION TO PREVENT STATE ERRORS ---
    def reset_app():
        st.session_state["pipeline_url_input"] = ""
        st.session_state["dashboard_cleared"] = True
        st.cache_data.clear()

    with st.sidebar:
        st.markdown('<div class="sidebar-title">Consumer Intelligence</div>', unsafe_allow_html=True)
        st.markdown('<div class="sidebar-subtitle">End-to-End Cloud Data Pipeline</div>', unsafe_allow_html=True)
        st.markdown('<hr class="sidebar-rule">', unsafe_allow_html=True)

        st.markdown('<div class="sidebar-section-heading">Navigation</div>', unsafe_allow_html=True)
        page = render_navigation_buttons()
        st.markdown('<hr class="sidebar-rule">', unsafe_allow_html=True)

        st.markdown('<div class="sidebar-section-heading">Run Product Test</div>', unsafe_allow_html=True)
        with st.form("pipeline_form"):
            url_input = st.text_input(
                "Enter Retailer URL",
                key="pipeline_url_input",
                placeholder="Enter Retailer URL",
                label_visibility="collapsed",
            )
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

                        st.session_state["dashboard_cleared"] = False
                        st.success("Analysis Complete!")
                        st.balloons()
                        st.cache_data.clear()
                        st.rerun()
                else:
                    st.warning("Please enter a valid URL first!")

        st.write("")
        # Use the callback function instead of standard execution flow
        st.button("Refresh for Next URL", use_container_width=True, on_click=reset_app)

        st.markdown('<hr class="sidebar-rule">', unsafe_allow_html=True)
        st.markdown('<div class="sidebar-section-heading">Download Results</div>', unsafe_allow_html=True)
        if dashboard_view_is_cleared():
            st.caption("Run a new analysis to download the latest results.")
        else:
            try:
                export_df = load_export_results()
            except Exception:
                export_df = pd.DataFrame()

            if export_df.empty:
                st.caption("Run the pipeline first to unlock CSV download.")
            else:
                st.download_button(
                    "Download Latest Results",
                    data=export_df.to_csv(index=False),
                    file_name="consumer_intelligence_results.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
    return page

# ==========================================
# 4. PAGE RENDERS
# ==========================================
def render_home_page() -> None:
    page_header(
        "Consumer Intelligence Pipeline",
        "An end-to-end, cloud-hosted data pipeline and leadership dashboard for live consumer sentiment and pricing intelligence.",
    )

    overview_tab, architecture_tab, warning_tab = st.tabs(["Overview", "Architecture", "Demo Limitations"])

    with overview_tab:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown("### Project Title")
        st.markdown('<p class="home-copy"><strong>Consumer Intelligence Pipeline</strong></p>', unsafe_allow_html=True)
        st.markdown("### What It Is")
        st.markdown(
            """
            <p class="home-copy">
                An end-to-end, cloud-hosted data pipeline and interactive dashboard that automates the extraction,
                natural language processing (NLP), and visualization of retail product data. It is designed to give
                business leaders a real-time, quantitative view of consumer sentiment and pricing intelligence without
                requiring manual data entry.
            </p>
            """,
            unsafe_allow_html=True,
        )
        st.info("Paste a supported product link into the sidebar and click Start Analysis to populate the live dashboard.")
        st.markdown("</div>", unsafe_allow_html=True)

    with architecture_tab:
        first_row = st.columns(2)
        second_row = st.columns(2)

        with first_row[0]:
            st.markdown(
                """
                <div class="architecture-step">
                    <h4>Step 1: Dynamic Data Extraction (Python / BeautifulSoup)</h4>
                    <p>
                        The pipeline features a "Universal Scraper" that accepts retail URLs. It bypasses basic anti-bot
                        measures to extract product titles, current pricing, and raw consumer review text. It includes
                        intelligent fallback routing to handle varying HTML structures (like Shopify or standard SSR sites)
                        and sparse data scenarios.
                    </p>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with first_row[1]:
            st.markdown(
                """
                <div class="architecture-step">
                    <h4>Step 2: Machine Learning & NLP (Scikit-Learn)</h4>
                    <p>
                        Raw text is normalized and passed through a Latent Dirichlet Allocation (LDA) model. The algorithm
                        mathematically groups words to assign each review to a core business topic (Quality, Pricing, or
                        Delivery) and calculates a model confidence score.
                    </p>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with second_row[0]:
            st.markdown(
                """
                <div class="architecture-step">
                    <h4>Step 3: Cloud Data Warehousing (PostgreSQL / SQLAlchemy)</h4>
                    <p>
                        The processed data is structurally transformed and loaded into a cloud-hosted PostgreSQL database.
                        The data is organized into a classic Star Schema (Fact and Dimension tables) utilizing TRUNCATE
                        logic to ensure the dashboard always reflects the most current product snapshot.
                    </p>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with second_row[1]:
            st.markdown(
                """
                <div class="architecture-step">
                    <h4>Step 4: Real-Time Visualization (Streamlit / Plotly)</h4>
                    <p>
                        The frontend acts as a live leadership dashboard. It executes direct SQL queries against the cloud
                        warehouse to render interactive charts, displaying aggregate review volume, topic distribution,
                        sentiment confidence, and a live pricing snapshot.
                    </p>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with warning_tab:
        st.markdown("### Suggested Test URLs for Reviewers")

        st.markdown("**1. The Sandbox Sites (100% Success Rate)**")
        st.caption("These sites are specifically built for scraping. They use standard HTML structures and are the safest way to demonstrate the full pipeline from extraction through fallback NLP logic.")
        st.markdown("Product (Book)")
        st.code("https://books.toscrape.com/", language=None)

        st.markdown("**2. Independent E-Commerce Brands (Stable Real-World Test)**")
        st.caption("Many modern brands run on platforms such as Shopify with server-side rendered pages. These are strong real-world candidates for this pipeline's extraction engine.")
        st.markdown("NuPhy (Tech Accessories)")
        st.code("https://nuphy.com/collections/keyboards/products", language=None)
        st.markdown("Allbirds (Apparel)")
        st.code("https://www.allbirds.com/products", language=None)

        st.markdown("**3. The High-Stakes Test (Enterprise Marketplaces)**")
        st.caption("Large marketplaces often use aggressive anti-bot services such as Datadome or Cloudflare. The pipeline includes realistic headers, but cloud-hosted IPs can still be rate-limited or blocked.")
        st.markdown("Amazon")
        st.code("https://www.amazon.com", language=None)

        st.warning("The following retailer links will not work on this demo site.")
        st.markdown(
            """
            <div class="section-card">
                <p class="home-copy"><strong>Unsupported Demo Sources</strong></p>
                <p class="home-copy">Shopee & Lazada</p>
                <p class="home-copy">Walmart & Target</p>
                <p class="home-copy">AliExpress & Temu</p>
                <p class="home-copy">Nike & Adidas</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.info("Note on e-commerce security: this pipeline is optimized for server-side rendered HTML. Modern single page applications heavily reliant on JavaScript rendering, such as Shopee or Lazada, intentionally block automated cloud extraction and are outside the scope of this demonstration.")


def render_empty_dashboard_state() -> None:
    st.markdown(
        """
        <div class="empty-state">
            <h2 style="margin-top: 0; color: #0f172a; text-align: center;">Dashboard Waiting for First Run</h2>
            <p style="margin-bottom: 0; color: #475569; font-size: 1.05rem; text-align: center; line-height: 1.8;">
                The warehouse is still empty. Enter a supported product URL in the sidebar and click Start Analysis to
                populate the dashboard with live intelligence.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_executive_summary() -> None:
    page_header("Executive Summary", "Live leadership view of product coverage, review volume, topic mix, and pricing signals.")

    if dashboard_view_is_cleared():
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Products In Catalog", "0")
        col2.metric("Reviews In Warehouse", "0")
        col3.metric("Tracked Topics", "0")
        col4.metric("Avg Topic Confidence", "0.0%")

        st.write("")
        left, right = st.columns([1.4, 1])

        with left:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("Live Topic Mix")
            st.info("No live data is currently displayed. Paste a new URL and click Start Analysis to repopulate this view.")
            st.markdown("</div>", unsafe_allow_html=True)

        with right:
            st.subheader("Strategic Signals")
            render_insight_card("Most Discussed Topic", "-", "")
            st.write("")
            render_insight_card("Highest Model Confidence", "-", "")
            st.write("")
            render_insight_card("Highest Current Price", "-", "")
        return

    kpis = load_executive_kpis()
    topic_df = load_topic_distribution()
    pricing_df = load_pricing_snapshot()

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
        chart_df = topic_df.sort_values("Review Count", ascending=True)
        if not chart_df.empty:
            fig = px.bar(
                chart_df,
                x="Review Count",
                y="Topic Label",
                color="Average Confidence",
                orientation="h",
                color_continuous_scale="Blues",
                text="Review Count",
            )
            fig.update_traces(textposition="outside")
            fig.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", xaxis_title="Reviews", yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.subheader("Strategic Signals")
        if not topic_df.empty and not pricing_df.empty:
            top_topic = topic_df.iloc[0]
            most_confident_topic = topic_df.sort_values("Average Confidence", ascending=False).iloc[0]
            priciest_product = pricing_df.iloc[0]

            render_insight_card("Most Discussed Topic", str(top_topic["Topic Label"]), f"{int(top_topic['Review Count'])} reviews mapped to this theme.")
            st.write("")
            render_insight_card("Highest Model Confidence", str(most_confident_topic["Topic Label"]), f"Avg confidence is {float(most_confident_topic['Average Confidence']) * 100:.1f}%.")
            st.write("")
            render_insight_card("Highest Current Price", f"{str(priciest_product['Product Name'])} (${float(priciest_product['Current Price']):.2f})", "Sourced from the product dimension.")

def render_consumer_sentiment() -> None:
    page_header("Consumer Sentiment", "Live NLP topic aggregation from the PostgreSQL warehouse.")

    if dashboard_view_is_cleared():
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("Topic Distribution")
        st.info("No sentiment results are currently displayed. Run a new analysis to generate topic-level review insights.")
        st.markdown("</div>", unsafe_allow_html=True)

        st.write("")
        col1, col2, col3 = st.columns(3)
        col1.metric("Most Discussed Topic", "-", "0 reviews")
        col2.metric("Highest Average Confidence", "-", "0.0%")
        col3.metric("Highest Average Rating", "-", "0.00 stars")
        st.dataframe(pd.DataFrame(columns=["Topic Label", "Review Count", "Average Confidence", "Average Rating"]), use_container_width=True, hide_index=True)
        return

    topic_df = load_topic_distribution()

    if not topic_df.empty:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("Topic Distribution")
        fig = px.bar(
            topic_df,
            x="Topic Label",
            y="Review Count",
            color="Topic Label",
            text="Review Count",
            custom_data=["Average Confidence", "Average Rating"],
            color_discrete_sequence=["#1d4ed8", "#0f766e", "#f97316", "#dc2626", "#7c3aed"],
        )
        fig.update_traces(textposition="outside", hovertemplate="<b>%{x}</b><br>Reviews: %{y}<br>Avg confidence: %{customdata[0]:.2%}<br>Avg rating: %{customdata[1]:.2f}<extra></extra>")
        fig.update_layout(height=440, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        st.write("")
        col1, col2, col3 = st.columns(3)
        most_discussed = topic_df.iloc[0]
        highest_confidence = topic_df.sort_values("Average Confidence", ascending=False).iloc[0]
        highest_rated = topic_df.sort_values("Average Rating", ascending=False).iloc[0]

        col1.metric("Most Discussed Topic", str(most_discussed["Topic Label"]), f"{int(most_discussed['Review Count'])} reviews")
        col2.metric("Highest Average Confidence", str(highest_confidence["Topic Label"]), f"{float(highest_confidence['Average Confidence']) * 100:.1f}%")
        col3.metric("Highest Average Rating", str(highest_rated["Topic Label"]), f"{float(highest_rated['Average Rating']):.2f} stars")
        st.dataframe(topic_df, use_container_width=True, hide_index=True)

def render_pricing_intelligence() -> None:
    page_header("Pricing Intelligence", "Live pricing snapshot from dim_product.")

    if dashboard_view_is_cleared():
        col1, col2, col3 = st.columns(3)
        col1.metric("Average Listed Price", "$0.00")
        col2.metric("Highest Listed Price", "$0.00")
        col3.metric("Lowest Listed Price", "$0.00")

        st.write("")
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("Current Product Price Snapshot")
        st.info("No pricing snapshot is currently displayed. Run a new analysis to compare the next product set.")
        st.markdown("</div>", unsafe_allow_html=True)
        st.dataframe(pd.DataFrame(columns=["Product Name", "Current Price", "Average Rating", "Review Count", "Topic Label"]), use_container_width=True, hide_index=True)
        return

    pricing_df = load_pricing_snapshot()

    if not pricing_df.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Average Listed Price", f"${pricing_df['Current Price'].mean():.2f}")
        col2.metric("Highest Listed Price", f"${pricing_df['Current Price'].max():.2f}")
        col3.metric("Lowest Listed Price", f"${pricing_df['Current Price'].min():.2f}")

        st.write("")
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("Current Product Price Snapshot")

        fig = px.bar(
            pricing_df.sort_values("Current Price", ascending=False),
            x="Product Name",
            y="Current Price",
            color="Average Rating",
            color_continuous_scale="Tealgrn",
            text="Current Price",
            custom_data=["Topic Label", "Review Count"],
        )
        fig.update_traces(texttemplate="$%{y:.2f}", textposition="outside", hovertemplate="<b>%{x}</b><br>Price: $%{y:.2f}<br>Avg rating: %{marker.color:.2f}<br>Topic: %{customdata[0]}<br>Reviews: %{customdata[1]}<extra></extra>")
        fig.update_layout(height=460, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", coloraxis_colorbar_title="Average Rating")
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        st.dataframe(pricing_df, use_container_width=True, hide_index=True)

# ==========================================
# 5. MAIN APP LOOP
# ==========================================
def main() -> None:
    apply_theme()
    initialize_page_state()
    selected_page = render_sidebar()

    try:
        warehouse_is_empty = check_if_empty()

        if selected_page == "Home":
            render_home_page()
        elif warehouse_is_empty:
            render_empty_dashboard_state()
        elif selected_page == "Executive Summary":
            render_executive_summary()
        elif selected_page == "Consumer Sentiment":
            render_consumer_sentiment()
        else:
            render_pricing_intelligence()
    except Exception as e:
        st.error(f"Dashboard Error: {e}")

if __name__ == "__main__":
    main()
    