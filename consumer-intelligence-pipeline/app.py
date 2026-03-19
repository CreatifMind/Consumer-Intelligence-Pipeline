from __future__ import annotations
import os
import sys
import subprocess
from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# 1. Page Config & Theme (Preserved from your original)
st.set_page_config(
    page_title="Consumer Intelligence Platform",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

def apply_theme():
    st.markdown("""
        <style>
            .stApp { background: radial-gradient(circle at top left, rgba(18, 83, 137, 0.12), transparent 28%), linear-gradient(180deg, #f4f7fb 0%, #eef3f8 100%); }
            section[data-testid="stSidebar"] { background: linear-gradient(180deg, #0f172a 0%, #172554 100%); border-right: 1px solid rgba(255, 255, 255, 0.08); }
            section[data-testid="stSidebar"] * { color: #f8fafc; }
            .hero-card { padding: 1.5rem 1.75rem; border-radius: 20px; background: linear-gradient(135deg, #0f172a 0%, #1d4ed8 100%); color: #ffffff; box-shadow: 0 18px 40px rgba(15, 23, 42, 0.18); margin-bottom: 1.5rem; }
            .hero-title { font-size: 2rem; font-weight: 700; margin-bottom: 0.35rem; letter-spacing: -0.02em; }
            .section-card { background: rgba(255, 255, 255, 0.78); border: 1px solid rgba(148, 163, 184, 0.18); border-radius: 18px; padding: 1rem 1.25rem; box-shadow: 0 14px 30px rgba(15, 23, 42, 0.06); }
            .insight-card { background: #ffffff; border-left: 4px solid #2563eb; border-radius: 14px; padding: 1rem 1.1rem; box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05); min-height: 132px; }
        </style>
    """, unsafe_allow_html=True)

# 2. Database Connection (Upgraded to Cloud PostgreSQL)
def get_engine():
    # Uses Streamlit Secrets on Cloud, or .streamlit/secrets.toml locally
    return create_engine(st.secrets["DATABASE_URL"])

@st.cache_data(show_spinner=False)
def run_query(query):
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)

# 3. Pipeline Orchestrator (The "Product Test" Engine)
def run_pipeline(url):
    root = Path(__file__).resolve().parent
    # Ensure folders exist for the Cloud Server
    (root / "data/raw").mkdir(parents=True, exist_ok=True)
    (root / "data/processed").mkdir(parents=True, exist_ok=True)
    
    scripts = [
        (root / "src/scraper.py", [url]),
        (root / "src/nlp_processor.py", []),
        (root / "src/db_connector.py", [])
    ]
    
    for script, args in scripts:
        st.write(f"⚙️ Running {script.name}...")
        subprocess.run([sys.executable, str(script)] + args, check=True)

# 4. Sidebar & Form
def render_sidebar():
    with st.sidebar:
        st.title("Consumer Intelligence")
        st.caption("End-to-End Cloud Data Pipeline")
        
        # NAVIGATION
        page = st.radio("Navigation", ["Executive Summary", "Consumer Sentiment", "Pricing Intelligence"])
        
        st.divider()
        
        # PRODUCT TEST FORM
        st.markdown("### 🚀 Run Product Test")
        with st.form("pipeline_form"):
            url_input = st.text_input("Enter Retail URL", placeholder="https://amazon.com/...")
            submit = st.form_submit_button("Start Analysis")
            
            if submit:
                try:
                    with st.spinner("🚀 Product Intelligence Engine Started..."):
                        root_path = os.path.dirname(__file__)
                        scraper_path = os.path.join(root_path, "src", "scraper.py")
                        nlp_path = os.path.join(root_path, "src", "nlp_processor.py")
                        db_path = os.path.join(root_path, "src", "db_connector.py")

                        # 1. Scraper
                        st.text("Step 1: Extracting Live Data...")
                        scrape_result = subprocess.run([sys.executable, scraper_path, user_url_input], capture_output=True, text=True)
                        if scrape_result.returncode != 0:
                            st.error(f"Scraper Error:\n{scrape_result.stdout}\n{scrape_result.stderr}")
                            st.stop()

                        # 2. NLP
                        st.text("Step 2: Analyzing Consumer Sentiment...")
                        nlp_result = subprocess.run([sys.executable, nlp_path], capture_output=True, text=True)
                        if nlp_result.returncode != 0:
                            st.error(f"NLP Error:\n{nlp_result.stdout}\n{nlp_result.stderr}")
                            st.stop()

                        # 3. Database
                        st.text("Step 3: Syncing to Cloud Warehouse...")
                        db_result = subprocess.run([sys.executable, db_path], capture_output=True, text=True)
                        if db_result.returncode != 0:
                            st.error(f"Database Error:\n{db_result.stdout}\n{db_result.stderr}")
                            st.stop()

                        st.success("Analysis Complete!")
                        st.balloons()
                        st.cache_data.clear()
                        st.rerun()
                except Exception as e:
                    st.error(f"Pipeline Error: {e}")
        
        st.divider()
        st.caption(f"Connected to Neon PostgreSQL")
    return page

# 5. Page Renderers (Re-linked to SQL queries)
def render_executive_summary():
    apply_theme()
    st.markdown('<div class="hero-card"><div class="hero-title">Executive Summary</div><p>Live Leadership Dashboard</p></div>', unsafe_allow_html=True)
    
    # Query logic updated for PostgreSQL (lowercase table names)
    data = run_query("SELECT COUNT(*) as count FROM fact_reviews")
    st.metric("Total Reviews Analyzed", data['count'][0])
    
    # Add your Bar Charts and Insight cards here using run_query()

# Main App Loop
def main():
    apply_theme()
    selected_page = render_sidebar()
    
    try:
        if selected_page == "Executive Summary":
            render_executive_summary()
        # Add Sentiment and Pricing elifs here...
    except Exception as e:
        st.error(f"Dashboard Error: {e}")
        st.info("Try running the pipeline in the sidebar to populate the database.")

if __name__ == "__main__":
    main()
