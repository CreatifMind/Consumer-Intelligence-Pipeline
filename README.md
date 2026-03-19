# 📈 Consumer Intelligence Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-Cloud-FF4B4B.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-336791.svg)
![Scikit-Learn](https://img.shields.io/badge/Scikit--Learn-Machine%20Learning-F7931E.svg)

**[Live Demo Dashboard Here]** (https://consumer-intelligence-pipeline.streamlit.app/)

## 📖 Project Overview
The Consumer Intelligence Pipeline is an end-to-end, cloud-hosted data engineering and analytics platform. It automates the extraction, natural language processing (NLP), and visualization of retail product data, giving business leaders a real-time, quantitative view of consumer sentiment and pricing intelligence without requiring manual data entry.

## 🏗️ Technical Architecture

This project is built using a modern, 4-step data pipeline:

1. **Dynamic Data Extraction (Python / BeautifulSoup):** A "Universal Scraper" accepts retail URLs, bypasses basic anti-bot measures, and extracts product titles, live pricing, and raw review text. It features intelligent fallback routing to handle sparse data scenarios.
2. **Machine Learning & NLP (Scikit-Learn):** Raw text is normalized and passed through a Latent Dirichlet Allocation (LDA) model. The algorithm assigns each review to a core business topic (Quality, Pricing, or Delivery) and calculates a model confidence score.
3. **Cloud Data Warehousing (PostgreSQL / SQLAlchemy):** Processed data is transformed and loaded into a cloud-hosted PostgreSQL database structured as a Star Schema (Fact and Dimension tables), utilizing `TRUNCATE` logic for real-time dashboard syncing.
4. **Real-Time Visualization (Streamlit / Plotly):** The frontend acts as a live leadership dashboard, executing direct SQL queries against the cloud warehouse to render interactive charts for review volume, topic distribution, and pricing snapshots.

---

## 🧪 Suggested Test URLs for Reviewers

To see the pipeline in action, copy and paste any of the links below into the live dashboard. 

**1. The "Sandbox" Sites (100% Success Rate)**
*These sites are specifically built for scraping. They utilize standard HTML structures and guarantee a perfect end-to-end pipeline run to demonstrate the fallback NLP logic.*
* **Product (Book):** `http://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html`

**2. Independent E-Commerce Brands (Stable Real-World Test)**
*Millions of modern brands run on platforms like Shopify that use Server-Side Rendering (SSR). These are the perfect real-world targets for this pipeline's extraction engine.*
* **NuPhy (Tech Accessories):** `https://nuphy.com/collections/keyboards/products/halo75-v2-qmk-via-wireless-custom-mechanical-keyboard`
* **Allbirds (Apparel):** `https://www.allbirds.com/products/mens-tree-runners-everyday-sneakers-medium-grey`

**3. The "High-Stakes" Test (Enterprise Marketplaces)**
*Major marketplaces employ aggressive anti-bot firewalls. This pipeline includes headers to bypass basic checks, but cloud-hosted IPs may occasionally be rate-limited.*
* **Amazon (Visual Timer):** `https://www.amazon.com/Yunbaoit-Upgraded-Protective-Countdown-Indicator/dp/B0B9S1K69P/`

> **⚠️ Note on E-Commerce Security:** This pipeline is optimized for Server-Side Rendered (SSR) HTML. Modern Single Page Applications (SPAs) heavily reliant on JavaScript rendering (e.g., Shopee, Lazada) actively block automated cloud extraction and are outside the scope of this demonstration.

---

## 💻 Local Setup & Installation

To run this pipeline locally on your machine:

**1. Clone the repository**
```bash
git clone [https://github.com/CreatifMind/Consumer-Intelligence-Pipeline.git](https://github.com/CreatifMind/Consumer-Intelligence-Pipeline.git)
cd Consumer-Intelligence-Pipeline
```

**2. Create a virtual environment and install dependencies**
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
pip install -r requirements.txt
```

**3. Configure your Database Secrets**
Create a `.streamlit/secrets.toml` file in the root directory and add your PostgreSQL connection string:
```toml
DATABASE_URL = "postgresql://user:password@your-host.com/dbname"
```

**4. Run the application**
```bash
streamlit run app.py
```
