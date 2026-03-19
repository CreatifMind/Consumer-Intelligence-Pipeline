from __future__ import annotations

import random
import sys
import time
from pathlib import Path
import pandas as pd
import requests
from bs4 import BeautifulSoup


DEFAULT_URL = "https://www.amazon.com/nuphy-Wireless-Mechanical-Keyboard-Swappable/dp/B0DZMLL649"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Connection": "keep-alive",
}

def fetch_page_html(urls: list[str]) -> str:
    """
    Fetch one or more pages using browser-like headers.

    The sleep call is kept between requests so the scraper already behaves more like a
    human browser if we expand it to paginate through additional review pages later.
    """
    html_pages: list[str] = []

    for index, url in enumerate(urls, start=1):
        print(f"[INFO] Requesting page {index}: {url}")
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        html_pages.append(response.text)

        if index < len(urls):
            wait_seconds = random.uniform(2, 5)
            print(f"[INFO] Waiting {wait_seconds:.2f} seconds before the next request.")
            time.sleep(wait_seconds)

    return "\n".join(html_pages)


def parse_product_reviews(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")

    title_element = soup.select_one("#productTitle") or soup.find("h1")
    price_element = soup.select_one(".a-price-whole") or soup.select_one(".price")
    review_nodes = soup.select(".review-text")

    product_name = title_element.get_text(" ", strip=True) if title_element else ""
    price = price_element.get_text(" ", strip=True) if price_element else ""

    records = []
    for review_node in review_nodes:
        review_text = review_node.get_text(" ", strip=True)
        if not review_text:
            continue

        review_container = review_node.parent if review_node.parent is not None else review_node
        rating_element = review_container.select_one(".review-rating")
        star_rating = rating_element.get_text(" ", strip=True) if rating_element else ""

        records.append(
            {
                "product_name": product_name,
                "price": price,
                "star_rating": star_rating,
                "review_text": review_text,
            }
        )

    if not records and (product_name or price):
        records.append(
            {
                "product_name": product_name,
                "price": price,
                "star_rating": "",
                "review_text": "",
            }
        )

    if not any([product_name, price, records]):
        raise ValueError("No title, price, or review text could be extracted from the supplied page.")

    return pd.DataFrame(
        records,
        columns=["product_name", "price", "star_rating", "review_text"],
    )


def clean_price_column(df: pd.DataFrame) -> pd.DataFrame:
    cleaned_df = df.copy()
    cleaned_df["price"] = (
        cleaned_df["price"]
        .astype(str)
        .str.replace(r"[^0-9.]", "", regex=True)
        .replace("", pd.NA)
        .astype(float)
    )
    return cleaned_df


def export_raw_data(df: pd.DataFrame) -> Path:
    project_root = Path(__file__).resolve().parents[1]
    output_dir = project_root / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "raw_scraped_data.csv"
    df.to_csv(output_path, index=False)
    return output_path


def main() -> None:
    target_url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL

    print(f"[INFO] Live extraction job started for URL: {target_url}")

    try:
        html = fetch_page_html([target_url])
        extracted_df = parse_product_reviews(html)

        found_pieces = int(bool(extracted_df["product_name"].fillna("").astype(str).str.strip().any()))
        found_pieces += int(bool(extracted_df["price"].fillna("").astype(str).str.strip().any()))
        found_pieces += int(bool(extracted_df["review_text"].fillna("").astype(str).str.strip().any()))

        if found_pieces == 0:
            raise ValueError("The scraper did not find a title, price, or review text.")

        print(f"[INFO] Extraction completed. Records found: {len(extracted_df)}")

        cleaned_df = clean_price_column(extracted_df)
        output_path = export_raw_data(cleaned_df)

        print(f"[INFO] Raw data exported successfully to: {output_path}")
        print("[INFO] Live extraction job finished successfully.")
    except requests.RequestException as exc:
        print(f"[ERROR] HTTP request failed: {exc}")
    except Exception as exc:
        print(f"[ERROR] Live extraction job failed: {exc}")


if __name__ == "__main__":
    main()
