from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

import pandas as pd
from bs4 import BeautifulSoup


MOCK_HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>{category_title} Search Results</title>
  </head>
  <body>
    <main class="search-results" data-category="{category_slug}">
      <article class="product-card" data-sku="EB-1001">
        <h2 class="product-name">Auralink Air One</h2>
        <span class="price">$79.99</span>
        <span class="star-rating">4.6 out of 5 stars</span>
        <p class="review-text">Balanced sound with reliable battery life for daily commuting.</p>
      </article>
      <article class="product-card featured" data-sku="EB-1002">
        <h2 class="product-name">PulseBuds Pro S</h2>
        <span class="price">$119.00</span>
        <span class="star-rating">4.8 out of 5 stars</span>
        <p class="review-text">Excellent noise cancellation and premium fit during workouts.</p>
      </article>
      <article class="result-tile" data-sku="EB-1003">
        <a class="title">NovaPods Lite</a>
        <div class="pricing"><span class="current-price">$59.50</span></div>
        <div class="rating-row"><span class="stars">4.2 out of 5 stars</span></div>
        <div class="review-snippet">Compact case and stable Bluetooth pairing for laptops and phones.</div>
      </article>
      <article class="product-card" data-sku="EB-1004">
        <h2 class="product-name">EchoBeat Max</h2>
        <span class="price">$149.99</span>
        <span class="star-rating">4.7 out of 5 stars</span>
        <p class="review-text">Strong bass response and intuitive touch controls.</p>
      </article>
      <article class="product-tile" data-sku="EB-1005">
        <span data-qa="product-name">WaveSync Mini</span>
        <span data-qa="product-price">$45.00</span>
        <span data-qa="product-rating">4.1 out of 5 stars</span>
        <p data-qa="product-review">Affordable choice with surprisingly clear microphone quality.</p>
      </article>
      <article class="product-card" data-sku="EB-1006">
        <h2 class="product-name">ZenAudio Fit</h2>
        <span class="price">$89.95</span>
        <span class="star-rating">4.4 out of 5 stars</span>
        <p class="review-text">Secure fit for runners and dependable sweat resistance.</p>
      </article>
      <article class="product-card" data-sku="EB-1007">
        <h2 class="product-name">CloudTune Ultra</h2>
        <span class="price">$139.49</span>
        <span class="star-rating">4.5 out of 5 stars</span>
        <p class="review-text">Crisp highs, rich mids, and fast charging in under an hour.</p>
      </article>
      <article class="result-tile" data-sku="EB-1008">
        <a class="title">VibeCore Studio</a>
        <div class="pricing"><span class="current-price">$99.99</span></div>
        <div class="rating-row"><span class="stars">4.3 out of 5 stars</span></div>
        <div class="review-snippet">Comfortable for long listening sessions and video calls.</div>
      </article>
      <article class="product-card premium" data-sku="EB-1009">
        <h2 class="product-name">SonicNest Elite</h2>
        <span class="price">$159.00</span>
        <span class="star-rating">4.9 out of 5 stars</span>
        <p class="review-text">Top-tier soundstage with excellent transparency mode.</p>
      </article>
      <article class="product-tile" data-sku="EB-1010">
        <span data-qa="product-name">LoopPods Neo</span>
        <span data-qa="product-price">$69.99</span>
        <span data-qa="product-rating">4.0 out of 5 stars</span>
        <p data-qa="product-review">Good all-round option for students and remote workers.</p>
      </article>
      <article class="product-card" data-sku="EB-1011">
        <h2 class="product-name">AeroPulse Go</h2>
        <span class="price">$54.75</span>
        <span class="star-rating">3.9 out of 5 stars</span>
        <p class="review-text">Entry-level price point with a lightweight portable design.</p>
      </article>
      <article class="product-card" data-sku="EB-1012">
        <h2 class="product-name">BassBloom Flex</h2>
        <span class="price">$84.20</span>
        <span class="star-rating">4.2 out of 5 stars</span>
        <p class="review-text">Warm sound signature and comfortable ear tips for long wear.</p>
      </article>
      <article class="result-tile" data-sku="EB-1013">
        <a class="title">QuietSphere Plus</a>
        <div class="pricing"><span class="current-price">$129.90</span></div>
        <div class="rating-row"><span class="stars">4.6 out of 5 stars</span></div>
        <div class="review-snippet">Impressive passive isolation and dependable app controls.</div>
      </article>
      <article class="product-card" data-sku="EB-1014">
        <h2 class="product-name">DriftAudio Core</h2>
        <span class="price">$72.40</span>
        <span class="star-rating">4.1 out of 5 stars</span>
        <p class="review-text">Clear vocals and strong value for budget-conscious shoppers.</p>
      </article>
      <article class="product-tile" data-sku="EB-1015">
        <span data-qa="product-name">OrbitSound Prime</span>
        <span data-qa="product-price">$109.30</span>
        <span data-qa="product-rating">4.5 out of 5 stars</span>
        <p data-qa="product-review">Reliable connectivity across devices and polished overall audio quality.</p>
      </article>
    </main>
  </body>
</html>
"""


def first_non_empty_text(node, selectors: Iterable[str]) -> str:
    for selector in selectors:
        element = node.select_one(selector)
        if element:
            text = element.get_text(" ", strip=True)
            if text:
                return text
    raise ValueError(f"Required field missing for selectors: {list(selectors)}")


def parse_search_results(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    product_nodes = soup.select(
        "main.search-results article.product-card, "
        "main.search-results article.product-tile, "
        "main.search-results article.result-tile"
    )

    if not product_nodes:
        raise ValueError("No product listings were found in the supplied HTML.")

    records = []
    for index, node in enumerate(product_nodes, start=1):
        try:
            record = {
                "product_name": first_non_empty_text(
                    node,
                    [
                        "[data-qa='product-name']",
                        ".product-name",
                        ".title",
                    ],
                ),
                "price": first_non_empty_text(
                    node,
                    [
                        "[data-qa='product-price']",
                        ".price",
                        ".current-price",
                    ],
                ),
                "star_rating": first_non_empty_text(
                    node,
                    [
                        "[data-qa='product-rating']",
                        ".star-rating",
                        ".stars",
                    ],
                ),
                "review_text": first_non_empty_text(
                    node,
                    [
                        "[data-qa='product-review']",
                        ".review-text",
                        ".review-snippet",
                    ],
                ),
            }
            records.append(record)
        except Exception as exc:
            print(f"[WARN] Skipping listing {index}: {exc}")

    if not records:
        raise ValueError("Product nodes were found, but none could be parsed successfully.")

    return pd.DataFrame(records)


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
    product_category = os.environ.get("PRODUCT_CATEGORY", "wireless earbuds").strip() or "wireless earbuds"
    category_slug = product_category.lower().replace(" ", "-")
    html = MOCK_HTML_TEMPLATE.format(category_title=product_category.title(), category_slug=category_slug)

    print(f"[INFO] Daily extraction job started for category: {product_category}")

    try:
        extracted_df = parse_search_results(html)
        print(f"[INFO] Extraction completed. Records found: {len(extracted_df)}")

        cleaned_df = clean_price_column(extracted_df)
        output_path = export_raw_data(cleaned_df)

        print(f"[INFO] Raw data exported successfully to: {output_path}")
        print("[INFO] Daily extraction job finished successfully.")
    except Exception as exc:
        print(f"[ERROR] Daily extraction job failed: {exc}")


if __name__ == "__main__":
    main()
