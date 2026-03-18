from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


REQUIRED_COLUMNS = {
    "product_name",
    "price",
    "star_rating",
    "review_text",
    "Dominant_Topic",
    "Topic_Confidence",
}


def parse_star_rating(value: object) -> float:
    match = re.search(r"(\d+(?:\.\d+)?)", str(value))
    if not match:
        raise ValueError(f"Unable to parse numeric star rating from value: {value}")
    return float(match.group(1))


def create_star_schema(connection) -> None:
    connection.execute(text("PRAGMA foreign_keys = ON"))

    connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS Dim_Product (
                Product_Key INTEGER PRIMARY KEY AUTOINCREMENT,
                Product_Name TEXT NOT NULL UNIQUE,
                Current_Price REAL NOT NULL
            )
            """
        )
    )

    connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS Dim_Topic (
                Topic_Key INTEGER PRIMARY KEY AUTOINCREMENT,
                Topic_Name TEXT NOT NULL UNIQUE
            )
            """
        )
    )

    connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS Fact_Reviews (
                Review_Key INTEGER PRIMARY KEY AUTOINCREMENT,
                Product_Key INTEGER NOT NULL,
                Topic_Key INTEGER NOT NULL,
                Review_Text TEXT NOT NULL,
                Star_Rating REAL NOT NULL,
                Topic_Confidence REAL NOT NULL,
                Load_Timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (Product_Key) REFERENCES Dim_Product(Product_Key),
                FOREIGN KEY (Topic_Key) REFERENCES Dim_Topic(Topic_Key)
            )
            """
        )
    )

    connection.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_fact_reviews_product_key
            ON Fact_Reviews (Product_Key)
            """
        )
    )

    connection.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_fact_reviews_topic_key
            ON Fact_Reviews (Topic_Key)
            """
        )
    )


def load_processed_reviews(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    missing_columns = REQUIRED_COLUMNS.difference(df.columns)
    if missing_columns:
        raise ValueError(f"Input file is missing required columns: {sorted(missing_columns)}")

    cleaned_df = df.copy()
    cleaned_df["product_name"] = cleaned_df["product_name"].astype(str).str.strip()
    cleaned_df["Dominant_Topic"] = cleaned_df["Dominant_Topic"].astype(str).str.strip()
    cleaned_df["review_text"] = cleaned_df["review_text"].fillna("").astype(str).str.strip()
    cleaned_df["price"] = pd.to_numeric(cleaned_df["price"], errors="coerce")
    cleaned_df["Topic_Confidence"] = pd.to_numeric(cleaned_df["Topic_Confidence"], errors="coerce")
    cleaned_df["Star_Rating_Value"] = cleaned_df["star_rating"].map(parse_star_rating)

    cleaned_df = cleaned_df.dropna(subset=["product_name", "Dominant_Topic", "price", "Topic_Confidence"])
    cleaned_df = cleaned_df[cleaned_df["review_text"] != ""].reset_index(drop=True)

    if cleaned_df.empty:
        raise ValueError("No valid review rows remained after data cleaning.")

    return cleaned_df


def refresh_dimensions(connection, reviews_df: pd.DataFrame) -> tuple[dict[str, int], dict[str, int]]:
    product_dimension = (
        reviews_df[["product_name", "price"]]
        .drop_duplicates(subset=["product_name"])
        .sort_values("product_name")
    )

    topic_dimension = (
        reviews_df[["Dominant_Topic"]]
        .drop_duplicates()
        .sort_values("Dominant_Topic")
        .rename(columns={"Dominant_Topic": "topic_name"})
    )

    # Rebuild the dimensions from the current processed file so the warehouse does not
    # retain stale products or topic labels from previous modeling runs.
    connection.execute(text("DELETE FROM Dim_Product"))
    connection.execute(text("DELETE FROM Dim_Topic"))

    connection.execute(
        text(
            """
            INSERT INTO Dim_Product (Product_Name, Current_Price)
            VALUES (:product_name, :current_price)
            """
        ),
        [
            {
                "product_name": row.product_name,
                "current_price": float(row.price),
            }
            for row in product_dimension.itertuples(index=False)
        ],
    )

    connection.execute(
        text(
            """
            INSERT OR IGNORE INTO Dim_Topic (Topic_Name)
            VALUES (:topic_name)
            """
        ),
        [{"topic_name": row.topic_name} for row in topic_dimension.itertuples(index=False)],
    )

    product_lookup = {
        row.Product_Name: row.Product_Key
        for row in connection.execute(
            text("SELECT Product_Key, Product_Name FROM Dim_Product")
        ).mappings()
    }
    topic_lookup = {
        row.Topic_Name: row.Topic_Key
        for row in connection.execute(
            text("SELECT Topic_Key, Topic_Name FROM Dim_Topic")
        ).mappings()
    }

    return product_lookup, topic_lookup


def build_fact_rows(
    reviews_df: pd.DataFrame,
    product_lookup: dict[str, int],
    topic_lookup: dict[str, int],
) -> list[dict]:
    fact_rows = []

    for row in reviews_df.itertuples(index=False):
        product_key = product_lookup.get(row.product_name)
        topic_key = topic_lookup.get(row.Dominant_Topic)

        if product_key is None:
            raise ValueError(f"Missing Product_Key for product: {row.product_name}")
        if topic_key is None:
            raise ValueError(f"Missing Topic_Key for topic: {row.Dominant_Topic}")

        fact_rows.append(
            {
                "product_key": product_key,
                "topic_key": topic_key,
                "review_text": row.review_text,
                "star_rating": float(row.Star_Rating_Value),
                "topic_confidence": float(row.Topic_Confidence),
            }
        )

    return fact_rows


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    db_path = project_root / "retail_intelligence.db"
    input_path = project_root / "data" / "processed" / "analyzed_reviews.csv"

    print("[INFO] Database load job started.")
    print(f"[INFO] Source file: {input_path}")
    print(f"[INFO] Target database: {db_path}")

    try:
        reviews_df = load_processed_reviews(input_path)
        print(f"[INFO] Loaded {len(reviews_df)} processed review rows from CSV.")

        engine = create_engine(f"sqlite:///{db_path}")

        with engine.begin() as connection:
            print("[INFO] Creating star schema tables if they do not already exist.")
            create_star_schema(connection)

            print("[INFO] Refreshing dimension and fact tables from the latest processed snapshot.")
            connection.execute(text("DELETE FROM Fact_Reviews"))
            product_lookup, topic_lookup = refresh_dimensions(connection, reviews_df)

            fact_rows = build_fact_rows(reviews_df, product_lookup, topic_lookup)

            connection.execute(
                text(
                    """
                    INSERT INTO Fact_Reviews (
                        Product_Key,
                        Topic_Key,
                        Review_Text,
                        Star_Rating,
                        Topic_Confidence
                    )
                    VALUES (
                        :product_key,
                        :topic_key,
                        :review_text,
                        :star_rating,
                        :topic_confidence
                    )
                    """
                ),
                fact_rows,
            )

        print(f"[INFO] Database load complete. {len(fact_rows)} rows were inserted into Fact_Reviews.")
    except FileNotFoundError:
        print(f"[ERROR] Processed input file not found: {input_path}")
    except (ValueError, SQLAlchemyError) as exc:
        print(f"[ERROR] Database load job failed: {exc}")
    except Exception as exc:
        print(f"[ERROR] Unexpected database load failure: {exc}")


if __name__ == "__main__":
    main()
