from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer, ENGLISH_STOP_WORDS


STOP_WORDS = set(ENGLISH_STOP_WORDS).union(
    {
        "earbud",
        "earbuds",
        "wireless",
        "product",
        "overall",
    }
)

IRREGULAR_LEMMAS = {
    "mice": "mouse",
    "men": "man",
    "women": "woman",
    "teeth": "tooth",
    "feet": "foot",
    "better": "good",
    "best": "good",
    "worse": "bad",
    "highs": "high",
    "mids": "mid",
}

TOPIC_KEYWORDS = {
    "Pricing & Value": {"affordable", "budget", "entry", "premium", "price", "value"},
    "Audio Quality": {
        "audio",
        "bass",
        "clear",
        "control",
        "crisp",
        "high",
        "level",
        "mic",
        "mid",
        "quality",
        "rich",
        "sound",
        "soundstage",
        "strong",
        "vocal",
    },
    "Comfort & Fit": {"comfortable", "comfort", "ear", "fit", "runner", "session", "tip", "wear", "workout"},
    "Battery & Performance": {"battery", "charg", "fast", "hour", "life", "performance"},
    "Connectivity & Usability": {
        "app",
        "bluetooth",
        "call",
        "case",
        "compact",
        "connectivity",
        "control",
        "device",
        "laptop",
        "pairing",
        "stable",
    },
    "Service & Delivery": {"customer", "delivery", "issue", "resolution", "shipping", "support"},
}


def simple_lemmatize(token: str) -> str:
    if token in IRREGULAR_LEMMAS:
        return IRREGULAR_LEMMAS[token]

    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"

    if token.endswith("ing") and len(token) > 5:
        lemma = token[:-3]
        if len(lemma) > 2 and lemma[-1] == lemma[-2]:
            lemma = lemma[:-1]
        return lemma

    if token.endswith("ed") and len(token) > 4:
        lemma = token[:-2]
        if len(lemma) > 2 and lemma[-1] == lemma[-2]:
            lemma = lemma[:-1]
        return lemma

    if token.endswith("es") and len(token) > 4 and not token.endswith("ses"):
        return token[:-2]

    if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
        return token[:-1]

    return token


def preprocess_text(text: str) -> str:
    normalized_text = re.sub(r"[^a-zA-Z\s]", " ", str(text).lower())
    tokens = normalized_text.split()

    cleaned_tokens = []
    for token in tokens:
        if token in STOP_WORDS or len(token) < 3:
            continue

        lemma = simple_lemmatize(token)
        if lemma and lemma not in STOP_WORDS and len(lemma) >= 3:
            cleaned_tokens.append(lemma)

    return " ".join(cleaned_tokens)


def infer_topic_label(topic_terms: list[str], topic_index: int) -> str:
    best_label = f"Topic {topic_index + 1}"
    best_score = 0

    for label, keywords in TOPIC_KEYWORDS.items():
        score = len(set(topic_terms) & keywords)
        if score > best_score:
            best_label = label
            best_score = score

    if best_score == 0:
        best_label = f"Topic {topic_index + 1}: {topic_terms[0].title()}"

    return best_label


def ensure_unique_labels(topic_details: list[dict]) -> list[dict]:
    label_counts: dict[str, int] = {}

    for detail in topic_details:
        label = detail["label"]
        label_counts[label] = label_counts.get(label, 0) + 1
        if label_counts[label] > 1:
            detail["label"] = f"{label} ({detail['top_terms'][0]})"

    return topic_details


def summarize_topics(
    lda_model: LatentDirichletAllocation,
    vectorizer: CountVectorizer,
    top_n_terms: int = 5,
) -> list[dict]:
    feature_names = vectorizer.get_feature_names_out()
    topic_details = []

    for topic_index, weights in enumerate(lda_model.components_):
        top_indices = weights.argsort()[-top_n_terms:][::-1]
        top_terms = [feature_names[index] for index in top_indices]
        topic_details.append(
            {
                "topic_id": topic_index,
                "label": infer_topic_label(top_terms, topic_index),
                "top_terms": top_terms,
            }
        )

    return ensure_unique_labels(topic_details)


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    raw_path = project_root / "data" / "raw" / "raw_scraped_data.csv"
    processed_path = project_root / "data" / "processed" / "analyzed_reviews.csv"

    print("[INFO] NLP processing job started.")

    try:
        print(f"[INFO] Reading raw review data from: {raw_path}")
        reviews_df = pd.read_csv(raw_path)

        if "review_text" not in reviews_df.columns:
            raise ValueError("The required 'review_text' column is missing from the input dataset.")

        print(f"[INFO] Loaded {len(reviews_df)} review records.")
        print("[INFO] Starting text preprocessing: stop-word removal, punctuation cleanup, and lemmatization.")

        processed_reviews = reviews_df["review_text"].fillna("").map(preprocess_text)
        valid_reviews = processed_reviews.str.strip().ne("")

        if not valid_reviews.any():
            raise ValueError("No usable review text remained after preprocessing.")

        print(f"[INFO] Preprocessing complete. Valid reviews available for modeling: {int(valid_reviews.sum())}")
        print("[INFO] Building document-term matrix for LDA topic modeling.")

        vectorizer = CountVectorizer(
            max_features=100,
            ngram_range=(1, 1),
            min_df=1,
            max_df=1.0,
        )
        doc_term_matrix = vectorizer.fit_transform(processed_reviews[valid_reviews])

        print(f"[INFO] Vectorization complete. Vocabulary size: {len(vectorizer.get_feature_names_out())}")
        print("[INFO] Training Latent Dirichlet Allocation model with 3 topics.")

        lda_model = LatentDirichletAllocation(
            n_components=3,
            random_state=42,
            max_iter=25,
            learning_method="batch",
        )
        topic_distribution = lda_model.fit_transform(doc_term_matrix)

        topic_details = summarize_topics(lda_model, vectorizer)
        topic_label_map = {detail["topic_id"]: detail["label"] for detail in topic_details}

        print("[INFO] Model training complete. Topic summaries:")
        for detail in topic_details:
            print(f"       - {detail['label']}: {', '.join(detail['top_terms'])}")

        dominant_topic_indices = topic_distribution.argmax(axis=1)
        topic_confidences = topic_distribution.max(axis=1)

        reviews_df["Dominant_Topic"] = "Unassigned"
        reviews_df["Topic_Confidence"] = 0.0

        reviews_df.loc[valid_reviews, "Dominant_Topic"] = [
            topic_label_map[index] for index in dominant_topic_indices
        ]
        reviews_df.loc[valid_reviews, "Topic_Confidence"] = topic_confidences.round(4)

        processed_path.parent.mkdir(parents=True, exist_ok=True)
        reviews_df.to_csv(processed_path, index=False)

        print(f"[INFO] Categorized review data exported to: {processed_path}")
        print("[INFO] NLP processing job finished successfully.")
    except FileNotFoundError:
        print(f"[ERROR] Raw input file not found: {raw_path}")
    except Exception as exc:
        print(f"[ERROR] NLP processing job failed: {exc}")


if __name__ == "__main__":
    main()
