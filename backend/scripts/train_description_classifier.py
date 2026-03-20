#!/usr/bin/env python3
"""
Train the TF-IDF + Logistic Regression description block classifier.

Usage (inside the api container):
    python scripts/train_description_classifier.py [--min-quality 60] [--verbose]

Training data is pulled directly from the PostgreSQL database:
  Positive examples: jobs where quality_score >= MIN_QUALITY and description length >= 300
  Negative examples: jobs where quality_score < 30 OR description is very short/None
                     (these are often wrong extractions — nav text, boilerplate, etc.)

The trained model is saved to /storage/models/description_classifier.joblib.
Once present, Layer 4.5 in description_extractor.py will load and use it automatically.

Expected outcome:
  ~5,000+ training examples → F1 > 0.85 on held-out test set
  Inference: < 1ms per page (vs 3-8 minutes for LLM layers)
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# Add project root to path so we can import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Train description block classifier")
    p.add_argument("--min-quality", type=int, default=60,
                   help="Minimum quality_score for positive examples (default: 60)")
    p.add_argument("--min-pos-len", type=int, default=300,
                   help="Minimum description length for positive examples (default: 300)")
    p.add_argument("--max-neg-len", type=int, default=200,
                   help="Max description length for negative 'short extraction' examples (default: 200)")
    p.add_argument("--test-size", type=float, default=0.15,
                   help="Fraction of data held out for evaluation (default: 0.15)")
    p.add_argument("--verbose", action="store_true", help="Verbose output")
    return p.parse_args()


def load_training_data(min_quality: int, min_pos_len: int, max_neg_len: int):
    """Pull positive and negative examples from the database.

    Returns (texts, labels) lists — texts are raw description strings.
    """
    import psycopg2

    dsn = (
        f"host={os.environ.get('POSTGRES_HOST', 'postgres')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'jobharvest')} "
        f"user={os.environ.get('POSTGRES_USER', 'jobharvest')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'jobharvest')}"
    )

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    # Positive examples: high-quality descriptions we're confident are correct
    cur.execute("""
        SELECT description
        FROM jobs
        WHERE is_active = TRUE
          AND description IS NOT NULL
          AND quality_score >= %s
          AND length(description) >= %s
        ORDER BY quality_score DESC
        LIMIT 10000
    """, (min_quality, min_pos_len))
    positives = [row[0] for row in cur.fetchall()]
    logger.info(f"Positive examples: {len(positives)}")

    # Negative examples — three sources:
    # 1. Jobs with very short descriptions (likely wrong extraction)
    cur.execute("""
        SELECT description
        FROM jobs
        WHERE is_active = TRUE
          AND description IS NOT NULL
          AND length(description) < %s
          AND (quality_score IS NULL OR quality_score < 30)
        LIMIT 3000
    """, (max_neg_len,))
    negatives_short = [row[0] for row in cur.fetchall() if row[0].strip()]
    logger.info(f"Negative examples (short extractions): {len(negatives_short)}")

    # 2. Known navigation/boilerplate patterns from DB (low quality, non-null description)
    cur.execute("""
        SELECT description
        FROM jobs
        WHERE is_active = TRUE
          AND description IS NOT NULL
          AND quality_score < 20
          AND length(description) BETWEEN 50 AND 500
        LIMIT 2000
    """)
    negatives_low_quality = [row[0] for row in cur.fetchall() if row[0].strip()]
    logger.info(f"Negative examples (low quality): {len(negatives_low_quality)}")

    cur.close()
    conn.close()

    # Also add synthetic negatives: fragments commonly extracted from nav/lists
    synthetic_negatives = _synthetic_negative_examples()
    logger.info(f"Synthetic negatives: {len(synthetic_negatives)}")

    negatives = negatives_short + negatives_low_quality + synthetic_negatives

    texts = positives + negatives
    labels = [1] * len(positives) + [0] * len(negatives)

    logger.info(f"Total: {len(texts)} examples ({len(positives)} pos, {len(negatives)} neg)")
    return texts, labels


def _synthetic_negative_examples() -> list[str]:
    """Hand-crafted negative examples covering common false-extraction patterns."""
    return [
        # Navigation menus
        "Home\nAbout Us\nCareers\nContact\nLocations\nPrivacy Policy",
        "Jobs\nInternships\nGraduate Programs\nEarly Careers\nFAQ\nApply",
        "Australia\nNew Zealand\nUnited Kingdom\nUnited States\nCanada\nSingapore",
        "Sort by: Relevance\nDate\nSalary\nLocation\nPage 1 of 23\nNext >",
        # Location/filter pages
        "Showing 47 jobs in Sydney\nFilter by:\nJob Type\nDepartment\nLocation\nDate Posted",
        "Package Handler - Part Time\nPackage Handler - Full Time\nDriver\nWarehouse Associate",
        "France\nGermany\nSpain\nItaly\nNetherlands\nBelgium\nAustria\nSwitzerland",
        # Cookie/legal notices
        "We use cookies to enhance your experience. By continuing to browse this site "
        "you agree to our use of cookies.",
        "JavaScript is required to view this page. Please enable JavaScript in your browser settings.",
        # Generic boilerplate (not role-specific)
        "Equal Opportunity Employer. We celebrate diversity and are committed to creating an "
        "inclusive environment for all employees.",
        "About our company. We are a global leader in our industry with offices in 50 countries.",
        # Pagination/breadcrumb
        "1 2 3 4 5 ... 47 Next\nSearch Results\nSort: Most Relevant",
        # Apply button + form text
        "Apply Now\nFirst Name\nLast Name\nEmail Address\nPhone Number\nResume/CV\nSubmit Application",
        # Error pages
        "404 - Page Not Found\nThe page you are looking for does not exist.",
        "This job has expired or been removed. Please search for other opportunities.",
    ]


def train(texts: list[str], labels: list[int], test_size: float, verbose: bool):
    """Fit the model and return evaluation metrics."""
    import numpy as np
    from scipy.sparse import hstack, csr_matrix
    from sklearn.model_selection import train_test_split, cross_val_score
    from sklearn.metrics import classification_report, f1_score

    from app.ml.description_classifier import (
        build_pipeline, EngineeringFeatures, MODEL_PATH
    )

    logger.info("Splitting train/test...")
    X_train_raw, X_test_raw, y_train, y_test = train_test_split(
        texts, labels, test_size=test_size, random_state=42, stratify=labels
    )

    word_tfidf, char_tfidf, lr = build_pipeline()
    eng = EngineeringFeatures()

    logger.info("Fitting TF-IDF vectorizers...")
    X_train_word = word_tfidf.fit_transform(X_train_raw)
    X_train_char = char_tfidf.fit_transform(X_train_raw)
    X_train_eng = csr_matrix(eng.transform(X_train_raw))
    X_train = hstack([X_train_word, X_train_char, X_train_eng])
    logger.info(f"  Feature matrix: {X_train.shape}")

    logger.info("Training LogisticRegression...")
    lr.fit(X_train, y_train)

    # Evaluate on held-out test set
    X_test_word = word_tfidf.transform(X_test_raw)
    X_test_char = char_tfidf.transform(X_test_raw)
    X_test_eng = csr_matrix(eng.transform(X_test_raw))
    X_test = hstack([X_test_word, X_test_char, X_test_eng])

    y_pred = lr.predict(X_test)
    f1 = f1_score(y_test, y_pred)

    logger.info(f"\n{'='*60}")
    logger.info(f"Test F1: {f1:.4f}")
    if verbose:
        logger.info("\n" + classification_report(y_test, y_pred, target_names=["negative", "positive"]))
    logger.info('='*60)

    if f1 < 0.70:
        logger.warning(
            f"F1={f1:.3f} is below 0.70 — model may be unreliable. "
            "Consider gathering more training data before deploying."
        )

    # Save model
    import joblib
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    state = {"word_tfidf": word_tfidf, "char_tfidf": char_tfidf, "lr": lr}
    joblib.dump(state, MODEL_PATH)
    logger.info(f"\nModel saved to {MODEL_PATH}")

    return f1


def main():
    args = parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("=== JobHarvest Description Classifier Training ===")
    logger.info(f"  min_quality={args.min_quality}, min_pos_len={args.min_pos_len}")
    logger.info(f"  max_neg_len={args.max_neg_len}, test_size={args.test_size}")

    texts, labels = load_training_data(
        args.min_quality, args.min_pos_len, args.max_neg_len
    )

    if sum(labels) < 50:
        logger.error(
            f"Only {sum(labels)} positive examples found — need at least 50 to train. "
            "Run more crawls/extractions first, then retry."
        )
        sys.exit(1)

    if len(labels) - sum(labels) < 20:
        logger.error("Too few negative examples to train a meaningful classifier.")
        sys.exit(1)

    f1 = train(texts, labels, args.test_size, args.verbose)
    logger.info(f"Done. F1={f1:.4f}")


if __name__ == "__main__":
    main()
