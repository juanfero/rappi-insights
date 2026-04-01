# tests/test_insights.py

from __future__ import annotations

import time

from src.db import Database
from src.insights_engine import InsightsEngine


def test_anomalies_structure():
    db = Database()
    engine = InsightsEngine(db)
    anomalies = engine.detect_anomalies()

    assert isinstance(anomalies, list)
    assert len(anomalies) > 0

    for a in anomalies:
        assert "zone" in a
        assert "metric" in a
        assert "wow_change" in a
        assert abs(a["wow_change"]) > 10
        assert a["direction"] in ("improvement", "deterioration")
        assert a["severity"] in ("critical", "high", "medium")

    engine.close()


def test_anomalies_limit():
    db = Database()
    engine = InsightsEngine(db)
    anomalies = engine.detect_anomalies()
    assert len(anomalies) <= 30
    engine.close()


def test_trends_consecutive_drops():
    db = Database()
    engine = InsightsEngine(db)
    trends = engine.detect_concerning_trends()

    for t in trends:
        assert t["consecutive_drops"] >= 3

        conn = db.get_readonly_connection()
        row = conn.execute(
            """
            SELECT w0, w1, w2, w3
            FROM metrics
            WHERE zone = ?
              AND country = ?
              AND city = ?
              AND metric = ?
            """,
            (t["zone"], t["country"], t["city"], t["metric"]),
        ).fetchone()
        conn.close()

        assert row is not None
        assert row["w0"] < row["w1"] < row["w2"] < row["w3"]


def test_benchmark_gaps_below_mean():
    db = Database()
    engine = InsightsEngine(db)
    gaps = engine.detect_benchmark_gaps()

    for g in gaps:
        assert g["zone_value"] < g["group_mean"]
        assert g["gap_pct"] < 0

    engine.close()


def test_correlations_significant():
    db = Database()
    engine = InsightsEngine(db)
    correlations = engine.detect_correlations()

    for c in correlations:
        assert abs(c["correlation"]) > 0.5
        assert c["p_value"] < 0.05
        assert c["n_zones"] >= 30

    engine.close()


def test_opportunities_logic():
    db = Database()
    engine = InsightsEngine(db)
    opportunities = engine.detect_opportunities()

    for o in opportunities:
        assert o["zone_priority"] in ("High Priority", "Prioritized")
        assert o["current_value"] < o["country_avg"]
        assert o["trend"] > 0

    engine.close()


def test_run_all_categories():
    db = Database()
    engine = InsightsEngine(db)
    results = engine.run_all()

    assert set(results.keys()) == {
        "anomalies",
        "trends",
        "benchmarks",
        "correlations",
        "opportunities",
    }

    for _, value in results.items():
        assert isinstance(value, list)

    engine.close()


def test_run_all_performance():
    db = Database()
    engine = InsightsEngine(db)

    start = time.time()
    engine.run_all()
    elapsed = time.time() - start

    assert elapsed < 30, f"run_all() tardó {elapsed:.1f}s (máximo 30s)"
    engine.close()