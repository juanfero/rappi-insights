from __future__ import annotations

import io

from src.db import Database
from src.agent import run_agent
from src.insights_engine import InsightsEngine, generate_report


def test_app_imports():
    from src.config import METRICS_DICTIONARY, DEFAULT_MODEL
    from src.db import Database
    from src.tools import execute_tool
    from src.agent import run_agent
    from src.insights_engine import InsightsEngine

    assert METRICS_DICTIONARY is not None
    assert DEFAULT_MODEL is not None
    assert Database is not None
    assert execute_tool is not None
    assert run_agent is not None
    assert InsightsEngine is not None


def test_e2e_chat_simple():
    db = Database()
    db.ingest_csvs("data/RAW_INPUT_METRICS.csv", "data/RAW_ORDERS.csv")
    response = run_agent("¿Cuántos países hay en la base de datos?", [], db)
    assert "9" in response


def test_e2e_chat_multivariable():
    db = Database()
    db.ingest_csvs("data/RAW_INPUT_METRICS.csv", "data/RAW_ORDERS.csv")
    response = run_agent(
        "¿Qué zonas de Colombia tienen alto Perfect Orders pero bajo Lead Penetration?",
        [],
        db,
    )
    assert any(word in response for word in ["CO", "Colombia", "Bogota", "Chapinero", "Usaquén"])


def test_e2e_insights_report():
    db = Database()
    db.ingest_csvs("data/RAW_INPUT_METRICS.csv", "data/RAW_ORDERS.csv")
    engine = InsightsEngine(db)
    insights = engine.run_all()
    report = generate_report(insights, db)
    assert "Resumen Ejecutivo" in report or "resumen ejecutivo" in report.lower()
    assert "Anomalía" in report or "anomalía" in report.lower()
    assert "Recomendacion" in report or "recomendación" in report.lower()
    assert len(report) > 500
    engine.close()


def test_e2e_memory():
    db = Database()
    db.ingest_csvs("data/RAW_INPUT_METRICS.csv", "data/RAW_ORDERS.csv")

    r1 = run_agent("¿Top 3 zonas por Perfect Orders en Colombia?", [], db)
    history = [
        {"role": "user", "content": "¿Top 3 zonas por Perfect Orders en Colombia?"},
        {"role": "assistant", "content": r1},
    ]

    r2 = run_agent("¿Y en México?", history, db)
    assert any(word in r2 for word in ["MX", "México", "Mexico"])


def test_e2e_csv_upload():
    db = Database()
    db.ingest_csvs("data/RAW_INPUT_METRICS.csv", "data/RAW_ORDERS.csv")
    meta = db.get_metadata()
    assert meta["total_zones"] == 964


def test_e2e_csv_upload_invalid():
    db = Database()
    bad_csv = io.StringIO("col_a,col_b\n1,2\n")
    import pytest
    with pytest.raises(ValueError):
        db.ingest_from_uploads(bad_csv, bad_csv)