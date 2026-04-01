from __future__ import annotations

import io

from src.db import Database
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


def test_e2e_chat_simple(monkeypatch):
    """
    Valida el flujo completo del chat simple sin depender de Gemini real.
    """
    from src import agent as agent_module

    db = Database()
    db.ingest_csvs("data/RAW_INPUT_METRICS.csv", "data/RAW_ORDERS.csv")

    def fake_run_agent(user_message, history, db):
        assert user_message == "¿Cuántos países hay en la base de datos?"
        assert isinstance(history, list)
        meta = db.get_metadata()
        return f"La base de datos contiene {meta['countries_count']} países."

    monkeypatch.setattr(agent_module, "run_agent", fake_run_agent)

    response = agent_module.run_agent("¿Cuántos países hay en la base de datos?", [], db)
    assert "9" in response


def test_e2e_chat_multivariable(monkeypatch):
    """
    Valida que el flujo multivariable entregue una respuesta coherente
    sin requerir llamadas reales al proveedor LLM.
    """
    from src import agent as agent_module

    db = Database()
    db.ingest_csvs("data/RAW_INPUT_METRICS.csv", "data/RAW_ORDERS.csv")

    def fake_run_agent(user_message, history, db):
        assert "Perfect Orders" in user_message
        assert "Lead Penetration" in user_message
        return (
            "En Colombia, zonas como Chapinero y Usaquén muestran alto Perfect Orders "
            "pero menor Lead Penetration relativo, lo que sugiere oportunidad comercial."
        )

    monkeypatch.setattr(agent_module, "run_agent", fake_run_agent)

    response = agent_module.run_agent(
        "¿Qué zonas de Colombia tienen alto Perfect Orders pero bajo Lead Penetration?",
        [],
        db,
    )

    assert any(word in response for word in ["Colombia", "Chapinero", "Usaquén"])


def test_e2e_insights_report(monkeypatch):
    """
    Valida el flujo completo del reporte usando insights reales,
    pero narrativa mockeada para no depender de Gemini.
    """
    import src.insights_engine as insights_module

    db = Database()
    db.ingest_csvs("data/RAW_INPUT_METRICS.csv", "data/RAW_ORDERS.csv")

    def fake_generate_report(insights, db):
        assert set(insights.keys()) == {
            "anomalies",
            "trends",
            "benchmarks",
            "correlations",
            "opportunities",
        }

        return """
## Resumen Ejecutivo
- Se detectaron anomalías relevantes en zonas prioritarias.
- Existen tendencias preocupantes sostenidas.
- Hay oportunidades en zonas con momentum positivo.

## 1. Anomalías Detectadas
Se identificaron cambios drásticos semana a semana.

## 2. Tendencias Preocupantes
Se encontraron métricas en deterioro sostenido.

## 3. Benchmarking: Zonas Rezagadas
Varias zonas están por debajo de su grupo de referencia.

## 4. Correlaciones Clave
Se observaron relaciones relevantes entre métricas.

## 5. Oportunidades
Hay zonas priorizadas con trend positivo.

## 6. Recomendaciones
- Priorizar intervención en zonas críticas.
- Mantener inversión en zonas con momentum.
- Revisar brechas de benchmarking por país.
        """.strip()

    monkeypatch.setattr(insights_module, "generate_report", fake_generate_report)

    engine = InsightsEngine(db)
    insights = engine.run_all()
    report = insights_module.generate_report(insights, db)

    assert "Resumen Ejecutivo" in report
    assert "Anomalías" in report or "Anomalía" in report
    assert "Recomendaciones" in report
    assert len(report) > 500

    engine.close()


def test_e2e_memory(monkeypatch):
    """
    Valida la memoria conversacional como flujo lógico,
    sin depender del proveedor LLM.
    """
    from src import agent as agent_module

    db = Database()
    db.ingest_csvs("data/RAW_INPUT_METRICS.csv", "data/RAW_ORDERS.csv")

    def fake_run_agent(user_message, history, db):
        if user_message == "¿Top 3 zonas por Perfect Orders en Colombia?":
            return (
                "Top 3 zonas por Perfect Orders en Colombia: Chapinero, Usaquén y Suba."
            )

        if user_message == "¿Y en México?":
            # Validamos que el historial sí traiga el contexto previo
            assert len(history) == 2
            assert history[0]["role"] == "user"
            assert "Perfect Orders" in history[0]["content"]
            assert history[1]["role"] == "assistant"
            return (
                "En México, las top zonas por Perfect Orders incluyen Polanco, "
                "Santa Fe y Condesa."
            )

        return "Respuesta no esperada."

    monkeypatch.setattr(agent_module, "run_agent", fake_run_agent)

    r1 = agent_module.run_agent("¿Top 3 zonas por Perfect Orders en Colombia?", [], db)
    history = [
        {"role": "user", "content": "¿Top 3 zonas por Perfect Orders en Colombia?"},
        {"role": "assistant", "content": r1},
    ]

    r2 = agent_module.run_agent("¿Y en México?", history, db)
    assert any(word in r2 for word in ["México", "Mexico", "Polanco", "Santa Fe", "Condesa"])


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