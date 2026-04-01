# tests/test_tools.py

from __future__ import annotations

from src.db import Database
from src.tools import execute_tool


def test_run_sql_valid():
    db = Database()
    result = execute_tool(
        "run_sql",
        {"query": "SELECT COUNT(*) as total FROM metrics"},
        db,
    )
    assert "12573" in result
    assert "total" in result


def test_run_sql_rejects_mutation():
    db = Database()
    result = execute_tool(
        "run_sql",
        {"query": "DROP TABLE metrics"},
        db,
    )
    assert "Error" in result or "Solo se permiten" in result


def test_run_sql_returns_error_on_bad_sql():
    db = Database()
    result = execute_tool(
        "run_sql",
        {"query": "SELECT nonexistent_column FROM metrics"},
        db,
    )
    assert "Error SQL" in result


def test_run_sql_truncates():
    db = Database()
    result = execute_tool(
        "run_sql",
        {"query": "SELECT * FROM metrics"},
        db,
    )
    assert "Mostrando 50 de" in result


def test_plot_chart_creates_figure():
    import streamlit as st

    db = Database()

    # Limpieza defensiva
    try:
        st.session_state.clear()
    except Exception:
        pass

    result = execute_tool(
        "plot_chart",
        {
            "query": "SELECT country, AVG(w0) as avg_value FROM metrics WHERE metric = 'Perfect Orders' GROUP BY country",
            "chart_type": "bar",
            "title": "Test Chart",
            "x": "country",
            "y": "avg_value",
        },
        db,
    )

    assert "gráfico generado" in result.lower() or "generado correctamente" in result.lower()
    assert "last_chart" in st.session_state


def test_plot_chart_invalid_x_column():
    db = Database()
    result = execute_tool(
        "plot_chart",
        {
            "query": "SELECT country, AVG(w0) as avg_value FROM metrics WHERE metric = 'Perfect Orders' GROUP BY country",
            "chart_type": "bar",
            "title": "Invalid X",
            "x": "pais",
            "y": "avg_value",
        },
        db,
    )
    assert "no existe en el resultado" in result.lower()


def test_unknown_tool():
    db = Database()
    result = execute_tool("unknown_tool", {}, db)
    assert "tool desconocida" in result.lower()