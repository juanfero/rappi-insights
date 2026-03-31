# tests/test_db.py
# ──────────────────────────────────────────────────────────────
# Tests de Fase 1: validación de schema, conteos, cálculos,
# vista all_data, conexión read-only y metadata.
# ──────────────────────────────────────────────────────────────

import sqlite3
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.config import (
    DEFAULT_METRICS_CSV,
    DEFAULT_ORDERS_CSV,
    METRICS_DICTIONARY,
    WEEK_COLUMNS_CLEAN,
)
from src.db import Database


# ── Fixture: DB creada una sola vez por sesión de tests ───────

@pytest.fixture(scope="session")
def db(tmp_path_factory):
    """
    Crea rappi.db en un directorio temporal usando los CSV reales.
    Scope=session para no repetir la ingesta en cada test.
    """
    tmp = tmp_path_factory.mktemp("db")
    db_path = str(tmp / "rappi_test.db")
    database = Database(db_path=db_path)
    database.ingest_csvs(
        metrics_path=DEFAULT_METRICS_CSV,
        orders_path=DEFAULT_ORDERS_CSV,
    )
    return database


# ── Test 4.1: Validación de schema ────────────────────────────

def test_schema_validation_metrics_missing_column(tmp_path):
    """Rechaza CSV de métricas con columna ZONE_TYPE ausente."""
    bad_metrics = pd.DataFrame({
        "COUNTRY": ["CO"], "CITY": ["Bogota"], "ZONE": ["X"],
        "ZONE_PRIORITIZATION": ["High Priority"], "METRIC": ["Perfect Orders"],
        # Intencionalmente sin ZONE_TYPE ni columnas de semanas
    })
    valid_orders = pd.read_csv(DEFAULT_ORDERS_CSV)
    db_tmp = Database(db_path=str(tmp_path / "test.db"))
    with pytest.raises(ValueError, match="Columnas faltantes"):
        db_tmp._ingest(bad_metrics, valid_orders)


def test_schema_validation_orders_missing_column(tmp_path):
    """Rechaza CSV de órdenes con columnas de semanas ausentes."""
    valid_metrics = pd.read_csv(DEFAULT_METRICS_CSV)
    bad_orders = pd.DataFrame({
        "COUNTRY": ["CO"], "CITY": ["Bogota"], "ZONE": ["X"],
        # Sin columnas L8W..L0W
    })
    db_tmp = Database(db_path=str(tmp_path / "test.db"))
    with pytest.raises(ValueError, match="Columnas faltantes"):
        db_tmp._ingest(valid_metrics, bad_orders)


# ── Test 4.2: Conteos post-ingesta ────────────────────────────

def test_row_counts(db):
    """Las tablas deben tener exactamente los conteos del dataset real."""
    conn = db.get_readonly_connection()
    try:
        metrics_count = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
        orders_count  = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    finally:
        conn.close()
    assert metrics_count == 12573, f"metrics: esperado 12573, obtenido {metrics_count}"
    assert orders_count  == 1242,  f"orders: esperado 1242, obtenido {orders_count}"


# ── Test 4.3: Renombramiento de columnas ──────────────────────

def test_column_names_metrics(db):
    """Columnas de semanas y metadatos deben estar en formato limpio."""
    conn = db.get_readonly_connection()
    try:
        cursor = conn.execute("SELECT * FROM metrics LIMIT 1")
        cols = [d[0] for d in cursor.description]
    finally:
        conn.close()

    # Columnas limpias presentes
    assert "w0" in cols
    assert "w8" in cols
    assert "zone_priority" in cols
    assert "zone_type" in cols
    assert "wow_change" in cols
    assert "trend" in cols

    # Columnas raw ausentes
    assert "L0W_ROLL" not in cols
    assert "L8W_ROLL" not in cols
    assert "ZONE_PRIORITIZATION" not in cols
    assert "ZONE_TYPE" not in cols
    assert "COUNTRY" not in cols


def test_column_names_orders(db):
    """Columnas de semanas en orders deben estar en formato limpio."""
    conn = db.get_readonly_connection()
    try:
        cursor = conn.execute("SELECT * FROM orders LIMIT 1")
        cols = [d[0] for d in cursor.description]
    finally:
        conn.close()

    assert "w0" in cols
    assert "w8" in cols
    assert "L0W" not in cols
    assert "L8W" not in cols


# ── Test 4.4: wow_change correcto ─────────────────────────────

def test_wow_change_normal_case(db):
    """Verifica cálculo numérico de wow_change para una fila conocida."""
    conn = db.get_readonly_connection()
    try:
        row = conn.execute("""
            SELECT w0, w1, wow_change FROM metrics
            WHERE metric = 'Perfect Orders'
              AND w1 IS NOT NULL AND w1 != 0 AND w0 IS NOT NULL
            LIMIT 1
        """).fetchone()
    finally:
        conn.close()

    assert row is not None, "No se encontró fila válida para test wow_change"
    expected = (row["w0"] - row["w1"]) / row["w1"] * 100
    assert abs(row["wow_change"] - expected) < 0.01, (
        f"wow_change={row['wow_change']}, esperado≈{expected:.4f}"
    )


def test_wow_change_zero_w1(db):
    """wow_change debe ser NULL cuando w1 == 0."""
    conn = db.get_readonly_connection()
    try:
        bad = conn.execute("""
            SELECT COUNT(*) FROM metrics
            WHERE w1 = 0 AND wow_change IS NOT NULL
        """).fetchone()[0]
    finally:
        conn.close()
    assert bad == 0, f"Hay {bad} filas con w1=0 y wow_change no NULL"


# ── Test 4.10: wow_change con w1 NULL ────────────────────────

def test_wow_change_null_w1(db):
    """wow_change debe ser NULL cuando w1 es NULL."""
    conn = db.get_readonly_connection()
    try:
        bad = conn.execute("""
            SELECT COUNT(*) FROM metrics
            WHERE w1 IS NULL AND wow_change IS NOT NULL
        """).fetchone()[0]
    finally:
        conn.close()
    assert bad == 0, f"Hay {bad} filas con w1 NULL y wow_change no NULL"


# ── Test 4.5: trend correcto ──────────────────────────────────

def test_trend_calculation(db):
    """trend debe ser la pendiente de regresión lineal sobre valores no-NaN."""
    conn = db.get_readonly_connection()
    try:
        row = conn.execute("""
            SELECT w8,w7,w6,w5,w4,w3,w2,w1,w0, trend FROM metrics
            WHERE metric = 'Perfect Orders'
              AND w8 IS NOT NULL AND trend IS NOT NULL
            LIMIT 1
        """).fetchone()
    finally:
        conn.close()

    assert row is not None
    values = [row[f"w{i}"] for i in range(8, -1, -1)]
    clean  = [v for v in values if v is not None]
    assert len(clean) >= 2

    x = np.arange(len(clean), dtype=float)
    expected_slope = np.polyfit(x, clean, 1)[0]
    assert abs(row["trend"] - expected_slope) < 1e-4, (
        f"trend={row['trend']}, esperado≈{expected_slope:.6f}"
    )


def test_trend_null_when_insufficient_data(db):
    """trend debe ser NULL cuando hay menos de 2 semanas con datos."""
    conn = db.get_readonly_connection()
    try:
        # Construir un conteo de semanas no-null y comparar con trend
        # Si trend es NOT NULL, debe haber al menos 2 semanas con dato
        # Verificamos que no haya filas con trend no-null y solo 0-1 semanas válidas
        result = conn.execute("""
            SELECT COUNT(*) FROM metrics
            WHERE trend IS NOT NULL
              AND (
                (CASE WHEN w0 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN w1 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN w2 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN w3 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN w4 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN w5 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN w6 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN w7 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN w8 IS NOT NULL THEN 1 ELSE 0 END) < 2
              )
        """).fetchone()[0]
    finally:
        conn.close()
    assert result == 0, f"Hay {result} filas con trend no-NULL y <2 semanas válidas"


# ── Test 4.6: Vista all_data ──────────────────────────────────

def test_all_data_view_total(db):
    """all_data debe tener exactamente metrics + orders filas."""
    conn = db.get_readonly_connection()
    try:
        total   = conn.execute("SELECT COUNT(*) FROM all_data").fetchone()[0]
        m_count = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
        o_count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    finally:
        conn.close()
    assert total == m_count + o_count, (
        f"all_data={total}, metrics={m_count}, orders={o_count}"
    )


def test_all_data_orders_with_zone_type(db):
    """Las zonas de orders que existen en metrics deben tener zone_type."""
    conn = db.get_readonly_connection()
    try:
        orders_with_type = conn.execute("""
            SELECT COUNT(*) FROM all_data
            WHERE metric = 'Orders' AND zone_type IS NOT NULL
        """).fetchone()[0]
    finally:
        conn.close()
    # Spec indica 226 zonas sin cruce → ~1016 órdenes deberían tener zone_type
    assert orders_with_type >= 900, (
        f"Solo {orders_with_type} filas de orders tienen zone_type"
    )


# ── Test 4.7: Conexión read-only ──────────────────────────────

def test_readonly_blocks_insert(db):
    """INSERT debe fallar con conexión read-only."""
    conn = db.get_readonly_connection()
    try:
        with pytest.raises(Exception):
            conn.execute("INSERT INTO metrics (country) VALUES ('XX')")
    finally:
        conn.close()


def test_readonly_blocks_drop(db):
    """DROP TABLE debe fallar con conexión read-only."""
    conn = db.get_readonly_connection()
    try:
        with pytest.raises(Exception):
            conn.execute("DROP TABLE metrics")
    finally:
        conn.close()


# ── Test 4.8: Metadata completa ───────────────────────────────

def test_metadata(db):
    """get_metadata() debe retornar valores correctos según el dataset real."""
    meta = db.get_metadata()

    assert len(meta["countries"]) == 9
    assert "CO" in meta["countries"]
    assert len(meta["metrics"]) == 13
    assert "Perfect Orders" in meta["metrics"]
    assert meta["total_zones"] == 964
    assert meta["total_rows_metrics"] == 12573
    assert meta["total_rows_orders"] == 1242
    assert set(meta["zone_types"]) == {"Wealthy", "Non Wealthy"}
    assert set(meta["zone_priorities"]) == {"High Priority", "Prioritized", "Not Prioritized"}
    assert "CO" in meta["sample_zones"]
    assert len(meta["sample_zones"]["CO"]) <= 3


# ── Test 4.9: Nombres de métricas consistentes ────────────────

def test_metric_names_match_config(db):
    """Los nombres de métrica en la DB deben coincidir exactamente con METRICS_DICTIONARY."""
    conn = db.get_readonly_connection()
    try:
        db_metrics = {
            r[0] for r in conn.execute("SELECT DISTINCT metric FROM metrics")
        }
    finally:
        conn.close()
    config_metrics = set(METRICS_DICTIONARY.keys())
    diff = db_metrics.symmetric_difference(config_metrics)
    assert db_metrics == config_metrics, (
        f"Diferencia entre DB y config: {diff}"
    )