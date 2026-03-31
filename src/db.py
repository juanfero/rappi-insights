# src/db.py
# ──────────────────────────────────────────────────────────────
# Ingesta CSV → SQLite, schema, columnas derivadas, metadata
# ──────────────────────────────────────────────────────────────

import sqlite3
import logging
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd

from src.config import (
    DEFAULT_DB_PATH,
    DEFAULT_METRICS_CSV,
    DEFAULT_ORDERS_CSV,
    WEEK_COLUMNS_METRICS_RAW,
    WEEK_COLUMNS_ORDERS_RAW,
    WEEK_COLUMNS_CLEAN,
    REQUIRED_COLUMNS_METRICS,
    REQUIRED_COLUMNS_ORDERS,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# DDL
# ─────────────────────────────────────────────────────────────

_DDL_METRICS = """
CREATE TABLE IF NOT EXISTS metrics (
    country       TEXT NOT NULL,
    city          TEXT NOT NULL,
    zone          TEXT NOT NULL,
    zone_type     TEXT,
    zone_priority TEXT,
    metric        TEXT NOT NULL,
    w8 REAL, w7 REAL, w6 REAL, w5 REAL, w4 REAL,
    w3 REAL, w2 REAL, w1 REAL, w0 REAL,
    wow_change    REAL,
    trend         REAL
);
"""

_DDL_ORDERS = """
CREATE TABLE IF NOT EXISTS orders (
    country TEXT NOT NULL,
    city    TEXT NOT NULL,
    zone    TEXT NOT NULL,
    metric  TEXT DEFAULT 'Orders',
    w8 REAL, w7 REAL, w6 REAL, w5 REAL, w4 REAL,
    w3 REAL, w2 REAL, w1 REAL, w0 REAL,
    wow_change REAL,
    trend      REAL
);
"""

# Vista unificada: primero todas las filas de metrics,
# luego órdenes enriquecidas con zone_type/zone_priority via LEFT JOIN.
_DDL_VIEW = """
CREATE VIEW IF NOT EXISTS all_data AS
    SELECT
        country, city, zone, zone_type, zone_priority, metric,
        w8, w7, w6, w5, w4, w3, w2, w1, w0,
        wow_change, trend
    FROM metrics
    UNION ALL
    SELECT
        COALESCE(m.country, o.country) AS country,
        COALESCE(m.city,    o.city)    AS city,
        o.zone,
        m.zone_type,
        m.zone_priority,
        o.metric,
        o.w8, o.w7, o.w6, o.w5, o.w4,
        o.w3, o.w2, o.w1, o.w0,
        o.wow_change, o.trend
    FROM orders o
    LEFT JOIN (
        SELECT DISTINCT country, city, zone, zone_type, zone_priority
        FROM metrics
    ) m ON o.country = m.country
       AND o.city    = m.city
       AND o.zone    = m.zone;
"""

_DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_metrics_country        ON metrics(country);",
    "CREATE INDEX IF NOT EXISTS idx_metrics_metric         ON metrics(metric);",
    "CREATE INDEX IF NOT EXISTS idx_metrics_zone           ON metrics(zone);",
    "CREATE INDEX IF NOT EXISTS idx_orders_zone            ON orders(zone);",
    "CREATE INDEX IF NOT EXISTS idx_metrics_country_metric ON metrics(country, metric);",
]


# ─────────────────────────────────────────────────────────────
# Funciones auxiliares (puras, testeables de forma aislada)
# ─────────────────────────────────────────────────────────────

def _validate_columns(df: pd.DataFrame, required: list[str], label: str) -> None:
    """Lanza ValueError si faltan columnas requeridas."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Columnas faltantes en {label}: {missing}"
        )


def _rename_week_columns(
    df: pd.DataFrame,
    raw_cols: list[str],
    label: str,
) -> pd.DataFrame:
    """Renombra columnas de semanas de formato raw al formato limpio w8..w0."""
    rename_map = dict(zip(raw_cols, WEEK_COLUMNS_CLEAN))
    missing = [c for c in raw_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Columnas de semanas faltantes en {label}: {missing}"
        )
    return df.rename(columns=rename_map)


def _calc_wow_change(df: pd.DataFrame) -> pd.Series:
    """
    Calcula (w0 - w1) / w1 * 100 redondeado a 2 decimales.
    Retorna NULL (NaN → None en SQLite) cuando:
      - w1 es NaN
      - w1 es 0
      - w0 es NaN
    """
    w0 = pd.to_numeric(df["w0"], errors="coerce")
    w1 = pd.to_numeric(df["w1"], errors="coerce")

    valid = ~pd.isna(w0) & ~pd.isna(w1) & (w1 != 0)

    result = pd.Series(np.nan, index=df.index, dtype="float64")
    result[valid] = ((w0[valid] - w1[valid]) / w1[valid] * 100).round(2)
    return result


def _calc_trend(row: pd.Series) -> float | None:
    """
    Pendiente de regresión lineal (np.polyfit grado 1) sobre los valores
    no-NaN de w8..w0.
    Retorna None si hay menos de 2 valores disponibles.
    """
    values = [row[c] for c in WEEK_COLUMNS_CLEAN]
    clean = [v for v in values if v is not None and not (isinstance(v, float) and np.isnan(v))]
    if len(clean) < 2:
        return None
    x = np.arange(len(clean), dtype=float)
    slope = np.polyfit(x, clean, 1)[0]
    return round(float(slope), 6)


# ─────────────────────────────────────────────────────────────
# Clase principal
# ─────────────────────────────────────────────────────────────

class Database:
    """
    Gestiona el ciclo completo: CSV → SQLite → acceso read-only + metadata.
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path

    # ── Ingesta desde rutas de archivo ───────────────────────

    def ingest_csvs(
        self,
        metrics_path: str = DEFAULT_METRICS_CSV,
        orders_path: str = DEFAULT_ORDERS_CSV,
    ) -> None:
        """
        Lee los CSV desde disco y ejecuta la ingesta completa.
        Elimina rappi.db anterior si existe (idempotente).
        """
        logger.info("Leyendo CSV de métricas: %s", metrics_path)
        metrics_df = pd.read_csv(metrics_path)

        logger.info("Leyendo CSV de órdenes: %s", orders_path)
        orders_df = pd.read_csv(orders_path)

        self._ingest(metrics_df, orders_df)

    # ── Ingesta desde file-objects (para Streamlit uploader) ─

    def ingest_from_uploads(self, metrics_file, orders_file) -> None:
        """
        Acepta file-objects (BytesIO, UploadedFile, etc.).
        """
        metrics_df = pd.read_csv(metrics_file)
        orders_df  = pd.read_csv(orders_file)
        self._ingest(metrics_df, orders_df)

    # ── Pipeline interno ──────────────────────────────────────

    def _ingest(self, metrics_df: pd.DataFrame, orders_df: pd.DataFrame) -> None:
        metrics_clean = self._prepare_metrics(metrics_df)
        orders_clean  = self._prepare_orders(orders_df)
        self._write_to_sqlite(metrics_clean, orders_clean)
        self._verify_counts(metrics_clean, orders_clean)

    def _prepare_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        # 1. Validar columnas
        _validate_columns(df, REQUIRED_COLUMNS_METRICS, "RAW_INPUT_METRICS")

        # 2. Renombrar semanas
        df = _rename_week_columns(df, WEEK_COLUMNS_METRICS_RAW, "metrics")

        # 3. Renombrar columnas de metadatos → lowercase estándar
        df = df.rename(columns={
            "COUNTRY":            "country",
            "CITY":               "city",
            "ZONE":               "zone",
            "ZONE_TYPE":          "zone_type",
            "ZONE_PRIORITIZATION":"zone_priority",
            "METRIC":             "metric",
        })

        # 4. Convertir columnas de semanas a numérico
        for col in WEEK_COLUMNS_CLEAN:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # 5. Calcular wow_change
        df["wow_change"] = _calc_wow_change(df)

        # 6. Calcular trend (apply fila por fila)
        df["trend"] = df.apply(_calc_trend, axis=1)

        # 7. Seleccionar y ordenar columnas finales
        cols = [
            "country", "city", "zone", "zone_type", "zone_priority", "metric",
            *WEEK_COLUMNS_CLEAN, "wow_change", "trend",
        ]
        return df[cols]

    def _prepare_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        # 1. Validar columnas
        _validate_columns(df, REQUIRED_COLUMNS_ORDERS, "RAW_ORDERS")

        # 2. Renombrar semanas
        df = _rename_week_columns(df, WEEK_COLUMNS_ORDERS_RAW, "orders")

        # 3. Renombrar columnas de metadatos → lowercase
        df = df.rename(columns={
            "COUNTRY": "country",
            "CITY":    "city",
            "ZONE":    "zone",
        })

        # 4. Añadir columna metric con valor fijo
        df["metric"] = "Orders"

        # 5. Convertir columnas de semanas a numérico
        for col in WEEK_COLUMNS_CLEAN:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # 6. Calcular wow_change y trend
        df["wow_change"] = _calc_wow_change(df)
        df["trend"]      = df.apply(_calc_trend, axis=1)

        # 7. Seleccionar y ordenar columnas finales
        cols = [
            "country", "city", "zone", "metric",
            *WEEK_COLUMNS_CLEAN, "wow_change", "trend",
        ]
        return df[cols]

    def _write_to_sqlite(
        self,
        metrics_df: pd.DataFrame,
        orders_df: pd.DataFrame,
    ) -> None:
        """Escribe tablas, vista e índices. Elimina DB anterior si existe."""
        db_file = Path(self.db_path)
        if db_file.exists():
            logger.info("Eliminando DB anterior: %s", self.db_path)
            db_file.unlink()

        conn = sqlite3.connect(self.db_path)
        try:
            # Tablas
            conn.execute(_DDL_METRICS)
            conn.execute(_DDL_ORDERS)

            # Datos (replace = ya manejado por unlink arriba; usamos append)
            metrics_df.to_sql("metrics", conn, if_exists="append", index=False)
            orders_df.to_sql("orders",  conn, if_exists="append", index=False)

            # Vista
            conn.execute(_DDL_VIEW)

            # Índices
            for ddl in _DDL_INDEXES:
                conn.execute(ddl)

            conn.commit()
            logger.info(
                "DB creada: %s | metrics=%d | orders=%d",
                self.db_path, len(metrics_df), len(orders_df),
            )
        finally:
            conn.close()

    def _verify_counts(
        self,
        metrics_df: pd.DataFrame,
        orders_df: pd.DataFrame,
    ) -> None:
        """Log de verificación post-ingesta. No lanza excepción, solo advierte."""
        conn = sqlite3.connect(self.db_path)
        try:
            m = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
            o = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            v = conn.execute("SELECT COUNT(*) FROM all_data").fetchone()[0]
        finally:
            conn.close()

        if m != len(metrics_df):
            logger.warning("metrics: esperado=%d, en DB=%d", len(metrics_df), m)
        if o != len(orders_df):
            logger.warning("orders: esperado=%d, en DB=%d", len(orders_df), o)

        logger.info("Verificación: metrics=%d | orders=%d | all_data=%d", m, o, v)

    # ── Acceso read-only ──────────────────────────────────────

    def get_readonly_connection(self) -> sqlite3.Connection:
        """
        Conexión SQLite en modo lectura. Cualquier intento de escritura
        falla a nivel de SQLite (no depende de validación en Python).
        """
        conn = sqlite3.connect(
            f"file:{self.db_path}?mode=ro",
            uri=True,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        return conn

    # ── Metadata ──────────────────────────────────────────────

    def get_metadata(self) -> dict:
        """
        Retorna un diccionario con información del dataset para
        inyectar en el system prompt del LLM.
        """
        conn = self.get_readonly_connection()
        try:
            countries = sorted(
                r[0] for r in conn.execute(
                    "SELECT DISTINCT country FROM metrics ORDER BY country"
                )
            )
            metrics = sorted(
                r[0] for r in conn.execute(
                    "SELECT DISTINCT metric FROM metrics ORDER BY metric"
                )
            )
            zone_types = sorted(
                r[0] for r in conn.execute(
                    "SELECT DISTINCT zone_type FROM metrics WHERE zone_type IS NOT NULL"
                )
            )
            zone_priorities = sorted(
                r[0] for r in conn.execute(
                    "SELECT DISTINCT zone_priority FROM metrics WHERE zone_priority IS NOT NULL"
                )
            )
            total_zones = conn.execute(
                "SELECT COUNT(DISTINCT zone) FROM metrics"
            ).fetchone()[0]
            total_rows_metrics = conn.execute(
                "SELECT COUNT(*) FROM metrics"
            ).fetchone()[0]
            total_rows_orders = conn.execute(
                "SELECT COUNT(*) FROM orders"
            ).fetchone()[0]

            # 3 zonas de ejemplo por país
            sample_zones: dict[str, list[str]] = {}
            for country in countries:
                rows = conn.execute(
                    "SELECT DISTINCT zone FROM metrics WHERE country = ? LIMIT 3",
                    (country,),
                ).fetchall()
                sample_zones[country] = [r[0] for r in rows]

        finally:
            conn.close()

        return {
            "countries":          countries,
            "countries_count":    len(countries),
            "metrics":            metrics,
            "zone_types":         zone_types,
            "zone_priorities":    zone_priorities,
            "total_zones":        total_zones,
            "total_rows_metrics": total_rows_metrics,
            "total_rows_orders":  total_rows_orders,
            "sample_zones":       sample_zones,
        }