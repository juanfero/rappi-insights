# src/insights_engine.py

from __future__ import annotations

import json
import os
from typing import Any

import pandas as pd
import scipy.stats
from dotenv import load_dotenv
from google import genai

from src.config import DEFAULT_MODEL


load_dotenv()


class InsightsEngine:
    def __init__(self, db):
        self.db = db
        self.conn = db.get_readonly_connection()

    def close(self):
        self.conn.close()

    def _query_to_dicts(self, sql: str) -> list[dict]:
        df = pd.read_sql_query(sql, self.conn)
        return df.to_dict(orient="records")

    def run_all(self) -> dict:
        return {
            "anomalies": self.detect_anomalies(),
            "trends": self.detect_concerning_trends(),
            "benchmarks": self.detect_benchmark_gaps(),
            "correlations": self.detect_correlations(),
            "opportunities": self.detect_opportunities(),
        }

    def detect_anomalies(self) -> list[dict]:
        sql = """
        SELECT
            zone, country, city, metric,
            w1 AS previous_value,
            w0 AS current_value,
            wow_change,
            CASE WHEN wow_change > 0 THEN 'improvement' ELSE 'deterioration' END AS direction,
            CASE
                WHEN ABS(wow_change) > 30 THEN 'critical'
                WHEN ABS(wow_change) > 20 THEN 'high'
                ELSE 'medium'
            END AS severity,
            zone_type, zone_priority
        FROM metrics
        WHERE ABS(wow_change) > 10
          AND w1 IS NOT NULL
          AND w1 != 0
        ORDER BY ABS(wow_change) DESC
        LIMIT 30
        """
        return self._query_to_dicts(sql)

    def detect_concerning_trends(self) -> list[dict]:
        sql = """
        SELECT
            zone, country, city, metric,
            w0 AS current_value,
            trend,
            zone_type, zone_priority,
            CASE
                WHEN w0 < w1 AND w1 < w2 AND w2 < w3 AND w3 < w4 AND w4 < w5 THEN 5
                WHEN w0 < w1 AND w1 < w2 AND w2 < w3 AND w3 < w4 THEN 4
                WHEN w0 < w1 AND w1 < w2 AND w2 < w3 THEN 3
            END AS consecutive_drops
        FROM metrics
        WHERE w0 < w1 AND w1 < w2 AND w2 < w3
          AND w0 IS NOT NULL AND w1 IS NOT NULL AND w2 IS NOT NULL AND w3 IS NOT NULL
        ORDER BY consecutive_drops DESC, ABS(trend) DESC
        LIMIT 20
        """
        return self._query_to_dicts(sql)

    def detect_benchmark_gaps(self) -> list[dict]:
        sql = """
        WITH stats AS (
            SELECT
                country, zone_type, metric,
                AVG(w0) AS group_mean,
                COUNT(*) AS n,
                AVG(w0 * w0) - AVG(w0) * AVG(w0) AS variance
            FROM metrics
            WHERE w0 IS NOT NULL
            GROUP BY country, zone_type, metric
            HAVING COUNT(*) >= 5
        )
        SELECT
            m.zone, m.country, m.city, m.metric, m.zone_type,
            m.w0 AS zone_value,
            ROUND(s.group_mean, 4) AS group_mean,
            ROUND(SQRT(s.variance), 4) AS group_std,
            ROUND((m.w0 - s.group_mean) / s.group_mean * 100, 2) AS gap_pct,
            s.n AS group_size,
            m.zone_priority
        FROM metrics m
        JOIN stats s ON m.country = s.country
                    AND m.zone_type = s.zone_type
                    AND m.metric = s.metric
        WHERE s.variance > 0
          AND m.w0 IS NOT NULL
          AND s.group_mean != 0
          AND m.w0 < (s.group_mean - 1.5 * SQRT(s.variance))
        ORDER BY gap_pct ASC
        LIMIT 20
        """
        return self._query_to_dicts(sql)

    def detect_correlations(self) -> list[dict]:
        df = pd.read_sql_query(
            "SELECT country, city, zone, metric, w0 FROM metrics WHERE w0 IS NOT NULL",
            self.conn,
        )

        pivot = df.pivot_table(
            index=["country", "city", "zone"],
            columns="metric",
            values="w0",
        )

        results = []
        metrics = pivot.columns.tolist()

        for i, m1 in enumerate(metrics):
            for m2 in metrics[i + 1:]:
                valid = pivot[[m1, m2]].dropna()
                if len(valid) < 30:
                    continue

                corr, pvalue = scipy.stats.pearsonr(valid[m1], valid[m2])
                if abs(corr) > 0.5 and pvalue < 0.05:
                    results.append(
                        {
                            "metric_1": m1,
                            "metric_2": m2,
                            "correlation": round(float(corr), 3),
                            "p_value": round(float(pvalue), 4),
                            "n_zones": int(len(valid)),
                            "direction": "positiva" if corr > 0 else "negativa",
                            "strength": "fuerte" if abs(corr) > 0.7 else "moderada",
                        }
                    )

        return sorted(results, key=lambda x: abs(x["correlation"]), reverse=True)

    def detect_opportunities(self) -> list[dict]:
        sql = """
        WITH country_avg AS (
            SELECT country, metric, AVG(w0) AS avg_w0
            FROM metrics
            WHERE w0 IS NOT NULL
            GROUP BY country, metric
        )
        SELECT
            m.zone, m.country, m.city, m.metric,
            m.w0 AS current_value,
            ROUND(c.avg_w0, 4) AS country_avg,
            ROUND((m.w0 - c.avg_w0) / c.avg_w0 * 100, 2) AS gap_pct,
            m.trend,
            m.wow_change,
            m.zone_priority, m.zone_type
        FROM metrics m
        JOIN country_avg c ON m.country = c.country AND m.metric = c.metric
        WHERE m.zone_priority IN ('High Priority', 'Prioritized')
          AND m.w0 IS NOT NULL
          AND c.avg_w0 != 0
          AND m.w0 < c.avg_w0
          AND m.trend > 0
        ORDER BY m.trend DESC
        LIMIT 15
        """
        return self._query_to_dicts(sql)


def generate_report(insights: dict, db) -> str:
    """
    Genera reporte ejecutivo en Markdown usando Gemini.
    Mantiene el contrato del spec original, cambiando solo el proveedor LLM.
    """
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "No se encontró GEMINI_API_KEY ni GOOGLE_API_KEY para generar el reporte."
        )

    meta = db.get_metadata()

    prompt = f"""
Genera un reporte ejecutivo en Markdown sobre la operación de Rappi.

Datos analizados: {meta['total_zones']} zonas en {meta['countries_count']} países.

Insights detectados automáticamente:
{json.dumps(insights, indent=2, ensure_ascii=False)}

Estructura requerida:
## Resumen Ejecutivo
Top 3-5 hallazgos más críticos en bullets concisos.

## 1. Anomalías Detectadas
Cambios drásticos semana a semana que requieren atención inmediata.
Prioriza severity "critical" y zonas "High Priority".

## 2. Tendencias Preocupantes
Métricas en deterioro sostenido (3+ semanas).
Menciona cuántas semanas consecutivas llevan cayendo.

## 3. Benchmarking: Zonas Rezagadas
Zonas significativamente por debajo de sus pares.
Contextualiza el gap vs el promedio del grupo.

## 4. Correlaciones Clave
Relaciones entre métricas. Explica la implicación operacional.

## 5. Oportunidades
Zonas con momentum positivo pero aún debajo del promedio.

## 6. Recomendaciones
3-5 acciones concretas, priorizadas por impacto y urgencia.
Cada recomendación debe ser específica: zona + métrica + acción.

Directrices:
- Sé conciso y directo.
- Usa Markdown limpio.
- No inventes datos.
- No repitas datos crudos si no agregan valor.
- Las recomendaciones deben ser accionables.
- Responde en español.
- Longitud total aproximada: entre 800 y 2000 palabras.
""".strip()

    client = genai.Client(api_key=api_key)
    model_name = os.getenv("RAPPI_MODEL", DEFAULT_MODEL)

    response = client.models.generate_content(
        model=model_name,
        contents=prompt,
    )

    text = getattr(response, "text", None)
    if text and text.strip():
        return text.strip()

    raise RuntimeError("Gemini no devolvió texto para el reporte.")