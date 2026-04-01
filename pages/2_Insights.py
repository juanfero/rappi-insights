# pages/2_Insights.py

from __future__ import annotations

import os
import sys

import streamlit as st

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.db import Database
from src.insights_engine import InsightsEngine, generate_report


st.set_page_config(
    page_title="Rappi Insights Report",
    page_icon="📊",
    layout="wide",
)


def get_db() -> Database:
    if "db" in st.session_state and st.session_state["db"] is not None:
        return st.session_state["db"]

    db = Database()
    st.session_state["db"] = db
    return db


@st.cache_data(ttl=300)
def get_insights_cached(_db_marker: str) -> dict:
    db = Database()
    engine = InsightsEngine(db)
    try:
        return engine.run_all()
    finally:
        engine.close()


def main() -> None:
    st.title("📊 Insights Automáticos")

    db = get_db()
    db_path = os.getenv("RAPPI_DB_PATH", "rappi.db")

    if "insights_results" not in st.session_state:
        st.session_state["insights_results"] = None

    if "insights_report" not in st.session_state:
        st.session_state["insights_report"] = None

    if st.button("🔄 Generar Reporte"):
        with st.spinner("Analizando datos..."):
            insights = get_insights_cached(db_path)
            st.session_state["insights_results"] = insights

        with st.spinner("Generando narrativa..."):
            report = generate_report(insights, db)
            st.session_state["insights_report"] = report

    results = st.session_state.get("insights_results")
    report = st.session_state.get("insights_report")

    with st.sidebar:
        st.subheader("Resumen de detecciones")
        if results:
            st.write(f"Anomalías: {len(results['anomalies'])}")
            st.write(f"Tendencias: {len(results['trends'])}")
            st.write(f"Benchmarks: {len(results['benchmarks'])}")
            st.write(f"Correlaciones: {len(results['correlations'])}")
            st.write(f"Oportunidades: {len(results['opportunities'])}")
        else:
            st.info("Aún no se ha generado el reporte.")

    if report:
        st.markdown(report)
        st.download_button(
            label="📥 Descargar Reporte (.md)",
            data=report,
            file_name="rappi_insights_report.md",
            mime="text/markdown",
        )
    else:
        st.info("Haz clic en 'Generar Reporte' para ejecutar el análisis automático.")


if __name__ == "__main__":
    main()