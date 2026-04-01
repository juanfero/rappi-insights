from __future__ import annotations

import os
import sys
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db import Database
from src.config import DEFAULT_METRICS_CSV, DEFAULT_ORDERS_CSV

load_dotenv()

st.set_page_config(
    page_title="Rappi Insights",
    page_icon="🟠",
    layout="wide",
    initial_sidebar_state="expanded",
)


def init_session_state() -> None:
    st.session_state.setdefault("db", None)
    st.session_state.setdefault("messages", [])
    st.session_state.setdefault("last_chart", None)
    st.session_state.setdefault("insights_cache", None)


def check_api_key() -> str | None:
    key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not key:
        st.error(
            "⚠️ No se encontró GEMINI_API_KEY ni GOOGLE_API_KEY. "
            "Configura tu archivo .env antes de usar la app."
        )
        st.code("GEMINI_API_KEY=tu_api_key")
        return None
    return key


def ensure_database() -> Database | None:
    db_path = Path(os.getenv("RAPPI_DB_PATH", "rappi.db"))

    if st.session_state["db"] is not None:
        return st.session_state["db"]

    try:
        db = Database()

        if db_path.exists():
            st.session_state["db"] = db
            return db

        metrics_csv = Path(DEFAULT_METRICS_CSV)
        orders_csv = Path(DEFAULT_ORDERS_CSV)

        if metrics_csv.exists() and orders_csv.exists():
            with st.spinner("Inicializando base de datos desde CSVs..."):
                db.ingest_csvs(str(metrics_csv), str(orders_csv))
            st.session_state["db"] = db
            return db

        return None
    except Exception as e:
        st.error(f"❌ Error al inicializar la base de datos: {e}")
        return None


def render_sidebar(db: Database | None) -> None:
    with st.sidebar:
        st.title("🟠 Rappi Insights")

        if db is None:
            st.warning("Estado: DB no cargada")
        else:
            st.success("Estado: ✅ DB cargada")

            try:
                meta = db.get_metadata()
                st.subheader("Dataset Info")
                st.write(f"Zonas: {meta['total_zones']}")
                st.write(f"Países: {meta['countries_count']}")
                st.write(f"Métricas: {len(meta['metrics'])}")
                st.write(f"Filas métricas: {meta['total_rows_metrics']}")
                st.write(f"Filas órdenes: {meta['total_rows_orders']}")
            except Exception as e:
                st.error(f"No se pudo leer metadata: {e}")

        st.divider()
        st.subheader("📂 Actualizar Datos")

        metrics_file = st.file_uploader(
            "CSV de Métricas",
            type="csv",
            key="up_metrics",
        )
        orders_file = st.file_uploader(
            "CSV de Órdenes",
            type="csv",
            key="up_orders",
        )

        if metrics_file and orders_file:
            if st.button("🔄 Cargar datos"):
                try:
                    new_db = Database()
                    new_db.ingest_from_uploads(metrics_file, orders_file)
                    st.session_state["db"] = new_db
                    st.session_state["messages"] = []
                    st.session_state["insights_cache"] = None
                    st.session_state["last_chart"] = None
                    meta = new_db.get_metadata()
                    st.success(f"✅ {meta['total_zones']} zonas cargadas")
                    st.rerun()
                except ValueError as e:
                    st.error(f"❌ Error de formato: {e}")
                except Exception as e:
                    st.error(f"❌ Error inesperado: {e}")

        st.divider()
        st.subheader("Modelo")
        model_name = os.getenv("RAPPI_MODEL", "gemini-3-flash-preview")
        st.write(model_name)
        st.caption("Costo aproximado depende de tu plan/cuota activa.")


def main() -> None:
    init_session_state()
    check_api_key()

    db = ensure_database()
    render_sidebar(db)


    st.title("🟠 Rappi Insights")
    st.write("Sistema de análisis inteligente para operaciones.")

    if db is None:
        st.warning(
            "No se encontró una base de datos lista ni ambos CSVs en la carpeta data/. "
            "Usa el sidebar para cargar archivos."
        )
    else:
        meta = db.get_metadata()
        st.success(
            f"DB lista: {meta['total_zones']} zonas, "
            f"{meta['countries_count']} países, "
            f"{len(meta['metrics'])} métricas."
        )

        st.markdown(
            """
### Navegación
Usa el menú lateral de páginas de Streamlit para entrar a:

- **1_Chat** → Bot conversacional
- **2_Insights** → Reporte automático
            """
        )


if __name__ == "__main__":
    main()