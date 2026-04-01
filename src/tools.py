# src/tools.py

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd
import plotly.express as px
import streamlit as st


ALLOWED_SQL_PREFIXES = ("SELECT", "WITH")
MAX_RETURNED_ROWS = 50


def _normalize_query(query: str) -> str:
    """
    Limpia espacios exteriores del query.
    """
    if not isinstance(query, str):
        raise TypeError("El query debe ser un string.")
    return query.strip()


def _validate_readonly_query(query: str) -> Optional[str]:
    """
    Valida que el query sea de solo lectura.
    Solo se permiten SELECT o WITH.
    Retorna None si es válido, o un mensaje de error si no lo es.
    """
    normalized = _normalize_query(query)
    upper = normalized.upper()

    if not upper:
        return "Error: El query está vacío."

    if not upper.startswith(ALLOWED_SQL_PREFIXES):
        return (
            "Error: Solo se permiten consultas de solo lectura que comiencen con "
            "SELECT o WITH."
        )

    return None


def _run_query_as_dataframe(query: str, db) -> pd.DataFrame:
    """
    Ejecuta el query sobre la conexión read-only provista por Database.
    """
    conn = db.get_readonly_connection()
    try:
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()


def run_sql(params: Dict[str, Any], db) -> str:
    """
    Ejecuta una consulta SQL SELECT/CTE contra SQLite y retorna una tabla markdown.

    Reglas del spec:
    - Solo SELECT o WITH
    - Conexión read-only
    - Máximo 50 filas mostradas
    - Si falla, retorna error legible
    """
    query = params.get("query", "")
    validation_error = _validate_readonly_query(query)
    if validation_error:
        return validation_error

    query = _normalize_query(query)

    try:
        df = _run_query_as_dataframe(query, db)
    except Exception as e:
        return f"Error SQL: {str(e)}\nCorrige el query."

    if df.empty:
        return "El query no devolvió resultados."

    total_rows = len(df)
    if total_rows > MAX_RETURNED_ROWS:
        truncated = df.head(MAX_RETURNED_ROWS)
        table_md = truncated.to_markdown(index=False)
        return (
            f"(Mostrando {MAX_RETURNED_ROWS} de {total_rows} filas)\n\n"
            f"{table_md}"
        )

    return df.to_markdown(index=False)


def plot_chart(params: Dict[str, Any], db) -> str:
    """
    Ejecuta un query SQL y genera un gráfico Plotly guardándolo en
    st.session_state['last_chart'].

    Parámetros esperados:
    - query
    - chart_type: line | bar | scatter | heatmap
    - title
    - x
    - y
    - color (opcional)
    """
    query = params.get("query", "")
    chart_type = params.get("chart_type", "")
    title = params.get("title", "Chart")
    x_col = params.get("x", "")
    y_col = params.get("y", "")
    color_col = params.get("color")

    validation_error = _validate_readonly_query(query)
    if validation_error:
        return validation_error

    query = _normalize_query(query)

    try:
        df = _run_query_as_dataframe(query, db)
    except Exception as e:
        return f"Error SQL: {str(e)}\nCorrige el query."

    if df.empty:
        return "El query no devolvió datos para graficar."

    cols = df.columns.tolist()

    if x_col not in cols:
        return (
            f"Error: La columna '{x_col}' no existe en el resultado. "
            f"Columnas disponibles: {cols}"
        )

    if y_col not in cols:
        return (
            f"Error: La columna '{y_col}' no existe en el resultado. "
            f"Columnas disponibles: {cols}"
        )

    if color_col is not None and color_col != "" and color_col not in cols:
        return (
            f"Error: La columna '{color_col}' no existe en el resultado. "
            f"Columnas disponibles: {cols}"
        )

    chart_type = str(chart_type).strip().lower()

    try:
        if chart_type == "line":
            fig = px.line(df, x=x_col, y=y_col, color=color_col, title=title)
        elif chart_type == "bar":
            fig = px.bar(df, x=x_col, y=y_col, color=color_col, title=title)
        elif chart_type == "scatter":
            fig = px.scatter(df, x=x_col, y=y_col, color=color_col, title=title)
        elif chart_type == "heatmap":
            fig = px.density_heatmap(
                df,
                x=x_col,
                y=y_col,
                z=color_col if color_col in cols else None,
                title=title,
            )
        else:
            return (
                f"Error: Tipo de gráfico '{chart_type}' no soportado. "
                "Usa: line, bar, scatter, heatmap."
            )
    except Exception as e:
        return f"Error al generar el gráfico: {str(e)}"

    st.session_state["last_chart"] = fig
    return f"Gráfico generado correctamente con {len(df)} filas."


def execute_tool(tool_name: str, params: Dict[str, Any], db) -> str:
    """
    Dispatcher general de tools.
    """
    if tool_name == "run_sql":
        return run_sql(params, db)

    if tool_name == "plot_chart":
        return plot_chart(params, db)

    return f"Error: Tool desconocida '{tool_name}'."