# pages/1_Chat.py

from __future__ import annotations

import streamlit as st

from src.db import Database
from src.agent import run_agent
from src.config import MAX_CONVERSATION_HISTORY


st.set_page_config(
    page_title="Rappi Insights Chat",
    page_icon="💬",
    layout="wide",
)


def get_db() -> Database:
    if "db" in st.session_state and st.session_state["db"] is not None:
        return st.session_state["db"]

    db = Database()
    st.session_state["db"] = db
    return db


def init_session_state() -> None:
    """
    Inicializa claves de session_state necesarias para el chat.
    """
    st.session_state.setdefault("messages", [])
    st.session_state.setdefault("last_chart", None)


def render_welcome() -> None:
    """
    Mensaje inicial con ejemplos de preguntas.
    """
    st.markdown(
        """
👋 ¡Hola! Soy tu asistente de datos operacionales de Rappi.

Puedo ayudarte a analizar métricas de zonas, países, órdenes y desempeño operacional.

Prueba con alguna de estas preguntas:

- **¿Cuáles son las 5 zonas con mayor Lead Penetration esta semana?**
- **Compara el Perfect Order entre zonas Wealthy y Non Wealthy en México**
- **Muestra la evolución de Gross Profit UE en Chapinero últimas 8 semanas**
- **¿Cuál es el promedio de Lead Penetration por país?**
- **¿Qué zonas tienen alto Lead Penetration pero bajo Perfect Order?**
- **¿Cuáles son las zonas que más crecen en órdenes en las últimas 5 semanas y qué podría explicar el crecimiento?**
        """
    )


def render_history() -> None:
    """
    Renderiza el historial del chat.
    """
    for message in st.session_state["messages"]:
        role = message.get("role", "assistant")
        content = message.get("content", "")

        with st.chat_message(role):
            st.markdown(content)


def get_trimmed_history() -> list[dict]:
    """
    Retorna solo los últimos N pares de mensajes.
    El spec indica truncar el historial para no crecer indefinidamente.
    """
    max_messages = MAX_CONVERSATION_HISTORY * 2
    return st.session_state["messages"][-max_messages:]


def render_last_chart() -> None:
    """
    Renderiza el último gráfico generado por plot_chart si existe.
    Luego lo limpia para no repetirlo en interacciones futuras.
    """
    fig = st.session_state.get("last_chart")
    if fig is not None:
        st.plotly_chart(fig, use_container_width=True)
        st.session_state["last_chart"] = None


def main() -> None:
    init_session_state()
    db = get_db()

    st.title("💬 Asistente de Datos Rappi")

    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("🗑️ Limpiar chat"):
            st.session_state["messages"] = []
            st.session_state["last_chart"] = None
            st.rerun()

    if not st.session_state["messages"]:
        render_welcome()

    render_history()

    user_prompt = st.chat_input("Pregunta sobre datos operacionales...")

    if user_prompt:
        # Render y persistencia del mensaje del usuario
        st.session_state["messages"].append(
            {"role": "user", "content": user_prompt}
        )

        with st.chat_message("user"):
            st.markdown(user_prompt)

        # Contexto truncado antes de agregar la respuesta nueva
        history = get_trimmed_history()[:-1]  # quitamos el último user actual para evitar duplicarlo

        with st.chat_message("assistant"):
            with st.spinner("Analizando..."):
                try:
                    response = run_agent(user_prompt, history, db)
                    st.markdown(response)
                    render_last_chart()

                    st.session_state["messages"].append(
                        {"role": "assistant", "content": response}
                    )

                except Exception as e:
                    error_msg = f"Error inesperado: {str(e)}"
                    st.error(error_msg)
                    st.session_state["messages"].append(
                        {"role": "assistant", "content": error_msg}
                    )


if __name__ == "__main__":
    main()