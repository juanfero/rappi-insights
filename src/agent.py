# src/agent.py

from __future__ import annotations

import os
import time
from typing import Any, Dict, List

from dotenv import load_dotenv
from google import genai
from google.genai import types

from src.config import DEFAULT_MODEL, MAX_AGENT_ITERATIONS
from src.prompts import build_system_prompt
from src.tools import execute_tool


load_dotenv()


def _get_client() -> genai.Client:
    """
    Crea un cliente de Gemini.
    El SDK toma GEMINI_API_KEY o GOOGLE_API_KEY desde variables de entorno.
    """
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "No se encontró GEMINI_API_KEY ni GOOGLE_API_KEY en el entorno."
        )
    return genai.Client(api_key=api_key)


def _build_tools() -> List[types.Tool]:
    """
    Declara las tools disponibles para el modelo.
    """
    run_sql_decl = types.FunctionDeclaration(
        name="run_sql",
        description=(
            "Ejecuta una consulta SQL SELECT o WITH contra la base de datos "
            "operacional de Rappi en modo read-only. "
            "Devuelve resultados como tabla markdown o un error legible."
        ),
        parameters_json_schema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Query SQL SELECT o WITH a ejecutar",
                },
                "description": {
                    "type": "string",
                    "description": "Breve descripción de lo que busca el query",
                },
            },
            "required": ["query"],
        },
    )

    plot_chart_decl = types.FunctionDeclaration(
        name="plot_chart",
        description=(
            "Genera un gráfico usando datos obtenidos por SQL. "
            "Ejecuta el query y guarda la figura en la sesión de Streamlit."
        ),
        parameters_json_schema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Query SQL para obtener los datos del gráfico",
                },
                "chart_type": {
                    "type": "string",
                    "enum": ["line", "bar", "scatter", "heatmap"],
                    "description": "Tipo de gráfico a generar",
                },
                "title": {
                    "type": "string",
                    "description": "Título del gráfico",
                },
                "x": {
                    "type": "string",
                    "description": "Nombre de la columna para eje X",
                },
                "y": {
                    "type": "string",
                    "description": "Nombre de la columna para eje Y",
                },
                "color": {
                    "type": "string",
                    "description": "Columna opcional para agrupar por color",
                },
            },
            "required": ["query", "chart_type", "title", "x", "y"],
        },
    )

    return [types.Tool(function_declarations=[run_sql_decl, plot_chart_decl])]


def _history_to_contents(
    conversation_history: List[Dict[str, str]],
    user_message: str,
) -> List[types.Content]:
    """
    Convierte el historial simple del chat al formato Content del SDK de Gemini.
    Solo maneja mensajes finales user/assistant.
    """
    contents: List[types.Content] = []

    for message in conversation_history:
        role = message.get("role", "").strip().lower()
        text = message.get("content", "")

        if not text:
            continue

        if role == "user":
            contents.append(
                types.Content(
                    role="user",
                    parts=[types.Part.from_text(text=text)],
                )
            )
        elif role == "assistant":
            # Para Gemini, el rol del modelo en historial es "model"
            contents.append(
                types.Content(
                    role="model",
                    parts=[types.Part.from_text(text=text)],
                )
            )

    contents.append(
        types.Content(
            role="user",
            parts=[types.Part.from_text(text=user_message)],
        )
    )

    return contents


def extract_text(response) -> str:
    """
    Extrae texto de la respuesta del modelo.
    """
    if getattr(response, "text", None):
        return response.text.strip()

    parts = []
    try:
        for candidate in getattr(response, "candidates", []) or []:
            content = getattr(candidate, "content", None)
            if not content:
                continue
            for part in getattr(content, "parts", []) or []:
                text = getattr(part, "text", None)
                if text:
                    parts.append(text)
    except Exception:
        pass

    return "\n".join(parts).strip()


def _make_config(system_prompt: str) -> types.GenerateContentConfig:
    """
    Config del modelo:
    - system instruction
    - tools habilitadas
    - function calling en modo AUTO
    """
    return types.GenerateContentConfig(
        system_instruction=system_prompt,
        tools=_build_tools(),
        automatic_function_calling=types.AutomaticFunctionCallingConfig(
            disable=True
        ),
        tool_config=types.ToolConfig(
            function_calling_config=types.FunctionCallingConfig(
                mode="AUTO"
            )
        ),
    )


def _generate_with_retry(
    client: genai.Client,
    model: str,
    contents: List[types.Content],
    config: types.GenerateContentConfig,
):
    """
    Reintenta en errores transitorios.
    """
    delays = [1, 2, 4]

    last_error: Exception | None = None
    for attempt, delay in enumerate([0] + delays):
        if delay:
            time.sleep(delay)

        try:
            return client.models.generate_content(
                model=model,
                contents=contents,
                config=config,
            )
        except Exception as e:
            last_error = e
            message = str(e).lower()

            # auth / key issues
            if "api key" in message or "authentication" in message or "unauthorized" in message:
                raise RuntimeError(
                    "API key inválida o no configurada correctamente para Gemini."
                ) from e

            # retry solo si parece transitorio
            transient_markers = [
                "rate",
                "quota",
                "timeout",
                "tempor",
                "unavailable",
                "deadline",
                "429",
                "500",
                "503",
            ]
            if not any(marker in message for marker in transient_markers):
                raise

            if attempt == len(delays):
                break

    raise RuntimeError(f"No se pudo completar la llamada al modelo: {last_error}")


def run_agent(user_message: str, conversation_history: list, db) -> str:
    """
    Orquestador principal del bot.

    Flujo:
    1. Arma historial + user message
    2. Llama al modelo con tools
    3. Si hay function calls, ejecuta tools
    4. Devuelve function responses al modelo
    5. Itera hasta MAX_AGENT_ITERATIONS
    6. Retorna respuesta final
    """
    if not user_message or not user_message.strip():
        return "Por favor escribe una pregunta para poder ayudarte."

    client = _get_client()
    system_prompt = build_system_prompt(db)
    config = _make_config(system_prompt)

    contents = _history_to_contents(conversation_history, user_message)
    model_name = os.getenv("RAPPI_MODEL", DEFAULT_MODEL)

    try:
        for _ in range(MAX_AGENT_ITERATIONS):
            response = _generate_with_retry(
                client=client,
                model=model_name,
                contents=contents,
                config=config,
            )

            function_calls = getattr(response, "function_calls", None) or []

            # Si no pidió tools, devolvemos respuesta final
            if not function_calls:
                final_text = extract_text(response)
                if final_text:
                    return final_text
                return "No pude generar una respuesta. Intenta reformular tu pregunta."

            # Guardamos el contenido completo del modelo, incluyendo function call
            model_content = response.candidates[0].content
            contents.append(model_content)

            # Ejecutamos todas las tools pedidas
            tool_response_parts = []
            for function_call in function_calls:
                tool_name = function_call.name
                tool_args = dict(function_call.args or {})

                tool_result = execute_tool(tool_name, tool_args, db)

                tool_response_parts.append(
                    types.Part.from_function_response(
                        name=tool_name,
                        response={"result": tool_result},
                    )
                )

            contents.append(
                types.Content(
                    role="tool",
                    parts=tool_response_parts,
                )
            )

        return (
            "No pude completar el análisis en el número máximo de iteraciones. "
            "Intenta reformular tu pregunta."
        )

    finally:
        try:
            client.close()
        except Exception:
            pass
