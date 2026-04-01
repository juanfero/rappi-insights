# tests/test_agent.py

from __future__ import annotations

from types import SimpleNamespace

from src.db import Database
from src.prompts import build_system_prompt
from src.agent import run_agent


class FakeFunctionCall:
    def __init__(self, name, args):
        self.name = name
        self.args = args


class FakeContent:
    def __init__(self, role="model", parts=None):
        self.role = role
        self.parts = parts or []


class FakeCandidate:
    def __init__(self, content):
        self.content = content


class FakeResponse:
    def __init__(self, text=None, function_calls=None, content=None):
        self.text = text
        self.function_calls = function_calls or []
        self.candidates = [FakeCandidate(content or FakeContent())]


def test_system_prompt_contains_schema():
    db = Database()
    prompt = build_system_prompt(db)

    assert "metrics" in prompt
    assert "orders" in prompt
    assert "wow_change" in prompt
    assert "trend" in prompt
    assert "Perfect Orders" in prompt
    assert "Pro Adoption (Last Week Status)" in prompt


def test_agent_returns_data_based_response(monkeypatch):
    db = Database()

    calls = {"n": 0}

    def fake_generate_with_retry(client, model, contents, config):
        calls["n"] += 1

        # Primera llamada: pide usar run_sql
        if calls["n"] == 1:
            function_calls = [
                FakeFunctionCall(
                    "run_sql",
                    {"query": "SELECT COUNT(*) as total FROM metrics WHERE country = 'CO'"},
                )
            ]
            return FakeResponse(
                text=None,
                function_calls=function_calls,
                content=FakeContent(role="model", parts=[]),
            )

        # Segunda llamada: devuelve respuesta final
        return FakeResponse(
            text="Hay 125 zonas en Colombia.",
            function_calls=[],
            content=FakeContent(role="model", parts=[]),
        )

    monkeypatch.setattr("src.agent._get_client", lambda: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr("src.agent._generate_with_retry", fake_generate_with_retry)

    response = run_agent("¿Cuántas zonas hay en Colombia?", [], db)
    assert "125" in response


def test_agent_recovers_from_sql_error(monkeypatch):
    db = Database()

    calls = {"n": 0}

    def fake_generate_with_retry(client, model, contents, config):
        calls["n"] += 1

        # Primera llamada: pide una métrica inexistente
        if calls["n"] == 1:
            function_calls = [
                FakeFunctionCall(
                    "run_sql",
                    {"query": "SELECT * FROM metrics WHERE metric = 'Conversion Rate' LIMIT 10"},
                )
            ]
            return FakeResponse(
                text=None,
                function_calls=function_calls,
                content=FakeContent(role="model", parts=[]),
            )

        # Segunda llamada: devuelve respuesta manejando el error
        return FakeResponse(
            text="No encontré esa métrica en la base disponible.",
            function_calls=[],
            content=FakeContent(role="model", parts=[]),
        )

    monkeypatch.setattr("src.agent._get_client", lambda: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr("src.agent._generate_with_retry", fake_generate_with_retry)

    response = run_agent("Muestra datos de la métrica 'Conversion Rate'", [], db)
    assert "no encontr" in response.lower() or "métrica" in response.lower()


def test_agent_handles_empty_user_message():
    db = Database()
    response = run_agent("   ", [], db)
    assert "escribe una pregunta" in response.lower()