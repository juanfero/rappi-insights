# 🟠 Rappi Insights — Sistema de Análisis Inteligente

## ¿Qué es?
Aplicación Streamlit para analizar métricas operacionales de Rappi desde dos frentes:
1. un bot conversacional que responde preguntas en lenguaje natural sobre la base de datos;
2. un sistema de insights automáticos que genera un reporte ejecutivo en Markdown.

## Componentes
1. **Bot Conversacional**
   - Preguntas en lenguaje natural
   - Text-to-SQL sobre SQLite read-only
   - Soporte para gráficos
   - Memoria conversacional

2. **Insights Automáticos**
   - Detección de anomalías
   - Tendencias preocupantes
   - Benchmarking
   - Correlaciones
   - Oportunidades
   - Reporte descargable en Markdown

## Setup Rápido

```bash
git clone <TU_REPO_URL>
cd rappi-insights
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edita .env y agrega tu GEMINI_API_KEY
export PYTHONPATH=.
streamlit run app.py