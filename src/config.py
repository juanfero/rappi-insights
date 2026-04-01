# src/config.py
# ──────────────────────────────────────────────────────────────
# Constantes globales y diccionario de métricas
# Fuente de verdad para nombres de columnas, rutas y parámetros
# ──────────────────────────────────────────────────────────────

# ── Rutas por defecto ─────────────────────────────────────────
DEFAULT_DB_PATH = "rappi.db"
DEFAULT_METRICS_CSV = "data/RAW_INPUT_METRICS.csv"
DEFAULT_ORDERS_CSV = "data/RAW_ORDERS.csv"

# ── Modelo y agente ───────────────────────────────────────────
DEFAULT_MODEL = "gemini-3-flash-preview"
MAX_AGENT_ITERATIONS = 5
MAX_CONVERSATION_HISTORY = 20  # pares de mensajes

# ── Columnas de semanas ───────────────────────────────────────
# Orden temporal: w8 = más antiguo, w0 = semana actual
WEEK_COLUMNS_METRICS_RAW = [f"L{i}W_ROLL" for i in range(8, -1, -1)]
WEEK_COLUMNS_ORDERS_RAW  = [f"L{i}W"      for i in range(8, -1, -1)]
WEEK_COLUMNS_CLEAN       = [f"w{i}"       for i in range(8, -1, -1)]

# ── Columnas requeridas en cada CSV (antes de renombrar) ──────
REQUIRED_COLUMNS_METRICS = [
    "COUNTRY", "CITY", "ZONE", "ZONE_TYPE",
    "ZONE_PRIORITIZATION", "METRIC",
    *WEEK_COLUMNS_METRICS_RAW,
]
REQUIRED_COLUMNS_ORDERS = [
    "COUNTRY", "CITY", "ZONE",
    *WEEK_COLUMNS_ORDERS_RAW,
]

# ── Diccionario de métricas ───────────────────────────────────
# Claves: nombres EXACTOS como aparecen en RAW_INPUT_METRICS.csv
METRICS_DICTIONARY = {
    "% PRO Users Who Breakeven": (
        "Usuarios Pro cuyo valor generado cubre el costo de su membresía "
        "/ Total usuarios Pro"
    ),
    "% Restaurants Sessions With Optimal Assortment": (
        "Sesiones con mínimo 40 restaurantes / Total sesiones"
    ),
    "Gross Profit UE": (
        "Margen bruto de ganancia / Total de órdenes "
        "(valor monetario, puede ser negativo)"
    ),
    "Lead Penetration": (
        "Tiendas habilitadas en Rappi / (Leads + Habilitadas + Salidas). "
        "Ratio, no porcentaje 0-1."
    ),
    "MLTV Top Verticals Adoption": (
        "Usuarios con órdenes en múltiples verticales / Total usuarios"
    ),
    "Non-Pro PTC > OP": (
        "Conversión de No Pro en Proceed to Checkout a Order Placed"
    ),
    "Perfect Orders": (
        "Órdenes sin cancelaciones, defectos ni demora / Total órdenes"
    ),
    "Pro Adoption (Last Week Status)": (
        "Usuarios suscripción Pro / Total usuarios Rappi"
    ),
    "Restaurants Markdowns / GMV": (
        "Descuentos restaurantes / GMV total restaurantes"
    ),
    "Restaurants SS > ATC CVR": (
        "Conversión en restaurantes: Select Store a Add to Cart"
    ),
    "Restaurants SST > SS CVR": (
        "Conversión: Seleccionar vertical Restaurantes a seleccionar tienda"
    ),
    "Retail SST > SS CVR": (
        "Conversión: Seleccionar vertical Retail/Supermercados "
        "a seleccionar tienda"
    ),
    "Turbo Adoption": (
        "Usuarios que compran en Turbo / Total usuarios con Turbo disponible "
        "(solo 278 zonas)"
    ),
}