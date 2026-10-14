import os

from dotenv import load_dotenv

load_dotenv()


def _limite(nombre_var: str, valor_default: float) -> float:
    """Lee una variable de entorno numérica, con fallback a un default."""
    return float(os.getenv(nombre_var, valor_default))


LIMITES = {
    "normal": {
        "peso_kg_max": _limite("LIMITE_NORMAL_PESO_KG_MAX", 25),
        "alto_cm_max": _limite("LIMITE_NORMAL_ALTO_CM_MAX", 60),
        "ancho_cm_max": _limite("LIMITE_NORMAL_ANCHO_CM_MAX", 60),
        "largo_cm_max": _limite("LIMITE_NORMAL_LARGO_CM_MAX", 80),
    },
    "fragil": {
        "peso_kg_max": _limite("LIMITE_FRAGIL_PESO_KG_MAX", 8),
        "alto_cm_max": _limite("LIMITE_FRAGIL_ALTO_CM_MAX", 40),
        "ancho_cm_max": _limite("LIMITE_FRAGIL_ANCHO_CM_MAX", 40),
        "largo_cm_max": _limite("LIMITE_FRAGIL_LARGO_CM_MAX", 50),
    },
}

# ---------------------------------------------------------------------------
# UT3: configuración del componente de admisión que este servicio consume.
#
# Misma táctica de binding en tiempo de configuración que se usó en la UT2 para
# los límites de paquete: ningún valor queda literal en el código.
# ---------------------------------------------------------------------------

ADMISION_URL = os.getenv("ADMISION_URL", "http://admision:8000")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://tfu:tfu@rabbitmq:5672/%2F")
COLA_NOMBRE = os.getenv("COLA_NOMBRE", "admision_paquetes")
COLA_INVALIDACIONES = os.getenv("COLA_INVALIDACIONES", "cinta_cache_invalidada")
CINTA_POR_DEFECTO = os.getenv("CINTA_ID", "C1")
TIMEOUT_ADMISION_S = float(os.getenv("TIMEOUT_ADMISION_S", 10))

# Hilos para el trabajo bloqueante (publicacion AMQP via asyncio.to_thread).
# Acota la concurrencia real contra RabbitMQ: el sobrante espera en el bucle
# de eventos en vez de abrir cientos de conexiones a la vez.
HILOS_MAX = int(float(os.getenv("HILOS_MAX", 64)))

# UT4 - Plazo de los chequeos de /health/ready.
TIMEOUT_SALUD_S = float(os.getenv("TIMEOUT_SALUD_S", 2))

# ---------------------------------------------------------------------------
# UT4 - Cache-Aside (patron de rendimiento).
#
# Cache en memoria para consultas GET /cintas/{cinta_id}. Reduce latencia y
# evita consultas innecesarias hacia admision y la base de datos.
# ---------------------------------------------------------------------------
CACHE_HABILITADA = os.getenv("CACHE_HABILITADA", "true").lower() in ("1", "true", "yes")
CACHE_TTL_S = float(os.getenv("CACHE_TTL_S", 5.0))

# ---------------------------------------------------------------------------
# UT4 - Gatekeeper (patron de seguridad).
#
# Clave interna que ingreso (Gatekeeper) adjunta en sus solicitudes hacia admision.
# ---------------------------------------------------------------------------
CLAVE_INTERNA = os.getenv("CLAVE_INTERNA", "cinta-interna-dev")

# ---------------------------------------------------------------------------
# UT4 - Gateway Offloading (patron de seguridad).
#
# Autenticacion de clientes externos descargada en la frontera (ingreso).
# ---------------------------------------------------------------------------
_claves_raw = os.getenv("API_KEYS_VALIDAS", "cliente-alpha,cliente-beta,cliente-operador")
API_KEYS_VALIDAS = {k.strip() for k in _claves_raw.split(",") if k.strip()}
ENFORCE_API_KEY = os.getenv("ENFORCE_API_KEY", "false").lower() in ("1", "true", "yes")

