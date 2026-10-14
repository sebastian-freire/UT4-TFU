"""Configuración del componente `admision`.

Igual que en la UT2, ningún valor operativo queda literal en el código: todos se
resuelven en tiempo de arranque desde el entorno, con un valor por defecto
conocido. Cambiar la capacidad de la cinta no requiere tocar código ni
reconstruir la imagen.
"""

import os

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql+psycopg://cinta:cinta@db:5432/cinta"
)

CINTA_ID = os.getenv("CINTA_ID", "C1")
CINTA_CAPACIDAD_KG = float(os.getenv("CINTA_CAPACIDAD_KG", 100))

# Ventana artificial que el endpoint de instrumentación sin transacción deja
# abierta entre la lectura y la escritura, para que la condición de carrera
# sea observable de forma reproducible en la demostración.
VENTANA_CARRERA_S = float(os.getenv("VENTANA_CARRERA_S", 0.15))

# UT4 - Plazo de los chequeos de /health/ready (PostgreSQL exige minimo 2 s).
TIMEOUT_SALUD_S = max(2, int(float(os.getenv("TIMEOUT_SALUD_S", 2))))

# ---------------------------------------------------------------------------
# UT4 - Gatekeeper (patron de seguridad).
#
# Clave interna que admision exige en el header X-Clave-Interna para aceptar
# solicitudes de admision. Asegura que nadie saltee al Gatekeeper (ingreso).
# ---------------------------------------------------------------------------
CLAVE_INTERNA = os.getenv("CLAVE_INTERNA", "cinta-interna-dev")

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://tfu:tfu@rabbitmq:5672/%2F")
COLA_NOMBRE = os.getenv("COLA_NOMBRE", "admision_paquetes")
COLA_INVALIDACIONES = os.getenv("COLA_INVALIDACIONES", "cinta_cache_invalidada")
COLA_PAUSA_PROCESAMIENTO_S = float(os.getenv("COLA_PAUSA_PROCESAMIENTO_S", 0))

# Consumo en paralelo de la cola: cada consumidor tiene su propia conexion
# AMQP y procesa un mensaje a la vez. Con uno solo, una rafaga de miles de
# mensajes tarda mas que el plazo de sondeo del generador y la demo falla
# aunque nada se haya perdido.
COLA_CONSUMIDORES = int(float(os.getenv("COLA_CONSUMIDORES", 8)))
COLA_PREFETCH = int(float(os.getenv("COLA_PREFETCH", 1)))

# Hilos para el trabajo bloqueante de base de datos via asyncio.to_thread.
HILOS_DB = int(float(os.getenv("HILOS_DB", 100)))

# Pileta de conexiones hacia PostgreSQL. El valor original (5 mas 10) se
# agotaba con rafagas de cientos de solicitudes concurrentes peleando por la
# misma fila de cinta; el sobrante ahora espera con plazo y recibe 503 en vez
# de quedarse sin respuesta.
DB_POOL_SIZE = int(float(os.getenv("DB_POOL_SIZE", 20)))
DB_MAX_OVERFLOW = int(float(os.getenv("DB_MAX_OVERFLOW", 30)))
DB_POOL_TIMEOUT_S = int(float(os.getenv("DB_POOL_TIMEOUT_S", 30)))

