"""
Componente ingreso. Expone la interfaz IIngresoPaquetes.

Valida el paquete con las reglas de su tipo y, si pasa, le pide cupo al
componente admision. No guarda nada entre solicitudes, por eso se puede replicar.

Los endpoints son asincronos: las llamadas hacia admision usan un cliente HTTP
con conexiones persistentes y la publicacion AMQP (bloqueante por naturaleza
en pika) se deriva a un ejecutor acotado con asyncio.to_thread. Asi el bucle
de eventos sigue aceptando solicitudes aunque cientos de hilos esperen al
broker o a la base, y la sobrecarga se traduce en mas latencia en vez de
conexiones caidas.
"""

import asyncio
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app import cliente_admision
from app.cache import cache_cintas
from app.cola import iniciar_listener_cache, publicar
from app.config import (CACHE_HABILITADA, CACHE_TTL_S, CINTA_POR_DEFECTO,
                        HILOS_MAX)
from app.reglas.registro import obtener_regla
from app.salud import INSTANCIA, vivo
from app.salud import router as router_salud
from app.seguridad import autenticar_cliente

LOG = logging.getLogger("ingreso")


@asynccontextmanager
async def ciclo_de_vida(_):
    bucle = asyncio.get_running_loop()
    bucle.set_default_executor(ThreadPoolExecutor(
        max_workers=HILOS_MAX, thread_name_prefix="ingreso-bloqueante"))
    iniciar_listener_cache(cache_cintas)
    yield
    await cliente_admision.cerrar()


app = FastAPI(
    title="Componente ingreso - API Cinta Transportadora",
    description="Interfaz IIngresoPaquetes. Consume IAdmisionCinta.",
    version="2.0.0",
    lifespan=ciclo_de_vida,
)


class Paquete(BaseModel):
    tipo_paquete: str
    peso_kg: float
    alto_cm: float
    ancho_cm: float
    largo_cm: float
    cinta_id: str = CINTA_POR_DEFECTO


# UT4 - Health Endpoint Monitoring.
app.include_router(router_salud)


@app.get("/health")
def health():
    """Compatibilidad con la UT3. Equivale a /health/live."""
    return vivo()


@app.get("/")
def raiz():
    return health()


@app.get("/cache/metricas")
def metricas_cache():
    """Metricas de rendimiento del patron Cache-Aside."""
    return cache_cintas.metricas()


@app.post("/cintas/{cinta_id}/cache/invalidar")
def invalidar_cache_cinta(cinta_id: str):
    """Invalida explicitamente la cache de una cinta (Cache-Aside)."""
    invalidada = cache_cintas.invalidar(f"cinta:{cinta_id}")
    return {"cinta_id": cinta_id, "invalidada": invalidada}


@app.get("/cintas/{cinta_id}")
async def consultar_cinta(cinta_id: str):
    """
    Estado actual de la cinta.
    Aplica el patron Cache-Aside (rendimiento):
    - Si el dato esta en cache (HIT), se devuelve sin consultar a admision.
    - Si no esta o expiro (MISS), se consulta a admision y se guarda en cache.
    """
    clave = f"cinta:{cinta_id}"
    if CACHE_HABILITADA:
        datos, hit = cache_cintas.obtener(clave)
        if hit:
            return JSONResponse(
                content={"cinta": datos, "instancia": {"ingreso": INSTANCIA, "cache": "hit"}},
                headers={"X-Cache": "HIT"},
            )

    codigo, cuerpo = await cliente_admision.estado_cinta(cinta_id)
    if codigo != 200:
        raise HTTPException(codigo, cuerpo)

    if CACHE_HABILITADA:
        cache_cintas.guardar(clave, cuerpo, CACHE_TTL_S)

    return JSONResponse(
        content={"cinta": cuerpo, "instancia": {"ingreso": INSTANCIA, "cache": "miss"}},
        headers={"X-Cache": "MISS"},
    )


@app.post("/paquetes")
async def recibir_paquete(paquete: Paquete, cliente_autenticado: str = Depends(autenticar_cliente)):
    """
    Ingreso de un paquete. Aplica:
      - Gateway Offloading (seguridad): autenticacion del cliente en la frontera.
      - Gatekeeper (seguridad): validacion exhaustiva de reglas de dominio antes
        de reenviar a admision con credencial interna.
      - Cache-Aside (rendimiento): invalidacion de cache tras admision exitosa.
    """
    # Control 1: reglas del tipo de paquete (codigo de la UT2, sin cambios).
    regla = obtener_regla(paquete.tipo_paquete)
    if regla is None:
        raise HTTPException(
            422, f"tipo_paquete '{paquete.tipo_paquete}' no está registrado")

    es_valido, motivo = regla.es_valido(paquete.model_dump())
    if not es_valido:
        raise HTTPException(422, motivo)

    # Control 2: cupo de la cinta (interfaz IAdmisionCinta).
    codigo, cuerpo = await cliente_admision.admitir(
        paquete.cinta_id, paquete.tipo_paquete, paquete.peso_kg)
    if codigo != 200:
        raise HTTPException(codigo, cuerpo)

    # Cache-Aside: al admitir un paquete, se invalida la cache de la cinta afectada
    # para garantizar consistencia en la proxima lectura.
    if CACHE_HABILITADA:
        cache_cintas.invalidar(f"cinta:{paquete.cinta_id}")

    return {
        "estado": "aceptado",
        "mensaje": "Paquete admitido en la cinta",
        "paquete_id": cuerpo["paquete_id"],
        "cinta": cuerpo["cinta"],
        "cliente": cliente_autenticado,
        "instancia": {"ingreso": INSTANCIA, "admision": cuerpo["instancia"]},
    }


@app.post("/paquetes/cola", status_code=202)
async def recibir_paquete_en_cola(
    paquete: Paquete,
    cliente_autenticado: str = Depends(autenticar_cliente),
):
    """Valida y encola el paquete; el cupo se confirma asincronicamente.

    La publicacion corre fuera del bucle de eventos para no bloquear la
    aceptacion de otras solicitudes. Si el broker no esta disponible se
    responde 503 (reintentable) en vez de dejar caer la conexion.
    """
    regla = obtener_regla(paquete.tipo_paquete)
    if regla is None:
        raise HTTPException(
            422, f"tipo_paquete '{paquete.tipo_paquete}' no esta registrado")
    es_valido, motivo = regla.es_valido(paquete.model_dump())
    if not es_valido:
        raise HTTPException(422, motivo)

    solicitud_id = uuid.uuid4()
    try:
        await asyncio.to_thread(publicar, {
            "solicitud_id": str(solicitud_id),
            "cinta_id": paquete.cinta_id,
            "tipo_paquete": paquete.tipo_paquete,
            "peso_kg": paquete.peso_kg,
        })
    except Exception as error:
        LOG.warning("publicacion en cola fallida: %s", error)
        raise HTTPException(
            503, f"cola no disponible, reintente: {error}")

    return JSONResponse(status_code=202, content={
        "solicitud_id": str(solicitud_id),
        "estado": "pendiente",
        "cliente": cliente_autenticado,
    })


@app.get("/solicitudes/{solicitud_id}")
async def consultar_solicitud(
    solicitud_id: uuid.UUID,
    _: str = Depends(autenticar_cliente),
):
    """Devuelve 202 mientras el trabajador procesa, o el resultado terminal."""
    codigo, cuerpo = await cliente_admision.estado_solicitud(solicitud_id)
    if codigo == 503:
        raise HTTPException(codigo, cuerpo)
    return JSONResponse(status_code=codigo, content=cuerpo)
