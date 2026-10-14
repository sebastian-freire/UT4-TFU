"""
Componente admision. Expone la interfaz IAdmisionCinta.

Administra el cupo de carga de la cinta, que es el recurso compartido y finito
del sistema. Es el unico componente que escribe estado, y siempre lo hace dentro
de una transaccion.

El servicio en si no guarda nada en memoria: todo el estado esta en PostgreSQL.
Por eso tambien se puede replicar, ya que dos replicas que compiten por la misma
cinta se serializan en la base y no en el proceso.

Los endpoints son asincronos y el trabajo de base (bloqueante con el driver
sincrono) se deriva a un ejecutor acotado. Todas las contendientes por la misma
fila de cinta se serializan igual que antes en SELECT ... FOR UPDATE; la
diferencia es que el bucle de eventos sigue respondiendo (salud, lecturas) y
una espera agotada devuelve 503 reintentable en vez de dejar caer la conexion.
"""

import asyncio
import secrets
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from decimal import Decimal

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError, IntegrityError

from app.config import CLAVE_INTERNA, HILOS_DB
from app.db import INSTANCIA, Sesion, repo, sembrar_cinta, vista_cinta
from app.demo import router as router_demo
from app.modelos import PaqueteEnTransito, ResultadoSolicitudCola
from app.salud import router as router_salud
from app.salud import vivo
from app.servicios import ejecutar_admision


def _verificar_gatekeeper(clave: str | None):
    """
    UT4 - Gatekeeper (patron de seguridad).

    Admision solo acepta solicitudes de admision que provengan del Gatekeeper
    (componente ingreso). Exige la cabecera X-Clave-Interna coincidente.
    Si falta o es invalida, rechaza con HTTP 403.
    """
    if clave is None or not secrets.compare_digest(clave, CLAVE_INTERNA):
        raise HTTPException(
            status_code=403,
            detail={
                "error": "acceso denegado",
                "motivo": "las admisiones requieren pasar por el Gatekeeper (componente ingreso)",
                "patron": "Gatekeeper",
            },
        )



@asynccontextmanager
async def ciclo_de_vida(_):
    bucle = asyncio.get_running_loop()
    bucle.set_default_executor(ThreadPoolExecutor(
        max_workers=HILOS_DB, thread_name_prefix="admision-bloqueante"))
    # La base ya esta sana por el depends_on del compose, pero puede tardar unos
    # segundos mas en aceptar conexiones.
    for intento in range(10):
        try:
            await asyncio.to_thread(sembrar_cinta)
            break
        except Exception:
            if intento == 9:
                raise
            await asyncio.sleep(2)
    yield


app = FastAPI(
    title="Componente admision: cupo de cinta transportadora",
    description="Interfaz IAdmisionCinta. Consume IRepositorioCinta.",
    version="1.0.0",
    lifespan=ciclo_de_vida,
)


class SolicitudAdmision(BaseModel):
    tipo_paquete: str
    peso_kg: float = Field(gt=0)


class SolicitudEgreso(BaseModel):
    paquete_id: uuid.UUID


# UT4 - Health Endpoint Monitoring.
app.include_router(router_salud)


@app.get("/health")
def health():
    """Compatibilidad con la UT3. Equivale a /health/live."""
    return vivo()


@app.get("/")
def raiz():
    return health()


def _consultar_en_hilo(cinta_id: str):
    with Sesion() as sesion:
        cinta = repo.obtener(sesion, cinta_id)
        if cinta is None:
            raise HTTPException(404, f"cinta {cinta_id} no existe")

        cantidad = sesion.scalar(
            select(func.count()).select_from(PaqueteEnTransito)
            .where(PaqueteEnTransito.cinta_id == cinta_id))
        vista = vista_cinta(cinta)

    vista["paquetes_en_transito"] = int(cantidad or 0)
    vista["instancia"] = INSTANCIA
    return vista


@app.get("/cintas/{cinta_id}")
async def consultar(cinta_id: str):
    """Estado actual de la cinta. Lectura sin bloqueo."""
    try:
        return await asyncio.to_thread(_consultar_en_hilo, cinta_id)
    except HTTPException:
        raise
    except DBAPIError as error:
        raise HTTPException(503, f"base no disponible, reintente: {error}")


def _admitir_en_hilo(cinta_id: str, tipo_paquete: str, peso: Decimal):
    with Sesion() as sesion:
        try:
            with sesion.begin():
                respuesta = ejecutar_admision(
                    sesion, cinta_id, tipo_paquete, peso)
        except IntegrityError as error:
            # Red de seguridad: si por algun camino se intentara confirmar una
            # carga fuera de rango, la restriccion CHECK aborta la transaccion.
            raise HTTPException(409, {
                "motivo": "la restriccion cupo_no_excedido rechazo la escritura",
                "error": str(error.orig),
            })
    return respuesta


@app.post("/cintas/{cinta_id}/admisiones")
async def admitir(
    cinta_id: str,
    solicitud: SolicitudAdmision,
    x_clave_interna: str | None = Header(default=None),
):
    """
    Reserva cupo para un paquete. Esta es la transaccion ACID del sistema:

        BEGIN
          SELECT ... FOR UPDATE   -> bloquea la fila de la cinta
          verificar cupo
          INSERT paquete_en_transito
          UPDATE cinta.carga_kg
        COMMIT

    Gatekeeper (seguridad): exige la clave interna provista por ingreso.
    Cualquier cliente directo sin la clave recibe 403 Forbidden.
    """
    _verificar_gatekeeper(x_clave_interna)
    peso = Decimal(str(solicitud.peso_kg))

    try:
        return await asyncio.to_thread(
            _admitir_en_hilo, cinta_id, solicitud.tipo_paquete, peso)
    except HTTPException:
        raise
    except DBAPIError as error:
        raise HTTPException(503, {
            "motivo": "base de datos saturada o no disponible, reintente",
            "error": str(error).strip().splitlines()[0] if str(error) else "?",
        })


def _consultar_solicitud_en_hilo(solicitud_id: uuid.UUID):
    with Sesion() as sesion:
        resultado = sesion.get(ResultadoSolicitudCola, solicitud_id)
    if resultado is None:
        return None
    return {
        "solicitud_id": str(solicitud_id),
        "estado": "admitida" if resultado.codigo_http == 200 else "rechazada",
        "codigo_resultado": resultado.codigo_http,
        "resultado": resultado.respuesta,
        "procesada_en": resultado.procesada_en.isoformat(),
    }


@app.get("/solicitudes/{solicitud_id}")
async def consultar_solicitud(solicitud_id: uuid.UUID,
                              x_clave_interna: str | None = Header(default=None)):
    """Consulta el resultado final de una solicitud recibida por la cola."""
    _verificar_gatekeeper(x_clave_interna)
    try:
        resultado = await asyncio.to_thread(
            _consultar_solicitud_en_hilo, solicitud_id)
    except DBAPIError as error:
        raise HTTPException(503, f"base no disponible, reintente: {error}")
    if resultado is None:
        return JSONResponse(status_code=202, content={
            "solicitud_id": str(solicitud_id), "estado": "pendiente"})
    return resultado


def _egresar_en_hilo(cinta_id: str, paquete_id: uuid.UUID):
    with Sesion() as sesion:
        with sesion.begin():
            cinta = repo.obtener_para_actualizar(sesion, cinta_id)
            if cinta is None:
                raise HTTPException(404, f"cinta {cinta_id} no existe")

            peso = repo.registrar_egreso(sesion, cinta, paquete_id)
            if peso is None:
                raise HTTPException(
                    404, f"el paquete {paquete_id} no esta en la cinta")
            vista = vista_cinta(cinta)

    return {"estado": "egresado", "peso_liberado_kg": float(peso),
            "cinta": vista, "instancia": INSTANCIA}


@app.post("/cintas/{cinta_id}/egresos")
async def egresar(cinta_id: str, solicitud: SolicitudEgreso):
    """Un paquete sale de la cinta y libera su peso. Tambien es transaccional."""
    try:
        return await asyncio.to_thread(
            _egresar_en_hilo, cinta_id, solicitud.paquete_id)
    except HTTPException:
        raise
    except DBAPIError as error:
        raise HTTPException(503, f"base no disponible, reintente: {error}")


# Endpoints de las demostraciones. No son parte de IAdmisionCinta.
app.include_router(router_demo)
