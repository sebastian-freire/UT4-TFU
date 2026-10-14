"""Consumidor durable de solicitudes de admision."""

import json
import logging
import secrets
import threading
import time
import uuid
from decimal import Decimal

import pika
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.config import (CLAVE_INTERNA, COLA_CONSUMIDORES, COLA_INVALIDACIONES,
                        COLA_NOMBRE, COLA_PAUSA_PROCESAMIENTO_S, COLA_PREFETCH,
                        RABBITMQ_URL)
from app.db import Sesion
from app.modelos import ResultadoSolicitudCola
from app.servicios import ejecutar_admision

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("cola_worker")


def _registrar_error_integridad(solicitud_id: uuid.UUID, error: IntegrityError):
    """Guarda el rechazo de la restriccion tras revertir la transaccion fallida."""
    with Sesion.begin() as sesion:
        sesion.execute(
            text("SELECT pg_advisory_xact_lock(:clave)"),
            {"clave": solicitud_id.int & ((1 << 63) - 1)},
        )
        if sesion.get(ResultadoSolicitudCola, solicitud_id) is None:
            sesion.add(ResultadoSolicitudCola(
                id=solicitud_id,
                codigo_http=409,
                respuesta={
                    "detail": {
                        "motivo": "la restriccion cupo_no_excedido rechazo la escritura",
                        "error": str(error.orig),
                    }
                },
            ))


def procesar(ch, metodo, propiedades, cuerpo):
    try:
        solicitud = json.loads(cuerpo)
        solicitud_id = uuid.UUID(solicitud["solicitud_id"])
        clave = (propiedades.headers or {}).get("clave_interna", "")
        if not secrets.compare_digest(str(clave), CLAVE_INTERNA):
            LOG.error("Solicitud %s rechazada: credencial interna invalida", solicitud_id)
            ch.basic_nack(delivery_tag=metodo.delivery_tag, requeue=False)
            return
        cinta_id = str(solicitud["cinta_id"])
        tipo_paquete = str(solicitud["tipo_paquete"])
        peso = Decimal(str(solicitud["peso_kg"]))
        if COLA_PAUSA_PROCESAMIENTO_S > 0:
            time.sleep(COLA_PAUSA_PROCESAMIENTO_S)

        with Sesion() as sesion:
            with sesion.begin():
                sesion.execute(
                    text("SELECT pg_advisory_xact_lock(:clave)"),
                    {"clave": solicitud_id.int & ((1 << 63) - 1)},
                )
                if sesion.get(ResultadoSolicitudCola, solicitud_id) is not None:
                    ch.basic_ack(delivery_tag=metodo.delivery_tag)
                    return

                try:
                    respuesta = ejecutar_admision(
                        sesion, cinta_id, tipo_paquete, peso)
                    codigo = 200
                except HTTPException as error:
                    respuesta = {"detail": error.detail}
                    codigo = error.status_code

                if codigo == 200:
                    ch.basic_publish(
                        exchange=COLA_INVALIDACIONES,
                        routing_key="",
                        body=json.dumps({"cinta_id": cinta_id}),
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
                sesion.add(ResultadoSolicitudCola(
                    id=solicitud_id, codigo_http=codigo, respuesta=respuesta))

        ch.basic_ack(delivery_tag=metodo.delivery_tag)
        LOG.info("Solicitud %s procesada con HTTP %s", solicitud_id, codigo)
    except IntegrityError as error:
        solicitud_id = uuid.UUID(json.loads(cuerpo)["solicitud_id"])
        _registrar_error_integridad(solicitud_id, error)
        ch.basic_ack(delivery_tag=metodo.delivery_tag)
        LOG.exception("Solicitud %s rechazada por una restriccion de base", solicitud_id)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        LOG.exception("Mensaje invalido enviado a la cola de mensajes fallidos")
        ch.basic_nack(delivery_tag=metodo.delivery_tag, requeue=False)
    except Exception:
        LOG.exception("Fallo transitorio procesando mensaje; se reencolara")
        time.sleep(1)
        ch.basic_nack(delivery_tag=metodo.delivery_tag, requeue=True)


def _consumir_un_hilo():
    """Bucle de consumo de un hilo: conexion y canal propios.

    pika no permite compartir canales entre hilos, asi que cada consumidor
    abre los suyos. Los mensajes se siguen confirmando uno por uno tras el
    commit en base, igual que antes.
    """
    while True:
        conexion = None
        try:
            parametros = pika.URLParameters(RABBITMQ_URL)
            parametros.heartbeat = 30
            parametros.blocked_connection_timeout = 10
            conexion = pika.BlockingConnection(parametros)
            canal = conexion.channel()
            canal.confirm_delivery()
            canal.exchange_declare(exchange=COLA_INVALIDACIONES, exchange_type="fanout", durable=True)
            cola_fallidos = f"{COLA_NOMBRE}.fallidos"
            canal.queue_declare(queue=cola_fallidos, durable=True)
            canal.queue_declare(
                queue=COLA_NOMBRE,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": cola_fallidos,
                },
            )
            canal.basic_qos(prefetch_count=COLA_PREFETCH)
            canal.basic_consume(queue=COLA_NOMBRE, on_message_callback=procesar)
            LOG.info("Consumidor activo: cola=%s hilo=%s",
                     COLA_NOMBRE, threading.current_thread().name)
            canal.start_consuming()
        except Exception:
            LOG.exception("Consumidor desconectado; reintentando")
            time.sleep(2)
        finally:
            if conexion is not None and conexion.is_open:
                conexion.close()


def ejecutar():
    cantidad = max(1, COLA_CONSUMIDORES)
    if cantidad == 1:
        _consumir_un_hilo()
        return
    LOG.info("Iniciando %d consumidores en paralelo", cantidad)
    hilos = [
        threading.Thread(target=_consumir_un_hilo,
                         name=f"consumidor-{i}", daemon=True)
        for i in range(cantidad)
    ]
    for hilo in hilos:
        hilo.start()
    for hilo in hilos:
        hilo.join()


if __name__ == "__main__":
    ejecutar()