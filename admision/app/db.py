from app.modelos import Base, Cinta, CintaEventual, ResultadoSolicitudCola
"""Conexion a la base y objetos que comparten main.py y demo.py."""

import socket
from decimal import Decimal

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.config import (CINTA_CAPACIDAD_KG, CINTA_ID, DATABASE_URL,
                        DB_MAX_OVERFLOW, DB_POOL_SIZE, DB_POOL_TIMEOUT_S)
from app.repositorio import RepositorioCintaSQL

INSTANCIA = socket.gethostname()

motor = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=DB_POOL_SIZE,
    max_overflow=DB_MAX_OVERFLOW,
    pool_timeout=DB_POOL_TIMEOUT_S,
    connect_args={
        "connect_timeout": 5,
        # Cota para cada sentencia, incluida la espera del bloqueo de fila.
        # Convierte una espera eterna en un error 503 reintentable.
        "options": "-c statement_timeout=20000",
    },
)
Sesion = sessionmaker(bind=motor, expire_on_commit=False)
repo = RepositorioCintaSQL()


def sembrar_cinta():
    """
    Registra la cinta configurada si todavia no existe.

    Las tablas ya las crea db/init.sql cuando arranca PostgreSQL. El ON CONFLICT
    evita que varias replicas arrancando a la vez se pisen entre si.
    """
    capacidad = Decimal(str(CINTA_CAPACIDAD_KG))
    Base.metadata.create_all(bind=motor, tables=[ResultadoSolicitudCola.__table__])
    with motor.begin() as conexion:
        for tabla in ("cinta", "cinta_eventual"):
            conexion.execute(
                text(f"INSERT INTO {tabla} (id, capacidad_kg) VALUES (:id, :cap) "
                     "ON CONFLICT (id) DO NOTHING"),
                {"id": CINTA_ID, "cap": capacidad},
            )


def vista_cinta(cinta):
    """Representacion de una cinta en la interfaz REST."""
    return {
        "id": cinta.id,
        "capacidad_kg": float(cinta.capacidad_kg),
        "carga_kg": float(cinta.carga_kg),
        "disponible_kg": float(cinta.capacidad_kg - cinta.carga_kg),
    }
