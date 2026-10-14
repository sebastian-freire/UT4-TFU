"""
Cliente de la interfaz IAdmisionCinta.

Es el unico punto donde el componente ingreso consume el componente admision.
Devuelve siempre (codigo, cuerpo) para que main.py decida que responder.

Usa un unico httpx.AsyncClient con conexiones persistentes (keep-alive). La
version anterior abria una conexion nueva con httpx.request por cada llamada
y corria en endpoints sincronos, asi que con cientos de solicitudes
concurrentes se agotaban los hilos del servidor antes que la cinta.
"""

import threading

import httpx

from app.config import ADMISION_URL, CLAVE_INTERNA, TIMEOUT_ADMISION_S

_cliente = None
_candado = threading.Lock()


def _cliente_async():
    global _cliente
    if _cliente is None:
        with _candado:
            if _cliente is None:
                _cliente = httpx.AsyncClient(
                    timeout=TIMEOUT_ADMISION_S,
                    limits=httpx.Limits(
                        max_connections=200,
                        max_keepalive_connections=100,
                    ),
                )
    return _cliente


async def cerrar():
    """Cierra el cliente compartido al apagar el servicio."""
    global _cliente
    cliente, _cliente = _cliente, None
    if cliente is not None:
        await cliente.aclose()


async def _pedir(metodo, ruta, json=None):
    cabeceras = {"X-Clave-Interna": CLAVE_INTERNA}
    try:
        r = await _cliente_async().request(
            metodo, ADMISION_URL + ruta, json=json, headers=cabeceras)
    except httpx.HTTPError as error:
        # Si no se puede confirmar el cupo, no se admite: ante la duda la cinta
        # no avanza.
        return 503, f"no se pudo contactar al componente de admision: {error}"

    try:
        cuerpo = r.json()
    except Exception:
        # Puertas intermedias (nginx) o caidas pueden devolver texto plano.
        return r.status_code, (r.text or "")[:500]
    if r.status_code != 200:
        cuerpo = cuerpo.get("detail", cuerpo) if isinstance(cuerpo, dict) else cuerpo
    return r.status_code, cuerpo


async def admitir(cinta_id, tipo_paquete, peso_kg):
    """Pide cupo para un paquete."""
    return await _pedir("POST", f"/cintas/{cinta_id}/admisiones",
                        {"tipo_paquete": tipo_paquete, "peso_kg": peso_kg})


async def estado_cinta(cinta_id):
    """Consulta el estado de una cinta."""
    return await _pedir("GET", f"/cintas/{cinta_id}")


async def estado_solicitud(solicitud_id):
    """Consulta si una solicitud encolada sigue pendiente o ya termino."""
    return await _pedir("GET", f"/solicitudes/{solicitud_id}")
