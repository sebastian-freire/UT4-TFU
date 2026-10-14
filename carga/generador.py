"""
Generador de carga para las demostraciones.

Corre dentro de la red de Docker y no en la maquina anfitriona: lanzar decenas
de procesos curl desde Windows mide sobre todo el costo de crear procesos, no la
capacidad de los servicios.

  rafaga       N solicitudes a la vez. Cuenta las respuestas por codigo HTTP.
  rendimiento  N solicitudes con concurrencia C. Mide solicitudes por segundo.
               Con --segundos S manda sin parar durante S segundos.
"""

import argparse
import json
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

ETIQUETAS = {
    200: "admitido",
    409: "rechazado por falta de cupo",
    422: "rechazado por la validacion del paquete",
    429: "rechazado por throttling",
    503: "servicio no disponible",
    0: "sin respuesta",
}


def instancia_de(texto):
    """Saca de la respuesta el nombre de la replica que la atendio."""
    try:
        valor = json.loads(texto).get("instancia", "")
        return valor.get("ingreso", "") if isinstance(valor, dict) else valor
    except Exception:
        return ""


def solicitar(url, cuerpo, timeout, api_key=None):
    datos = json.dumps(cuerpo).encode() if cuerpo else None
    cabeceras = {"Content-Type": "application/json"} if datos else {}
    if api_key:
        cabeceras["X-API-Key"] = api_key
    pedido = urllib.request.Request(url, data=datos, headers=cabeceras)

    inicio = time.perf_counter()
    try:
        respuesta = urllib.request.urlopen(pedido, timeout=timeout)
        codigo, texto = respuesta.status, respuesta.read().decode()
    except urllib.error.HTTPError as error:
        codigo, texto = error.code, error.read().decode()
    except Exception:
        codigo, texto = 0, ""

    return codigo, (time.perf_counter() - inicio) * 1000, instancia_de(texto), texto


def correr(args, concurrencia):
    cuerpo = json.loads(args.cuerpo) if args.cuerpo else None
    with ThreadPoolExecutor(max_workers=concurrencia) as pool:
        inicio = time.perf_counter()
        if args.segundos:
            # Cada hilo manda solicitudes una tras otra hasta que se acaba el tiempo.
            fin = inicio + args.segundos

            def hasta_el_final(_):
                propios = []
                while time.perf_counter() < fin:
                    propios.append(solicitar(args.url, cuerpo, args.timeout))
                return propios

            resultados = [r for lote in pool.map(hasta_el_final, range(concurrencia))
                          for r in lote]
        else:
            resultados = list(pool.map(
                lambda _: solicitar(args.url, cuerpo, args.timeout), range(args.n)))
    return resultados, time.perf_counter() - inicio


def mostrar_ejemplos(resultados):
    """Una respuesta de ejemplo por cada codigo distinto."""
    vistos = {}
    for codigo, _, _, texto in resultados:
        vistos.setdefault(codigo, texto)
    for codigo, texto in sorted(vistos.items()):
        if texto:
            print(f"      ejemplo HTTP {codigo}: {texto[:150]}")


def rafaga(args):
    resultados, segundos = correr(args, args.n)
    print(f"  {args.n} solicitudes simultaneas en {segundos:.2f} s")
    for codigo, cantidad in sorted(Counter(c for c, _, _, _ in resultados).items()):
        print(f"      HTTP {codigo}  x{cantidad:<4} {ETIQUETAS.get(codigo, '')}")
    if args.ejemplos:
        mostrar_ejemplos(resultados)


def rendimiento(args):
    resultados, segundos = correr(args, args.c)
    latencias = [ms for _, ms, _, _ in resultados]
    latencias_ordenadas = sorted(latencias)
    codigos = Counter(c for c, _, _, _ in resultados)
    instancias = Counter(i for _, _, i, _ in resultados if i)

    print(f"  solicitudes     : {len(resultados)} (concurrencia {args.c})")
    print(f"  tiempo total    : {segundos:.2f} s")
    print(f"  throughput      : {len(resultados) / segundos:.1f} solicitudes/segundo")
    print(f"  latencia media  : {sum(latencias) / len(latencias):.1f} ms")
    print(f"  latencia p50    : {latencias_ordenadas[(len(latencias) - 1) // 2]:.1f} ms")
    indice_p95 = min(len(latencias) - 1, (len(latencias) * 95 + 99) // 100 - 1)
    print(f"  latencia p95    : {latencias_ordenadas[indice_p95]:.1f} ms")
    print(f"  codigos         : {dict(sorted(codigos.items()))}")
    for instancia, cantidad in sorted(instancias.items()):
        print(f"      {instancia}  ->  {cantidad} solicitudes")


def cola(args):
    """Mide la respuesta de encolado y el tiempo hasta el resultado final."""
    cuerpo = json.loads(args.cuerpo) if args.cuerpo else None
    inicio = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.c) as pool:
        envios = list(pool.map(
            lambda _: solicitar(args.url, cuerpo, args.timeout, args.api_key),
            range(args.n),
        ))
    fin_envios = time.perf_counter()
    encoladas = []
    for codigo, _, _, texto in envios:
        if codigo == 202:
            try:
                encoladas.append(json.loads(texto)["solicitud_id"])
            except (json.JSONDecodeError, KeyError):
                pass

    latencias = sorted(ms for codigo, ms, _, _ in envios if codigo == 202)
    if latencias:
        p50 = latencias[(len(latencias) - 1) // 2]
        p95 = latencias[min(len(latencias) - 1, int(len(latencias) * 0.95))]
    else:
        p50 = p95 = 0
    print(f"  solicitudes enviadas : {len(envios)}")
    print(f"  aceptadas por la cola: {len(encoladas)} (HTTP 202)")
    print(f"  latencia de encolado : media {sum(latencias) / len(latencias):.1f} ms, "
          f"p50 {p50:.1f} ms, p95 {p95:.1f} ms" if latencias else
          "  latencia de encolado : sin solicitudes aceptadas")
    for codigo, cantidad in sorted(Counter(c for c, _, _, _ in envios).items()):
        if codigo != 202:
            print(f"  respuesta inmediata HTTP {codigo}: {cantidad}")
    print(f"  tiempo para aceptar la rafaga: {fin_envios - inicio:.2f} s")

    # Sondeo de resultados. Solo HTTP 200 es terminal: 202 significa que el
    # trabajador aun no llega, y cualquier otro codigo (0 por timeout, 429,
    # 503 por saturacion) es transitorio y se reintenta hasta el plazo de
    # vaciado. La version anterior contaba esos fallos como resultados
    # finales y vaciaba el conjunto de pendientes sin haber terminado.
    pendientes = set(encoladas)
    resultados = Counter()
    transitorios = 0
    tope_sondeo = fin_envios + args.drain_timeout
    hilos_sondeo = max(1, min(args.c, args.poll_workers))
    espera = args.poll_interval
    with ThreadPoolExecutor(max_workers=hilos_sondeo) as pool:
        while pendientes and time.perf_counter() < tope_sondeo:
            lote = list(pendientes)
            estados = list(pool.map(
                lambda solicitud_id: solicitar(
                    f"{args.estado_url.rstrip('/')}/{solicitud_id}", None,
                    args.timeout, args.api_key),
                lote,
            ))
            for solicitud_id, (codigo, _, _, texto) in zip(lote, estados):
                if codigo == 202:
                    continue
                if codigo == 200:
                    pendientes.discard(solicitud_id)
                    try:
                        resultados[json.loads(texto).get("estado", "desconocido")] += 1
                    except json.JSONDecodeError:
                        resultados["respuesta invalida"] += 1
                else:
                    transitorios += 1
            if pendientes:
                time.sleep(espera)
                espera = min(espera * 1.25, 5.0)

    final = time.perf_counter()
    print(f"  resultados finales : {dict(sorted(resultados.items()))}")
    if transitorios:
        print(f"  sondeos transitorios reintentados: {transitorios} "
              f"(timeouts o 429/503 durante el vaciado, no son resultados)")
    print(f"  solicitudes aun pendientes: {len(pendientes)}")
    print(f"  tiempo desde inicio hasta vaciar la rafaga: {final - inicio:.2f} s")


analizador = argparse.ArgumentParser(description="Generador de carga")
analizador.add_argument("modo", choices=["rafaga", "rendimiento", "cola"])
analizador.add_argument("--url", required=True)
analizador.add_argument("--cuerpo", help="cuerpo JSON de la solicitud")
analizador.add_argument("--n", type=int, default=50, help="cantidad de solicitudes")
analizador.add_argument("--c", type=int, default=30, help="concurrencia")
analizador.add_argument("--timeout", type=float, default=30)
analizador.add_argument("--segundos", type=float, help="mandar durante S segundos en vez de N solicitudes")
analizador.add_argument("--ejemplos", action="store_true", help="mostrar una respuesta por codigo")
analizador.add_argument("--api-key", help="API Key externa opcional")
analizador.add_argument("--estado-url", help="URL base para consultar estados de solicitudes encoladas")
analizador.add_argument("--poll-interval", type=float, default=0.5,
                        help="pausa inicial entre rondas de sondeo (con retroceso suave)")
analizador.add_argument("--poll-workers", type=int, default=50,
                        help="hilos para el sondeo, acotados para no saturar al servidor")
analizador.add_argument("--drain-timeout", type=float, default=120,
                        help="plazo total para vaciar la cola tras aceptar la rafaga")

args = analizador.parse_args()
if args.modo == "cola" and not args.estado_url:
    analizador.error("el modo cola requiere --estado-url")
{"rafaga": rafaga, "rendimiento": rendimiento, "cola": cola}[args.modo](args)
