#!/usr/bin/env bash
#
# UT4 - Queue-Based Load Leveling (rendimiento).
# Compara una rafaga sincronica con la misma rafaga aceptada y procesada por cola.
# Se ejecuta aparte para no alterar la secuencia de demos existente.
#
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/comun.sh"

titulo "UT4 - QUEUE-BASED LOAD LEVELING"
requiere_sistema

CODIGO_READY="$(curl -s -o /dev/null -w '%{http_code}' "$INGRESO/health/ready")"
if [ "$CODIGO_READY" != "200" ]; then
    fallo "ingreso no esta listo para usar la cola (HTTP $CODIGO_READY)"
    cierre
    exit $?
fi

N="${N:-3000}"
CONCURRENCIA="${CONCURRENCIA:-500}"
URL_SYNC="http://balanceador:82/paquetes"
URL_COLA="http://balanceador:82/paquetes/cola"
URL_ESTADO="http://balanceador:82/solicitudes"

nota "La demo envia $N paquetes de 10 kg con concurrencia $CONCURRENCIA."
nota "Ambas pruebas usan la entrada interna 82 para aislar la cola del"
nota "throttling de NGINX; la ruta publica y las demos actuales no cambian."
nota "Se compara latencia p95 de respuesta, tiempo de vaciado y resultados."

paso "Reconstruyendo servicios y generador con los cambios actuales"
dc build ingreso admision procesador_cola generador
# force-recreate: la plantilla de nginx solo se procesa al arrancar, asi que
# un simple up no aplicaria cambios de nginx.conf ni reabriria conexiones.
dc up -d --force-recreate balanceador ingreso admision procesador_cola
esperar

seccion "Q.1  Rafaga sincronica: el cliente espera la admision"
reiniciar_cinta
SALIDA_SYNC="$(generador rendimiento --n "$N" --c "$CONCURRENCIA" \
    --url "$URL_SYNC" --cuerpo "$PAQUETE_10KG")"
printf '%s\n' "$SALIDA_SYNC"
SYNC_P95="$(printf '%s\n' "$SALIDA_SYNC" | sed -n 's/.*latencia p95    : \([0-9.]*\) ms.*/\1/p')"
SYNC_TIEMPO="$(printf '%s\n' "$SALIDA_SYNC" | sed -n 's/.*tiempo total    : \([0-9.]*\) s.*/\1/p')"

seccion "Q.2  Misma rafaga encolada: respuesta al confirmar el mensaje"
reiniciar_cinta
SALIDA_COLA="$(generador cola --n "$N" --c "$CONCURRENCIA" \
    --url "$URL_COLA" --estado-url "$URL_ESTADO" \
    --cuerpo "$PAQUETE_10KG" --drain-timeout 180 \
    --poll-interval 0.5 --poll-workers 50)"
printf '%s\n' "$SALIDA_COLA"
COLA_P95="$(printf '%s\n' "$SALIDA_COLA" | sed -n 's/.*p95 \([0-9.]*\) ms.*/\1/p')"
COLA_ACEPTADAS="$(printf '%s\n' "$SALIDA_COLA" | sed -n 's/.*aceptadas por la cola: \([0-9][0-9]*\).*/\1/p')"
COLA_PENDIENTES="$(printf '%s\n' "$SALIDA_COLA" | sed -n 's/.*solicitudes aun pendientes: \([0-9][0-9]*\).*/\1/p')"
COLA_VACIADO="$(printf '%s\n' "$SALIDA_COLA" | sed -n 's/.*tiempo desde inicio hasta vaciar la rafaga: \([0-9.]*\) s.*/\1/p')"

seccion "Q.3  Comparacion observada"
printf '\n'
printf '   %-38s %14s %18s\n' "medicion" "sin cola" "con cola"
printf '   %-38s %14s %18s\n' "--------------------------------------" "--------------" "------------------"
printf '   %-38s %14s %18s\n' "p95 respuesta al cliente (ms)" "${SYNC_P95:-N/D}" "${COLA_P95:-N/D}"
printf '   %-38s %14s %18s\n' "tiempo hasta respuesta/vaciado (s)" "${SYNC_TIEMPO:-N/D}" "${COLA_VACIADO:-N/D}"
printf '\n'

verificar "las $N solicitudes fueron aceptadas por RabbitMQ" "${COLA_ACEPTADAS:-0}" "$N"
verificar "la cola termino de procesar toda la rafaga" "${COLA_PENDIENTES:-N/D}" "0"

if [ -n "$SYNC_P95" ] && [ -n "$COLA_P95" ]; then
    if awk -v cola="$COLA_P95" -v sync="$SYNC_P95" 'BEGIN { exit !(cola < sync) }'; then
        ok "en esta ejecucion, la confirmacion de cola tuvo menor p95; el trabajo continua en segundo plano"
    else
        nota "en esta ejecucion no se observo menor p95 de encolado; el beneficio principal es desacoplar y absorber la rafaga"
    fi
fi

nota "El cliente recibe HTTP 202 al quedar el mensaje durablemente encolado;"
nota "el resultado admitida/rechazada se consulta despues. La cola no aumenta"
nota "la capacidad de la cinta: distribuye el trabajo en el tiempo."

reiniciar_cinta
cierre