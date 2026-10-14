#!/usr/bin/env bash
#
# Throttling (disponibilidad).
#   1. un cliente que se pasa del limite recibe 429
#   2. un cliente que inunda el sistema no degrada a los demas
#
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/comun.sh"

titulo "UT4 — THROTTLING"
requiere_sistema

TASA="$(sed -n 's/^THROTTLE_TASA=//p' "$RAIZ/.env")"
RAFAGA="$(sed -n 's/^THROTTLE_RAFAGA=//p' "$RAIZ/.env")"

nota "Configurado en .env y aplicado por nginx en la entrada publica:"
nota ""
nota "    THROTTLE_TASA=$TASA      solicitudes sostenidas por cliente"
nota "    THROTTLE_RAFAGA=$RAFAGA        cuantas mas tolera de golpe"
nota ""
nota "El cliente se identifica por su IP. El generador de carga corre en su"
nota "propio contenedor, asi que para nginx es un cliente distinto de esta"
nota "terminal."

# --- 1 ------------------------------------------------------------------------
seccion "T.1  Un cliente que se pasa del limite"
nota "80 solicitudes simultaneas de un mismo cliente. Uso un paquete invalido"
nota "para no tocar la cinta: las que pasan el limite dan 422 (las rechaza la"
nota "validacion de ingreso), las que no, 429 (las rechaza nginx)."
printf '\n'

SALIDA="$(generador rafaga --n 80 --url http://balanceador/paquetes \
          --cuerpo "$PAQUETE_INVALIDO" --ejemplos)"
printf '%s\n' "$SALIDA"

PASARON="$(printf '%s' "$SALIDA" | sed -n 's/.*HTTP 422 *x\([0-9]*\).*/\1/p')"
FRENADAS="$(printf '%s' "$SALIDA" | sed -n 's/.*HTTP 429 *x\([0-9]*\).*/\1/p')"
printf '\n'
nota "Pasaron ${PASARON:-0}: la rafaga de $RAFAGA mas lo que el limite repuso mientras llegaban."
if [ "${FRENADAS:-0}" -gt 0 ]; then
    ok "nginx freno ${FRENADAS} solicitudes con 429"
else
    fallo "no hubo ninguna respuesta 429"
fi

# --- 2 ------------------------------------------------------------------------
seccion "T.2  Un cliente abusivo no degrada a los demas"
nota "El generador inunda el sistema durante 15 s con 60 hilos. Mientras tanto,"
nota "desde esta terminal, un cliente normal consulta la cinta 25 veces y se"
nota "mide cuanto tarda cada respuesta."
nota ""
nota "Se repite dos veces: inundando por una entrada interna SIN throttling"
nota "(puerto 82, solo alcanzable dentro de Docker) y por la entrada publica"
nota "CON throttling (puerto 80)."

# Mide al cliente normal. Deja MEDIA, MAXIMO y ERRORES en milisegundos.
medir_cliente_normal() {
    local tiempos
    tiempos="$(for _ in $(seq 1 25); do
        curl -s -o /dev/null -w '%{http_code} %{time_total}\n' "$INGRESO/cintas/$CINTA"
    done)"
    MEDIA="$(printf '%s\n' "$tiempos" | awk '{ s += $2 } END { printf "%.0f", s / NR * 1000 }')"
    MAXIMO="$(printf '%s\n' "$tiempos" | awk '$2 > m { m = $2 } END { printf "%.0f", m * 1000 }')"
    ERRORES="$(printf '%s\n' "$tiempos" | awk '$1 != 200 { e++ } END { print e + 0 }')"
    nota "cliente normal: media $MEDIA ms, maximo $MAXIMO ms, errores $ERRORES"
}

# inundar <puerto>: inunda en segundo plano y mide al cliente normal mientras.
inundar() {
    local salida; salida="$(mktemp)"
    reiniciar_cinta
    generador rendimiento --segundos 15 --c 60 \
        --url "http://balanceador:$1/paquetes" --cuerpo "$PAQUETE_10KG" > "$salida" &
    sleep 3                          # que la inundacion ya este en marcha
    medir_cliente_normal
    wait
    nota "cliente abusivo:"
    grep -E 'solicitudes  |codigos' "$salida" | sed 's/^/   /'
    rm -f "$salida"
}

paso "Sin carga, como referencia"
medir_cliente_normal
REF_MEDIA="$MEDIA"; REF_MAX="$MAXIMO"

paso "Inundacion SIN throttling (puerto 82)"
inundar 82
SIN_MEDIA="$MEDIA"; SIN_MAX="$MAXIMO"; SIN_ERR="$ERRORES"

paso "Inundacion CON throttling (puerto 80)"
inundar 80
CON_MEDIA="$MEDIA"; CON_MAX="$MAXIMO"; CON_ERR="$ERRORES"

seccion "T.3  Comparacion — lo que ve el cliente normal"
printf '\n'
printf '   %-30s %12s %12s %10s\n' "" "media (ms)" "maximo (ms)" "errores"
printf '   %-30s %12s %12s %10s\n' "------------------------------" "----------" "-----------" "-------"
printf '   %-30s %12s %12s %10s\n' "sin carga"                 "$REF_MEDIA" "$REF_MAX" "0"
printf '   %-30s %12s %12s %10s\n' "inundacion SIN throttling" "$SIN_MEDIA" "$SIN_MAX" "$SIN_ERR"
printf '   %-30s %12s %12s %10s\n' "inundacion CON throttling" "$CON_MEDIA" "$CON_MAX" "$CON_ERR"
printf '\n'

if [ "$CON_ERR" -eq 0 ] && [ "$CON_MEDIA" -lt $(( SIN_MEDIA / 2 )) ]; then
    ok "con throttling el cliente normal no se degrada ($CON_MEDIA ms contra $SIN_MEDIA ms)"
elif [ "$CON_ERR" -eq 0 ] && [ "$SIN_MEDIA" -le $(( REF_MEDIA * 3 + 5 )) ] \
    && [ "$CON_MEDIA" -le $(( REF_MEDIA * 3 + 5 )) ]; then
    ok "el cliente normal no se degrada ni siquiera sin throttling (el sistema absorbe la inundacion: $SIN_MEDIA ms contra referencia $REF_MEDIA ms, sin errores)"
else
    fallo "se esperaba que con throttling el cliente normal tardara menos de la mitad"
fi

nota ""
nota "Sin throttling, el exceso llega a ingreso, admision y la base, y todos los"
nota "clientes compiten por la misma capacidad. Con throttling, el exceso se"
nota "rechaza en nginx (responder un 429 no le cuesta casi nada) y la"
nota "capacidad queda para los clientes que respetan el limite."

reiniciar_cinta
cierre
