#!/bin/bash

# Colores
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${YELLOW}=== PRUEBA DE FALLO INTELIGENTE (SNIPER MODE) ===${NC}"

# 1. Limpieza
make clean > /dev/null 2>&1
mkdir -p logs data
rm -f logs/master.log # Importante borrar log viejo para no confundir grep

# 2. Levantar CLUSTER (Redirigiendo salida de Master a un archivo para leerlo)
echo -e "${CYAN}2. Levantando clúster...${NC}"
# Asumimos que make run-cluster guarda el log del master en logs/master.log
# Si tu makefile no lo hace, asegurate de correr el master así: ./bin/master > logs/master.log 2>&1 &
make run-cluster > /dev/null 2>&1 
sleep 3

# 3. Preparar el Job con SLEEP
JOB_FILE="jobs/fail_test_delay.json"
cat <<EOF > $JOB_FILE
{
  "name": "FAIL_TEST_DELAY",
  "dag": {
    "nodes": [
      {"id": "read_delay", "op": "read_csv", "path": "data/users.csv"},
      {"id": "map_delay",  "op": "map",      "fn": "sleep_10s"}
    ],
    "edges": [["read_delay", "map_delay"]]
  },
  "parallelism": 1
}
EOF

# 4. Enviar el Job
echo -e "${GREEN}4. Enviando job...${NC}"
./bin/client submit $JOB_FILE > logs/fail_submit.log
FAIL_ID=$(grep "job_id" logs/fail_submit.log | awk -F'"' '{print $4}')
echo -e "   Job ID: ${YELLOW}$FAIL_ID${NC}"

# 5. DETECTAR VÍCTIMA (La parte mágica)
echo -e "${CYAN}5. Esperando asignación para identificar al Worker objetivo...${NC}"
sleep 2 # Dar tiempo a que el Master asigne la tarea

# Buscamos en el log quién tiene 'map_delay'
# Buscamos la cadena "9001" o "9002" en la línea más reciente de asignación
TARGET_PORT=$(grep "Asignando tarea a worker" logs/master.log | grep "map_delay" | tail -n 1 | grep -o '900[1-2]')

if [ -z "$TARGET_PORT" ]; then
    echo -e "${RED}❌ No pude detectar quién tiene la tarea. ¿El log del Master se está guardando en logs/master.log?${NC}"
    exit 1
fi

# Determinar qué PID matar basado en el puerto encontrado
if [ "$TARGET_PORT" == "9001" ]; then
    VICTIM_PID=$(cat logs/worker1.pid)
    VICTIM_NAME="Worker 1"
else
    VICTIM_PID=$(cat logs/worker2.pid)
    VICTIM_NAME="Worker 2"
fi

echo -e "${RED}🎯 OJETIVO DETECTADO: $VICTIM_NAME (Puerto $TARGET_PORT, PID $VICTIM_PID)${NC}"
echo -e "${RED}   Ejecutando 'kill -9' en 3 segundos...${NC}"
sleep 3

# 6. Asesinato
kill -9 $VICTIM_PID
# Borramos el PID file para evitar errores futuros
if [ "$TARGET_PORT" == "9001" ]; then rm logs/worker1.pid; else rm logs/worker2.pid; fi

echo -e "${CYAN}7. Esperando recuperación del sistema...${NC}"

# 7. Esperar resultado
while true; do
    STATUS=$(./bin/client status $FAIL_ID 2>/dev/null | grep "\"status\"" | cut -d'"' -f4)
    
    if [ "$STATUS" == "COMPLETED" ]; then
        echo -e "${GREEN}✅ PRUEBA SUPERADA! El sistema recuperó la tarea de $VICTIM_NAME.${NC}"
        break
    fi
    if [ "$STATUS" == "FAILED" ]; then
        echo -e "${RED}❌ El Job Falló.${NC}"
        break
    fi
    echo -n "."
    sleep 2
done

echo -e "${YELLOW}Demo terminada. Use 'make stop' para limpiar.${NC}"