#!/bin/bash

# Colores para salida en terminal
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== PRUEBA DE TOLERANCIA A FALLOS (FAULT TOLERANCE TEST) ===${NC}"

# 1. Limpieza de entorno anterior
make clean > /dev/null 2>&1
mkdir -p logs data
rm -f logs/master.log # Eliminar log anterior para asegurar lectura correcta del grep

# 2. Inicialización del Clúster
echo -e "${CYAN}2. Iniciando clúster...${NC}"
# Nota: Se asume que 'make run-cluster' redirige la salida del master a logs/master.log
make run-cluster > /dev/null 2>&1 
sleep 3

# 3. Generación del archivo de Job (con retardo simulado)
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

# 4. Envío del Job
echo -e "${GREEN}4. Enviando job al master...${NC}"
./bin/client submit $JOB_FILE > logs/fail_submit.log
FAIL_ID=$(grep "job_id" logs/fail_submit.log | awk -F'"' '{print $4}')
echo -e "   Job ID asignado: ${YELLOW}$FAIL_ID${NC}"

# 5. Identificación del Worker asignado (Lógica de detección)
echo -e "${CYAN}5. Esperando asignación de tareas para identificar al Worker objetivo...${NC}"
sleep 2 

# Análisis de logs: Buscar qué puerto recibió la tarea 'map_delay'
# Se busca la ocurrencia más reciente de asignación en los puertos 9001 o 9002
TARGET_PORT=$(grep "Asignando tarea a worker" logs/master.log | grep "map_delay" | tail -n 1 | grep -o '900[1-2]')

if [ -z "$TARGET_PORT" ]; then
    echo -e "${RED}[ERROR] No se pudo detectar el worker asignado.${NC}"
    echo -e "${RED}Verifique que el log del Master se esté escribiendo en logs/master.log${NC}"
    exit 1
fi

# Determinar el PID del proceso a terminar basado en el puerto
if [ "$TARGET_PORT" == "9001" ]; then
    VICTIM_PID=$(cat logs/worker1.pid)
    VICTIM_NAME="Worker 1"
else
    VICTIM_PID=$(cat logs/worker2.pid)
    VICTIM_NAME="Worker 2"
fi

echo -e "${RED}[INFO] OBJETIVO DETECTADO: $VICTIM_NAME (Puerto $TARGET_PORT, PID $VICTIM_PID)${NC}"
echo -e "${RED}       Ejecutando SIGKILL en 3 segundos...${NC}"
sleep 3

# 6. Simulación de fallo crítico
kill -9 $VICTIM_PID

# Eliminar archivo PID para mantener consistencia de estado
if [ "$TARGET_PORT" == "9001" ]; then rm logs/worker1.pid; else rm logs/worker2.pid; fi

echo -e "${CYAN}7. Monitoreando recuperación del sistema...${NC}"

# 7. Verificación de recuperación
while true; do
    STATUS=$(./bin/client status $FAIL_ID 2>/dev/null | grep "\"status\"" | cut -d'"' -f4)
    
    if [ "$STATUS" == "COMPLETED" ]; then
        echo -e "${GREEN}[OK] PRUEBA SUPERADA. El sistema recuperó la tarea de $VICTIM_NAME.${NC}"
        break
    fi
    if [ "$STATUS" == "FAILED" ]; then
        echo -e "${RED}[FALLO] El Job terminó con estado FAILED.${NC}"
        break
    fi
    echo -n "."
    sleep 2
done

echo -e "${YELLOW}Prueba finalizada. Ejecute 'make stop' para limpiar los procesos restantes.${NC}"