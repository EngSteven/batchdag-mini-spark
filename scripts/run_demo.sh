#!/bin/bash

# Colores
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== INICIANDO DEMO MINI-SPARK ===${NC}"

# 1. Limpieza previa
echo "Limpiando entorno..."
rm -rf /tmp/mini-spark
mkdir -p /tmp/mini-spark
rm -f master_state.json
pkill master
pkill worker

# 2. Compilar
echo -e "${GREEN}[1/5] Compilando proyecto...${NC}"
go build -o bin/master cmd/master/main.go
go build -o bin/worker cmd/worker/main.go
go build -o bin/client cmd/client/main.go

# 3. Iniciar Infraestructura
echo -e "${GREEN}[2/5] Iniciando Cluster (1 Master, 2 Workers)...${NC}"
./bin/master > master.log 2>&1 &
MASTER_PID=$!
echo "Master PID: $MASTER_PID"

sleep 2

./bin/worker -port 9001 > worker1.log 2>&1 &
W1_PID=$!
echo "Worker 1 PID: $W1_PID"

./bin/worker -port 9002 > worker2.log 2>&1 &
W2_PID=$!
echo "Worker 2 PID: $W2_PID"

sleep 3 # Esperar registro

# 4. Enviar Job
echo -e "${GREEN}[3/5] Enviando Job de Prueba (Join)...${NC}"
# Asegúrate de que existe join_job.json y data/
./bin/client submit join_job.json > client_submit.log
JOB_ID=$(grep "job_id" client_submit.log | awk -F'"' '{print $4}')
echo "Job ID detectado: $JOB_ID"

# 5. Polling de estado
echo -e "${GREEN}[4/5] Esperando finalización...${NC}"
for i in {1..10}; do
    STATUS=$(./bin/client status $JOB_ID)
    echo "$STATUS" | grep "progress_percent"
    if echo "$STATUS" | grep -q '"status": "COMPLETED"'; then
        echo "¡Job Completado!"
        break
    fi
    sleep 2
done

# 6. Resultados
echo -e "${GREEN}[5/5] Obteniendo resultados...${NC}"
./bin/client results $JOB_ID

echo -e "${GREEN}=== DEMO FINALIZADA EXITOSAMENTE ===${NC}"
echo "Logs disponibles en master.log, worker1.log, worker2.log"

# Cleanup al salir
trap "kill $MASTER_PID $W1_PID $W2_PID" EXIT