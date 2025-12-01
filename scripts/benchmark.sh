#!/bin/bash

# Colores
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m'

echo -e "${YELLOW}=== MINI-SPARK BENCHMARK SUITE (CARGA PESADA EXTREMA) ===${NC}"

# 0. Preparar directorios
mkdir -p data
mkdir -p jobs
mkdir -p logs

# 1. Compilar Generador y Generar Datos
# AUMENTO DE CARGA SIGNIFICATIVO:
# - Sales: 10,000,000 registros (~350 MB)
# - Lines: 5,000,000 líneas (~250 MB)
echo -e "${GREEN}[1/3] Generando Datos de Prueba Masivos en 'data/'...${NC}"
go run cmd/datagen/main.go -sales 10000000 -lines 5000000

# 2. Benchmark WordCount
echo -e "${GREEN}[2/3] Ejecutando WordCount Masivo (jobs/bench_wordcount.json)...${NC}"

if [ ! -f jobs/bench_wordcount.json ]; then
    echo "❌ Error: jobs/bench_wordcount.json no encontrado."
    exit 1
fi

# Guardar log en carpeta logs/
go run cmd/client/main.go submit jobs/bench_wordcount.json > logs/wc_submit.log
WC_ID=$(grep "job_id" logs/wc_submit.log | awk -F'"' '{print $4}')
echo "Job ID: $WC_ID - Procesando..."

# Polling
while true; do
    STATUS=$(go run cmd/client/main.go status $WC_ID)
    if echo "$STATUS" | grep -q "COMPLETED"; then
        echo "✅ WordCount Terminado."
        # Extraer duración y métricas si es posible
        echo "$STATUS" | grep "duration_secs"
        break
    fi
    if echo "$STATUS" | grep -q "FAILED"; then
        echo "❌ WordCount Falló."
        exit 1
    fi
    sleep 2
done

# 3. Benchmark Join
echo -e "${GREEN}[3/3] Ejecutando Join Masivo (jobs/bench_join.json)...${NC}"

if [ ! -f jobs/bench_join.json ]; then
    echo "❌ Error: jobs/bench_join.json no encontrado."
    exit 1
fi

# Guardar log en carpeta logs/
go run cmd/client/main.go submit jobs/bench_join.json > logs/join_submit.log
JOIN_ID=$(grep "job_id" logs/join_submit.log | awk -F'"' '{print $4}')
echo "Job ID: $JOIN_ID - Procesando..."

while true; do
    STATUS=$(go run cmd/client/main.go status $JOIN_ID)
    if echo "$STATUS" | grep -q "COMPLETED"; then
        echo "✅ Join Terminado."
        echo "$STATUS" | grep "duration_secs"
        break
    fi
    sleep 2
done

echo -e "${YELLOW}=== BENCHMARKS FINALIZADOS ===${NC}"