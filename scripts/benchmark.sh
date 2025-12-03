#!/bin/bash

# Colores
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

# Configuración de Carga (Cumple > 1M registros)
NUM_SALES=5000000   # 5 Millones (~150MB)
NUM_LINES=3000000   # 3 Millones (~100MB)
REPORT_FILE="benchmark_report.md"

# Directorios
mkdir -p data jobs logs

echo -e "${YELLOW}=== MINI-SPARK BENCHMARK SUITE (AUTO-REPORT) ===${NC}"
echo "Generando reporte en: $REPORT_FILE"

# ==========================================
# 1. GENERAR ENCABEZADO DEL REPORTE
# ==========================================
echo "# Reporte de Benchmarks - Mini-Spark" > $REPORT_FILE
echo "**Fecha:** $(date)" >> $REPORT_FILE
echo "" >> $REPORT_FILE

echo "## 1. Entorno de Ejecución" >> $REPORT_FILE
echo "| Componente | Detalle |" >> $REPORT_FILE
echo "| --- | --- |" >> $REPORT_FILE
echo "| **OS** | $(uname -sr) |" >> $REPORT_FILE
# Intentar obtener CPU info (Linux vs Mac/WSL)
if command -v lscpu &> /dev/null; then
    CPU_MODEL=$(lscpu | grep "Model name" | cut -d: -f2 | xargs)
    CPU_CORES=$(lscpu | grep "^CPU(s):" | cut -d: -f2 | xargs)
else
    CPU_MODEL="Unknown"
    CPU_CORES="Unknown"
fi
echo "| **CPU** | $CPU_MODEL ($CPU_CORES vCores) |" >> $REPORT_FILE
echo "| **Go Version** | $(go version) |" >> $REPORT_FILE
echo "" >> $REPORT_FILE

echo "## 2. Parámetros de la Prueba" >> $REPORT_FILE
echo "- **Registros de Ventas (Join):** $(printf "%'d" $NUM_SALES)" >> $REPORT_FILE
echo "- **Líneas de Texto (WordCount):** $(printf "%'d" $NUM_LINES)" >> $REPORT_FILE
echo "- **Workers:** 2 (Configuración default Docker/Local)" >> $REPORT_FILE
echo "" >> $REPORT_FILE

echo "## 3. Resultados" >> $REPORT_FILE
echo "| Benchmark | Job ID | Estado | Duración (s) |" >> $REPORT_FILE
echo "| --- | --- | --- | --- |" >> $REPORT_FILE

# ==========================================
# 2. GENERACIÓN DE DATOS
# ==========================================
echo -e "${GREEN}[1/3] Generando Datos de Prueba...${NC}"
START_GEN=$(date +%s)
go run cmd/datagen/main.go -sales $NUM_SALES -lines $NUM_LINES
END_GEN=$(date +%s)
GEN_TIME=$((END_GEN - START_GEN))
echo -e "${CYAN}   -> Datos generados en ${GEN_TIME}s${NC}"

# ==========================================
# 3. BENCHMARK WORDCOUNT
# ==========================================
echo -e "${GREEN}[2/3] Ejecutando WordCount...${NC}"

if [ ! -f jobs/bench_wordcount.json ]; then
    echo -e "${RED}Error: jobs/bench_wordcount.json no encontrado.${NC}"
    exit 1
fi

go run cmd/client/main.go submit jobs/bench_wordcount.json > logs/wc_submit.log
WC_ID=$(grep "job_id" logs/wc_submit.log | awk -F'"' '{print $4}')
echo "   -> Job ID: $WC_ID"

# Polling
WC_DURATION="N/A"
WC_STATUS="TIMEOUT"
for i in {1..60}; do # Timeout de 2 mins aprox
    RESP=$(go run cmd/client/main.go status $WC_ID)
    STATUS=$(echo "$RESP" | grep "\"status\"" | cut -d'"' -f4)
    
    if [ "$STATUS" == "COMPLETED" ]; then
        WC_DURATION=$(echo "$RESP" | grep "duration_secs" | awk -F': ' '{print $2}' | sed 's/,//')
        WC_STATUS="EXITO"
        echo -e "${CYAN}   -> Completado en ${WC_DURATION}s${NC}"
        break
    elif [ "$STATUS" == "FAILED" ]; then
        WC_STATUS="FALLIDO"
        echo -e "${RED}   -> Job Falló${NC}"
        break
    fi
    echo -n "."
    sleep 2
done
echo ""

# Escribir en reporte
echo "| WordCount (Shuffle) | \`$WC_ID\` | $WC_STATUS | **$WC_DURATION** |" >> $REPORT_FILE

# ==========================================
# 4. BENCHMARK JOIN
# ==========================================
echo -e "${GREEN}[3/3] Ejecutando Join...${NC}"

if [ ! -f jobs/bench_join.json ]; then
    echo -e "${RED}Error: jobs/bench_join.json no encontrado.${NC}"
    exit 1
fi

go run cmd/client/main.go submit jobs/bench_join.json > logs/join_submit.log
JOIN_ID=$(grep "job_id" logs/join_submit.log | awk -F'"' '{print $4}')
echo "   -> Job ID: $JOIN_ID"

# Polling
JOIN_DURATION="N/A"
JOIN_STATUS="TIMEOUT"
for i in {1..60}; do
    RESP=$(go run cmd/client/main.go status $JOIN_ID)
    STATUS=$(echo "$RESP" | grep "\"status\"" | cut -d'"' -f4)
    
    if [ "$STATUS" == "COMPLETED" ]; then
        JOIN_DURATION=$(echo "$RESP" | grep "duration_secs" | awk -F': ' '{print $2}' | sed 's/,//')
        JOIN_STATUS="EXITO"
        echo -e "${CYAN}   -> Completado en ${JOIN_DURATION}s${NC}"
        break
    elif [ "$STATUS" == "FAILED" ]; then
        JOIN_STATUS="FALLIDO"
        echo -e "${RED}   -> Job Falló${NC}"
        break
    fi
    echo -n "."
    sleep 2
done
echo ""

# Escribir en reporte
echo "| Join (Large Dataset) | \`$JOIN_ID\` | $JOIN_STATUS | **$JOIN_DURATION** |" >> $REPORT_FILE

echo "" >> $REPORT_FILE
echo "---" >> $REPORT_FILE
echo "*Reporte generado automáticamente por script de prueba.*" >> $REPORT_FILE

echo -e "${YELLOW}=== FINALIZADO ===${NC}"
echo -e "📄 Reporte disponible en: ${GREEN}$REPORT_FILE${NC}"
cat $REPORT_FILE