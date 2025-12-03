# --- VARIABLES DE CONFIGURACIÓN ---
BINARY_DIR=bin
MASTER_BIN=$(BINARY_DIR)/master
WORKER_BIN=$(BINARY_DIR)/worker
CLIENT_BIN=$(BINARY_DIR)/client

# Directorios de datos y logs
DATA_DIR=data
TMP_DIR=/tmp/mini-spark
SHARED_TMP=tmp_shared
LOG_DIR=logs

.PHONY: all build clean run-cluster run-master run-worker-1 run-worker-2 stop docker-build docker-up docker-run docker-clean

# ==========================================
# 1. ENTORNO LOCAL (Desarrollo / WSL)
# ==========================================

# Compilación de todos los binarios
build:
	@echo "[BUILD] Compilando binarios..."
	@mkdir -p $(BINARY_DIR)
	@go build -o $(MASTER_BIN) cmd/master/main.go
	@go build -o $(WORKER_BIN) cmd/worker/main.go
	@go build -o $(CLIENT_BIN) cmd/client/main.go
	@echo "[OK] Compilación exitosa."

# Limpieza de binarios, archivos temporales y logs
clean: stop
	@echo "[CLEAN] Limpiando entorno local..."
	@rm -rf $(BINARY_DIR)
	@rm -rf $(TMP_DIR)/*
	@rm -rf $(LOG_DIR)
	@rm -f master_state.json
	@echo "[OK] Sistema limpio."

# Ejecución del Cluster en segundo plano (Background)
# Redirige stdout/stderr a archivos en logs/
run-cluster: build
	@echo "[INFO] Iniciando Cluster Local..."
	@mkdir -p $(TMP_DIR)
	@mkdir -p $(LOG_DIR)
	@# Iniciar Master
	@./$(MASTER_BIN) > $(LOG_DIR)/master.log 2>&1 & echo $$! > $(LOG_DIR)/master.pid
	@echo "   -> Master iniciado (PID en $(LOG_DIR)/master.pid). Logs: $(LOG_DIR)/master.log"
	@sleep 2
	@# Iniciar Worker 1
	@./$(WORKER_BIN) -port 9001 > $(LOG_DIR)/worker1.log 2>&1 & echo $$! > $(LOG_DIR)/worker1.pid
	@echo "   -> Worker 1 iniciado (PID en $(LOG_DIR)/worker1.pid). Logs: $(LOG_DIR)/worker1.log"
	@# Iniciar Worker 2
	@./$(WORKER_BIN) -port 9002 > $(LOG_DIR)/worker2.log 2>&1 & echo $$! > $(LOG_DIR)/worker2.pid
	@echo "   -> Worker 2 iniciado (PID en $(LOG_DIR)/worker2.pid). Logs: $(LOG_DIR)/worker2.log"
	@echo "[OK] Cluster operativo."

# Ejecución de Master (Primer plano / Foreground)
run-master: build
	@echo "[INFO] Iniciando Master..."
	@mkdir -p $(TMP_DIR)
	@./$(MASTER_BIN)

# Ejecución de Worker 1 (Primer plano / Foreground)
run-worker-1: build
	@echo "[INFO] Iniciando Worker 1 (Puerto 9001)..."
	@./$(WORKER_BIN) -port 9001

# Ejecución de Worker 2 (Primer plano / Foreground)
run-worker-2: build
	@echo "[INFO] Iniciando Worker 2 (Puerto 9002)..."
	@./$(WORKER_BIN) -port 9002

# Detención segura de procesos
stop:
	@echo "[STOP] Deteniendo servicios del cluster..."
	@# 1. Terminación por PID específico (SIGKILL para liberación inmediata de puertos)
	@if [ -f $(LOG_DIR)/master.pid ]; then \
		pid=$$(cat $(LOG_DIR)/master.pid); \
		if [ -n "$$pid" ]; then kill -9 $$pid 2>/dev/null || true; echo "   -> Master PID $$pid terminado."; fi; \
		rm -f $(LOG_DIR)/master.pid; \
	fi
	@if [ -f $(LOG_DIR)/worker1.pid ]; then \
		pid=$$(cat $(LOG_DIR)/worker1.pid); \
		if [ -n "$$pid" ]; then kill -9 $$pid 2>/dev/null || true; echo "   -> Worker 1 PID $$pid terminado."; fi; \
		rm -f $(LOG_DIR)/worker1.pid; \
	fi
	@if [ -f $(LOG_DIR)/worker2.pid ]; then \
		pid=$$(cat $(LOG_DIR)/worker2.pid); \
		if [ -n "$$pid" ]; then kill -9 $$pid 2>/dev/null || true; echo "   -> Worker 2 PID $$pid terminado."; fi; \
		rm -f $(LOG_DIR)/worker2.pid; \
	fi
	@# 2. Limpieza de respaldo por nombre de proceso (match exacto)
	@pkill -9 -x "master" || true
	@pkill -9 -x "worker" || true
	@echo "[OK] Procesos detenidos y puertos liberados."

# Ejecución de suite de pruebas (Unitarias, Integración, E2E)
test:
	@echo "[TEST] Ejecutando suite de pruebas..."
	@# Limpieza previa para evitar conflictos con procesos activos
	@$(MAKE) stop > /dev/null 2>&1 || true
	@go test -v ./tests/...
	@echo "[OK] Pruebas finalizadas."

# ==========================================
# 2. ENTORNO DOCKER (Entrega / Demo)
# ==========================================

# Construcción de imágenes
docker-build:
	@echo "[DOCKER] Construyendo imágenes..."
	@docker-compose build

# Despliegue del cluster
docker-up:
	@echo "[DOCKER] Iniciando contenedores (Ctrl+C para detener)..."
	@echo "[INFO] Los resultados se mapearán en el directorio local '$(SHARED_TMP)'."
	@mkdir -p $(SHARED_TMP)
	@chmod 777 $(SHARED_TMP)
	@docker-compose up --scale worker-1=1 --scale worker-2=1

# Alias: Construir y Desplegar
docker-run: docker-build docker-up

# Limpieza profunda de Docker
docker-clean:
	@echo "[DOCKER] Deteniendo contenedores y eliminando volúmenes..."
	@docker-compose down -v
	@# Limpieza de archivos generados por root dentro del contenedor
	@docker run --rm -v $(PWD)/$(SHARED_TMP):/clean alpine rm -rf /clean/* || true
	@echo "[OK] Entorno Docker limpio."