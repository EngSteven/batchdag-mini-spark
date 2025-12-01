# --- VARIABLES ---
BINARY_DIR=bin
MASTER_BIN=$(BINARY_DIR)/master
WORKER_BIN=$(BINARY_DIR)/worker
CLIENT_BIN=$(BINARY_DIR)/client

# Directorios de datos
DATA_DIR=data
TMP_DIR=/tmp/mini-spark
SHARED_TMP=tmp_shared
LOG_DIR=logs

.PHONY: all build clean run-cluster run-master run-worker-1 run-worker-2 stop docker-build docker-up docker-run docker-clean

# ==========================================
# 1. ENTORNO LOCAL (Desarrollo / WSL)
# ==========================================

# Compilar todos los binarios
build:
	@echo "🛠️  Compilando binarios..."
	@mkdir -p $(BINARY_DIR)
	@go build -o $(MASTER_BIN) cmd/master/main.go
	@go build -o $(WORKER_BIN) cmd/worker/main.go
	@go build -o $(CLIENT_BIN) cmd/client/main.go
	@echo "✅ Compilación exitosa."

# Limpiar binarios, temporales y logs
clean: stop
	@echo "🧹 Limpiando sistema local..."
	@rm -rf $(BINARY_DIR)
	@rm -rf $(TMP_DIR)/*
	@rm -rf $(LOG_DIR)
	@rm -f master_state.json
	@echo "✨ Sistema limpio."

# Levantar Master y 2 Workers (Logs en carpeta logs/)
run-cluster: build
	@echo "🚀 Iniciando Cluster Local..."
	@mkdir -p $(TMP_DIR)
	@mkdir -p $(LOG_DIR)
	@# Iniciamos Master
	@./$(MASTER_BIN) > $(LOG_DIR)/master.log 2>&1 & echo $$! > $(LOG_DIR)/master.pid
	@echo "   -> Master iniciado (PID en $(LOG_DIR)/master.pid). Logs en $(LOG_DIR)/master.log"
	@sleep 2
	@# Iniciamos Worker 1
	@./$(WORKER_BIN) -port 9001 > $(LOG_DIR)/worker1.log 2>&1 & echo $$! > $(LOG_DIR)/worker1.pid
	@echo "   -> Worker 1 iniciado (PID en $(LOG_DIR)/worker1.pid). Logs en $(LOG_DIR)/worker1.log"
	@# Iniciamos Worker 2
	@./$(WORKER_BIN) -port 9002 > $(LOG_DIR)/worker2.log 2>&1 & echo $$! > $(LOG_DIR)/worker2.pid
	@echo "   -> Worker 2 iniciado (PID en $(LOG_DIR)/worker2.pid). Logs en $(LOG_DIR)/worker2.log"
	@echo "✅ Cluster listo."

# Levantar solo Master (bloqueante)
run-master: build
	@echo "👑 Iniciando Master..."
	@mkdir -p $(TMP_DIR)
	@./$(MASTER_BIN)

# Levantar Worker 1 (bloqueante)
run-worker-1: build
	@echo "👷 Iniciando Worker 1 (Puerto 9001)..."
	@./$(WORKER_BIN) -port 9001

# Levantar Worker 2 (bloqueante)
run-worker-2: build
	@echo "👷 Iniciando Worker 2 (Puerto 9002)..."
	@./$(WORKER_BIN) -port 9002

# Detener todos los procesos de forma SEGURA
stop:
	@echo "🛑 Deteniendo cluster..."
	@# 1. Matar por PID guardado (Usamos -9 para asegurar muerte inmediata y liberar puerto)
	@if [ -f $(LOG_DIR)/master.pid ]; then \
		pid=$$(cat $(LOG_DIR)/master.pid); \
		if [ -n "$$pid" ]; then kill -9 $$pid 2>/dev/null || true; echo "   -> Master PID $$pid eliminado."; fi; \
		rm -f $(LOG_DIR)/master.pid; \
	fi
	@if [ -f $(LOG_DIR)/worker1.pid ]; then \
		pid=$$(cat $(LOG_DIR)/worker1.pid); \
		if [ -n "$$pid" ]; then kill -9 $$pid 2>/dev/null || true; echo "   -> Worker 1 PID $$pid eliminado."; fi; \
		rm -f $(LOG_DIR)/worker1.pid; \
	fi
	@if [ -f $(LOG_DIR)/worker2.pid ]; then \
		pid=$$(cat $(LOG_DIR)/worker2.pid); \
		if [ -n "$$pid" ]; then kill -9 $$pid 2>/dev/null || true; echo "   -> Worker 2 PID $$pid eliminado."; fi; \
		rm -f $(LOG_DIR)/worker2.pid; \
	fi
	@# 2. Limpieza de respaldo por nombre EXACTO de proceso (evita matar 'make')
	@pkill -9 -x "master" || true
	@pkill -9 -x "worker" || true
	@echo "✅ Procesos detenidos y puertos liberados."

# ==========================================
# 2. ENTORNO DOCKER (Entrega / Demo)
# ==========================================

# Solo compilar imágenes
docker-build:
	@echo "🐳 Construyendo imágenes Docker..."
	@docker-compose build

# Levantar cluster
docker-up:
	@echo "🐳 Levantando contenedores en primer plano (Ctrl+C para detener)..."
	@echo "⚠️  IMPORTANTE: Los resultados estarán en la carpeta '$(SHARED_TMP)' de tu PC."
	@mkdir -p $(SHARED_TMP)
	@chmod 777 $(SHARED_TMP)
	@docker-compose up --scale worker-1=1 --scale worker-2=1

# Compilar + Levantar
docker-run: docker-build docker-up

# Limpiar Docker
docker-clean:
	@echo "🐳 Bajando contenedores y limpiando..."
	@docker-compose down -v
	@# Limpieza de archivos creados por docker (root)
	@docker run --rm -v $(PWD)/$(SHARED_TMP):/clean alpine rm -rf /clean/* || true
	@echo "✨ Docker limpio."