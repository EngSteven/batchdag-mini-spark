# --- VARIABLES ---
BINARY_DIR=bin
MASTER_BIN=$(BINARY_DIR)/master
WORKER_BIN=$(BINARY_DIR)/worker
CLIENT_BIN=$(BINARY_DIR)/client

# Directorios de datos
DATA_DIR=data
TMP_DIR=/tmp/mini-spark
SHARED_TMP=tmp_shared

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

# Limpiar binarios, temporales y estado
clean:
	@echo "🧹 Limpiando sistema local..."
	@rm -rf $(BINARY_DIR)
	@rm -rf $(TMP_DIR)/*
	@rm -f master_state.json
	@echo "✨ Sistema limpio."

# Levantar Master y 2 Workers de un solo golpe (en background)
run-cluster: build
	@echo "🚀 Iniciando Cluster Local..."
	@mkdir -p $(TMP_DIR)
	@# Iniciamos Master en background
	@./$(MASTER_BIN) > master.log 2>&1 & echo $$! > master.pid
	@echo "   -> Master iniciado (PID en master.pid)"
	@sleep 2
	@# Iniciamos Worker 1
	@./$(WORKER_BIN) -port 9001 > worker1.log 2>&1 & echo $$! > worker1.pid
	@echo "   -> Worker 1 iniciado (PID en worker1.pid)"
	@# Iniciamos Worker 2
	@./$(WORKER_BIN) -port 9002 > worker2.log 2>&1 & echo $$! > worker2.pid
	@echo "   -> Worker 2 iniciado (PID en worker2.pid)"
	@echo "✅ Cluster listo. Logs en *.log"

# Levantar solo Master (bloqueante, para ver logs en consola)
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

# Detener todos los procesos (matar por nombre o PID)
stop:
	@echo "🛑 Deteniendo cluster..."
	@# Intentar matar por PID si existen los archivos
	@if [ -f master.pid ]; then kill $$(cat master.pid) 2>/dev/null || true; rm master.pid; fi
	@if [ -f worker1.pid ]; then kill $$(cat worker1.pid) 2>/dev/null || true; rm worker1.pid; fi
	@if [ -f worker2.pid ]; then kill $$(cat worker2.pid) 2>/dev/null || true; rm worker2.pid; fi
	@# Limpieza agresiva por si acaso (pkill busca por nombre de binario)
	@pkill -f "$(MASTER_BIN)" || true
	@pkill -f "$(WORKER_BIN)" || true
	@echo "✅ Procesos detenidos."

# ==========================================
# 2. ENTORNO DOCKER (Entrega / Demo)
# ==========================================

# Solo compilar imágenes
docker-build:
	@echo "🐳 Construyendo imágenes Docker..."
	@docker-compose build

# Levantar cluster (si ya existen imágenes)
# NOTA: Se ejecuta SIN '-d' para ver logs en terminal.
# NOTA 2: Los archivos se crean en ./tmp_shared (Host) aunque el API diga /tmp/mini-spark (Docker)
docker-up:
	@echo "🐳 Levantando contenedores en primer plano (Ctrl+C para detener)..."
	@echo "⚠️  IMPORTANTE: Los resultados estarán en la carpeta '$(SHARED_TMP)' de tu PC."
	@mkdir -p $(SHARED_TMP)
	@chmod 777 $(SHARED_TMP)
	@docker-compose up --scale worker-1=1 --scale worker-2=1

# Compilar + Levantar (Todo en uno)
docker-run: docker-build docker-up

# Limpiar Docker (Bajar contenedores y borrar volúmenes)
docker-clean:
	@echo "🐳 Bajando contenedores y limpiando..."
	@docker-compose down -v
	@# Usamos docker para borrar archivos creados por root dentro del volumen
	@docker run --rm -v $(PWD)/$(SHARED_TMP):/clean alpine rm -rf /clean/* || true
	@echo "✨ Docker limpio."