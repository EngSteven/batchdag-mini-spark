# Mini-Spark: Motor de Procesamiento Distribuido

Este proyecto implementa desde cero un sistema de procesamiento distribuido por lotes (Batch Processing) basado en grafos acíclicos dirigidos (DAG), inspirado en la arquitectura de Apache Spark.

Fue desarrollado como parte del curso Principios de Sistemas Operativos en el Tecnológico de Costa Rica.

## Autores

Steven Sequeira Araya

Jefferson Salas Cordero

## Descripción General

El sistema permite definir trabajos (Jobs) como una serie de etapas interconectadas (DAG) y ejecutarlas en un clúster de nodos trabajadores (Workers) coordinados por un nodo maestro (Master).

### Características Principales

- **Arquitectura Master-Worker**: Comunicación vía HTTP/JSON.
- **Planificador Inteligente**: Asignación Round-Robin con manejo de dependencias entre tareas.
- **Tolerancia a Fallos**: Detección de workers caídos (Heartbeats) y re-planificación automática de tareas.
- **Gestión de Memoria**: Implementación de Spill to Disk cuando el uso de memoria excede el umbral configurado.
- **Persistencia**: El Master guarda su estado en disco para sobrevivir a reinicios.
- **Operadores Soportados**: `map`, `flat_map`, `filter`, `reduce_by_key`, `join`.
- **Formatos de Datos**: Lectura y escritura de CSV y JSONL.
- **Observabilidad**: Logging estructurado, métricas de CPU/RAM en tiempo real y API de estado.

## Requisitos Previos

- Go 1.22+ (para ejecución local)
- Docker & Docker Compose (para ejecución en contenedores)
- Make (opcional, para usar los scripts de automatización)
- Entorno Linux/WSL recomendado

## Compilación y Ejecución

### Listado de Comandos Disponibles

| Comando | Descripción | Comportamiento |
|---------|-------------|----------------|
| `make build` | Compila todos los binarios del sistema | Crea ejecutables en `bin/` para master, worker y client |
| `make clean` | Limpia completamente el sistema local | Elimina binarios, archivos temporales, logs y estado |
| `make run-cluster` | Inicia el cluster completo (Master + 2 Workers) | Levanta procesos en segundo plano con logs en `logs/` |
| `make run-master` | Inicia solo el Master (modo bloqueante) | Ejecuta el master en primer plano para debugging |
| `make run-worker-1` | Inicia Worker 1 en puerto 9001 (bloqueante) | Útil para pruebas individuales |
| `make run-worker-2` | Inicia Worker 2 en puerto 9002 (bloqueante) | Útil para pruebas individuales |
| `make stop` | Detiene todos los procesos del cluster | Mata procesos por PID y limpia archivos .pid |
| `make test` | Ejecuta suite completa de pruebas | Corre tests unitarios, de integración y E2E |
| `make docker-build` | Construye imágenes Docker del sistema | Crea imágenes basadas en el Dockerfile |
| `make docker-up` | Levanta contenedores en primer plano | Inicia master y workers con logs visibles |
| `make docker-run` | Compila y levanta contenedores | Combina `docker-build` y `docker-up` |
| `make docker-clean` | Limpia entorno Docker completamente | Elimina contenedores, volúmenes y archivos temporales |

### Levantar Cluster Modo Desarrollo

**Opción 1: Levantar master + workers sin logs en terminal**
```bash
make run-cluster
```

**Opción 2: Levantar master + workers con logs en terminal. Correr cada uno en una terminal distinta**
```bash
make run-master
make run-worker-1
make run-worker-2
```

**Opción extra: Levantar workers con puerto personalizado**
```bash
go run cmd/worker/main.go --port #puerto
```

**Detener el Clúster**
```bash
make stop
```

**Limpiar binarios y datos temporales**
```bash
make clean
```

### Levantar Cluster Modo Despligue con Docker

**Construir y Levantar Contenedores**
```bash
make docker-run
```

**Verificar Archivos de Salida**
Los resultados generados por los workers dentro de Docker aparecerán automáticamente en tu carpeta local:
```bash
tmp_shared/
```

**Apagar y Limpiar Docker**
```bash
make docker-clean
```

## Uso del Cliente 

El cliente permite interactuar con el Master para enviar trabajos y consultar resultados. **Debe asegurarse de haber levantado el Master y al menos un Worker, ya sea en modo desarrollo o despliegue.**

Además, puede ser usado desde la terminal o desde algún cliente http como postman.

### 1. Enviar un Trabajo 

Envía un archivo JSON con la definición del DAG (puede consultar el directorio /logs para revisar los ya existentes).

**Terminal** 

```bash
./bin/client submit archivo
```

**Cliente HTTP**

`POST`
```bash
http://localhost:8080/api/v1/jobs
```

**Body ejemplo**
```bash
{
  "name": "wordcount-batch",
  "dag": {
    "nodes": [
      {
        "id": "read",
        "op": "read_csv",
        "path": "data/books.csv",
        "partitions": 4
      },
      {
        "id": "flat",
        "op": "flat_map",
        "fn": "tokenize"
      },
      {
        "id": "map1",
        "op": "map",
        "fn": "to_lower"
      },
      {
        "id": "agg",
        "op": "reduce_by_key",
        "key": "token",
        "fn": "sum"
      }
    ],
    "edges": [
      ["read", "flat"],
      ["flat", "map1"],
      ["map1", "agg"]
    ]
  },
  "parallelism": 4
}
```

**Salida ejemplo**

```bash
{"job_id":"6eecef97-42f2-4e16-8b9f-4ae8eaf37889","status":"ACCEPTED"}
```


### 2. Consultar Estado 

Muestra el progreso (%), métricas y estado de cada nodo del grafo.

**Terminal**
```bash
./bin/client status <JOB_ID>
```

**Cliente HTTP**

`GET`
```bash
http://localhost:8080/api/v1/jobs/<JOB_ID>
```

**Salida ejemplo**

```bash
{
    "id": "6eecef97-42f2-4e16-8b9f-4ae8eaf37889",
    "name": "wordcount-batch",
    "status": "COMPLETED",
    "submitted_at": "2025-12-01T00:05:22.598493621-06:00",
    "duration_secs": 0.015053549,
    "progress_percent": 100,
    "node_status": {
        "agg": "COMPLETED",
        "flat": "COMPLETED",
        "map1": "COMPLETED",
        "read": "COMPLETED"
    },
    "failure_count": 0
}
```

### 3. Obtener Resultados 

Descarga/Muestra las rutas de los archivos finales generados.

**Terminal**

```bash
./bin/client results <JOB_ID>
```

**Cliente HTTP**

`GET`
```bash
http://localhost:8080/api/v1/jobs/<JOB_ID>/results
```

**Salida ejemplo**

```bash
{"job_id":"6eecef97-42f2-4e16-8b9f-4ae8eaf37889","outputs":{"agg":"/tmp/mini-spark/6eecef97-42f2-4e16-8b9f-4ae8eaf37889_agg.txt"}}
```

## Pruebas Disponibles

Se tienen 3 tipos de prueba: por scripts, manuales y automatizadas.

### Scripts

#### Demo E2E (End-to-End)
Ejecuta un flujo completo de demostración: limpia el entorno, compila el código, levanta el clúster, envía un trabajo de prueba (join_job.json) y muestra los resultados. Es ideal para verificar rápidamente que todo el sistema funciona.

```bash
chmod +x scripts/run_demo.sh
./scripts/run_demo.sh
```

### Benchmark de Rendimiento
Genera grandes volúmenes de datos sintéticos (millones de registros) y ejecuta trabajos pesados (WordCount y Join) para probar la escalabilidad, el paralelismo y el mecanismo de Spill to Disk.

```bash
chmod +x scripts/benchmark.sh
./scripts/benchmark.sh
```

### Manuales 

Puedes ejecutar trabajos individuales definidos en la carpeta `jobs/`, que usan datos de la carpeta `data`, para probar funcionalidades específicas.

#### Prueba de Join
Usa el archivo `jobs/join_job.json` para probar el operador de cruce de datos entre dos fuentes CSV (usuarios y compras).

```bash
./bin/client submit jobs/join_job.json
```

#### Prueba de JSONL
Usa el archivo `jobs/jsonl_test_job.json` para probar la lectura y escritura en formato JSON Lines.

```bash
./bin/client submit jobs/jsonl_test_job.json
```

#### Prueba de wordcount
Usa el archivo `jobs/donquijote-wordcount.json` para probar el conteo de palabras de un extracto del grande del libro.

```bash
./bin/client submit jobs/donquijote-wordcount.json
```

### Automatizadas

El proyecto cuenta con una suite de pruebas automatizadas en Go que cubren desde la lógica de los operadores hasta la integración del sistema completo.

| Tipo de Prueba | Archivo | Descripción |
|----------------|---------|-------------|
| **Unitarias** | `tests/unit_test.go` | Verifican la lógica interna de los operadores (Map, Reduce, Filter, Join) de forma aislada. |
| **Integración** | `tests/integration_test.go` | Prueban flujos de datos complejos encadenando múltiples operadores sin necesidad de levantar todo el clúster. |
| **End-to-End** | `tests/e2e_test.go` | Levantan un clúster real (procesos Master y Worker), envían un trabajo real y validan el resultado final en el sistema de archivos. |

#### Ejecutar Todas las Pruebas Automatizadas

```bash
make test
```


##  Estructura del Proyecto

```bash
mini-spark/
├── bin/                       # Binarios compilados
│   ├── client
│   ├── master
│   └── worker
├── cmd/                       # Puntos de entrada (Main)
│   ├── client/
│   │   └── main.go
│   ├── datagen/
│   │   └── main.go
│   ├── master/
│   │   └── main.go
│   └── worker/
│       └── main.go
├── data/                      # Datasets de entrada
│   ├── big_text.csv
│   ├── books.csv
│   ├── catalog.csv
│   ├── don_quijote.txt
│   ├── purchases.csv
│   ├── sales.csv
│   ├── users.csv
│   └── users2.csv
├── internal/                  # Lógica interna del sistema
│   ├── common/
│   │   └── types.go
│   ├── master/
│   │   ├── api.go
│   │   ├── scheduler.go
│   │   └── state.go
│   ├── operators/
│   │   └── operators.go
│   ├── utils/
│   │   └── helpers.go
│   └── worker/
│       ├── agent.go
│       └── executor.go
├── jobs/                      # Definiciones de trabajos (JSON)
│   ├── bench_join.json
│   ├── bench_wordcount.json
│   ├── donquijote-wordcount.json
│   ├── join_job.json
│   ├── jsonl_test_job.json
│   └── test_job.json
├── logs/                      # Archivos de registro
│   ├── join_submit.log
│   ├── master.log
│   ├── wc_submit.log
│   ├── worker1.log
│   └── worker2.log
├── scripts/                   # Scripts de utilidad/bash
│   ├── benchmark.sh
│   └── run_demo.sh
├── tests/                     # Tests unitarios y de integración
│   ├── e2e_test.go
│   ├── integration_test.go
│   └── unit_test.go
├── tmp_shared/                # Logs de resultado de Docker
│   ├── b990ee63..._agg.txt
│   ├── b990ee63..._flat.txt
│   ├── b990ee63..._map1.txt
│   └── b990ee63..._read.txt
├── vendor/                    # Dependencias vendored (Go)
├── Dockerfile
├── Makefile
├── README.md
├── docker-compose.yml
├── go.mod
├── go.sum
└── master_state.json
```