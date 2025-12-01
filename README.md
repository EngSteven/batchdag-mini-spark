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

### Opción A: Ejecución Local 

Ideal para desarrollo y pruebas rápidas. 

**Compilar Binarios**
```bash
make build
```

**Iniciar el Clúster (Master + 2 Workers):**
```bash
make run-cluster
```
Esto compila el código y levanta los procesos. Los logs se guardan en `master.log`, `worker1.log`, etc.


**Detener el Clúster:**
```bash
make stop
```

**Limpiar todo (binarios y datos temporales):**
```bash
make clean
```

**Workers personalizables por terminal separada**

```bash
go run cmd/worker/main.go --port #puerto
```

### Opción B: Ejecución con Docker 

Simula un entorno distribuido real con redes aisladas.

**Construir y Levantar Contenedores:**
```bash
make docker-run
```
Verás los logs de todos los nodos en tu terminal.

**Verificar Archivos de Salida:**
Los resultados generados por los workers dentro de Docker aparecerán automáticamente en tu carpeta local:
```bash
ls -l tmp_shared/
```

**Apagar y Limpiar Docker:**
```bash
make docker-clean
```

## Uso del Cliente 

El cliente permite interactuar con el Master para enviar trabajos y consultar resultados.

### 1. Enviar un Trabajo 

Envía un archivo JSON con la definición del DAG.

Terminal: 

```bash
# Si usas entorno local:
./bin/client submit archivo

# Si no has compilado antes:
go run cmd/client/main.go submit archivo
```

O por su defecto desde postman o similar:

POST
```bash
http://localhost:8080/api/v1/jobs
```

Body ejemplo:

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

Salida ejemplo:

```bash
{"job_id":"6eecef97-42f2-4e16-8b9f-4ae8eaf37889","status":"ACCEPTED"}
```


### 2. Consultar Estado 

Muestra el progreso (%), métricas y estado de cada nodo del grafo.

Terminal:

```bash
./bin/client status <JOB_ID>
```

O por su defecto desde postman o similar:

GET
```bash
http://localhost:8080/api/v1/jobs/<JOB_ID>
```

Salida ejemplo:

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

Terminal:

```bash
./bin/client results <JOB_ID>
```

O por su defecto desde postman o similar:

GET
```bash
http://localhost:8080/api/v1/jobs/<JOB_ID>/results
```

Salida ejemplo:

```bash
{"job_id":"6eecef97-42f2-4e16-8b9f-4ae8eaf37889","outputs":{"agg":"/tmp/mini-spark/6eecef97-42f2-4e16-8b9f-4ae8eaf37889_agg.txt"}}
```

## Pruebas Disponibles

Hemos incluido scripts y datos de prueba para verificar la funcionalidad.

**Generar datos de prueba:**
Asegúrate de que exista la carpeta `data/` con archivos CSV (`users.csv`, `purchases.csv`).

**Script de Demo E2E:**
Ejecuta un flujo completo (Limpieza → Compilación → Ejecución → Test → Reporte).

```bash
./scripts/run_demo.sh
```

**Prueba de Join:**
Usa el archivo `join_job.json` para probar el operador de cruce de datos.

**Prueba de JSONL:**
Usa el archivo `jsonl_test_job.json` para probar la lectura/escritura en formato JSON Lines.

##  Estructura del Proyecto


