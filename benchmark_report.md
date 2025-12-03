# Reporte de Benchmarks - Mini-Spark
**Fecha:** Tue Dec  2 22:06:31 CST 2025

## 1. Entorno de Ejecución
| Componente | Detalle |
| --- | --- |
| **OS** | Linux 6.6.87.2-microsoft-standard-WSL2 |
| **CPU** | 11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz (8 vCores) |
| **Go Version** | go version go1.22.2 linux/amd64 |

## 2. Parámetros de la Prueba
- **Registros de Ventas (Join):** 5000000
- **Líneas de Texto (WordCount):** 3000000
- **Workers:** 2 (Configuración default Docker/Local)

## 3. Resultados
| Benchmark | Job ID | Estado | Duración (s) |
| --- | --- | --- | --- |
| WordCount (Shuffle) | `2e74f7df-f566-4c07-9e89-8ffaae324f95` | EXITO | **8.858501584** |
| Join (Large Dataset) | `e4258a55-9de6-47a7-b27d-964810f7fc67` | EXITO | **2.481913823** |

---
*Reporte generado automáticamente por script de prueba.*
