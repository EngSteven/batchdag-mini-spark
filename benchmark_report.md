# Reporte de Benchmarks - Mini-Spark
**Fecha:** Tue Dec  2 21:31:31 CST 2025

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
| WordCount (Shuffle) | `f5a5c2b8-7904-4f41-8ea2-49622530575c` | EXITO | **8.578043743** |
| Join (Large Dataset) | `4bbff9f8-0181-4152-a076-776d89f5970f` | EXITO | **2.397945294** |

---
*Reporte generado automáticamente por script de prueba.*
