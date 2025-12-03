# Reporte de Benchmarks - Mini-Spark
**Fecha:** Tue Dec  2 23:24:20 CST 2025

## 1. Entorno de Ejecución
| Componente | Detalle |
| --- | --- |
| **OS** | Linux 6.6.87.2-microsoft-standard-WSL2 |
| **CPU** | 11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz (8 vCores) |
| **Go Version** | go version go1.22.2 linux/amd64 |

## 2. Parámetros de la Prueba
- **Registros de Ventas (Join):** 5000000
- **Líneas de Texto (WordCount):** 3000000
- **Workers:** 2 (Configuración por defecto)

## 3. Resultados
| Benchmark | Job ID | Estado | Duración (s) |
| --- | --- | --- | --- |
| WordCount (Shuffle) | `a854d950-7adc-4f99-b6cc-bf4508612dcf` | EXITO | **8.660661347** |
| Join (Large Dataset) | `cd2987d0-3fc8-4b50-9c4d-f9086c73f18b` | EXITO | **2.211272147** |

---
*Reporte generado automáticamente por script de benchmark.*
