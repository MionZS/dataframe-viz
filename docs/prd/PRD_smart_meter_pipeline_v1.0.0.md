# PRD — Smart Meter Communication Consolidation Pipeline

**Project:** `visualizer-tuis`
**PRD Version:** v1.0.0
**Status:** Draft
**Author:** Data Processing Team
**Last Updated:** 2026-02-25

---

## 1. Overview

### 1.1 Objective

Construir um pipeline lazy, streaming-first, memory-safe para:

* Padronizar datas entre ORCA e SANPLAT
* Consolidar ambas as origens
* Aplicar regra de janela movel de 5 dias
* Enriquecer com MUNICIPIO via arquivo diario
* Agregar por municipio/origem
* Produzir outputs particionados por data
* Controlar memoria e fila de sinks simultaneos

---

## 2. Scope

### 2.1 In Scope

* Processamento via Polars LazyFrame
* Streaming obrigatorio
* Controle de memoria
* Fila limitada de sinks (max 3)
* Agregacao municipal por data

### 2.2 Out of Scope

* Interface visual
* Persistencia em banco relacional
* Orquestracao via Airflow
* Reprocessamento historico completo automatico

---

## 3. Data Sources

### 3.1 Inputs

```text
RAW ORCA:
D:\Projects\visualizer-tuis\data\raw\ORCA\Dados_Comunicacao.parquet

REFINED SANPLAT:
D:\Projects\visualizer-tuis\data\refined\SANPLAT\Dados_Comunicacao_SANPLAT.csv

DIARIO:
D:\dados\Diario\Diario_<YYYY-MM-DD>.parquet
```

### 3.2 Outputs

```text
TRUSTED MIXED:
D:\Projects\visualizer-tuis\data\trusted\mixed\

MUNICIPIO DAILY:
D:\Projects\visualizer-tuis\data\trusted\municipio_daily\
```

---

## 4. Business Rules

### 4.1 Date Handling

* Datas absolutas
* Mes alvo
* 5 dias anteriores ao mes
* Total esperado: 36 datas

### 4.2 Moving Window Rule (5 Days)

Para cada data D:

```
DISP(D) = max(D-5 ... D-1)
```

Criterio:

* Se qualquer dia anterior != 0 -> 1
* Se todos forem 0 -> 0

Nao e media aritmetica.
E janela logica OR binaria.

### 4.3 Join Rule

```
INNER JOIN ON NIO
```

Mantem apenas medidores com disponibilidade registrada.

### 4.4 Aggregation Rule

Group By:

```
[MUNICIPIO, ORIGEM]
```

Outputs:

```
CONTAGEM_COMM = sum(DISP)
CONTAGEM_TOT  = count(NIO)
```

Schema final:

```
[MUNICIPIO, ORIGEM, CONTAGEM_COMM, CONTAGEM_TOT, DATA]
```

---

## 5. Technical Architecture

### 5.1 Processing Model

* 100% LazyFrame
* Streaming execution
* Nunca materializar dataset completo
* Uso obrigatorio de `scan_parquet` / `scan_csv`

### 5.2 Pipeline Flow

```
RAW -> REFINED (datas absolutas)
        |
DATE RANGE EXPORT
        |
Concat ORCA + SANPLAT
        |
Moving Window
        |
Join Diario
        |
Group By Municipio
        |
Sink Particionado
```

---

## 6. Memory & IO Management

### 6.1 Memory Constraints

* Apenas 3 objetos ativos simultaneamente:
  * Mixed parcial
  * Diario atual
  * Resultado agregado
* Arquivos nao usados nao ficam na memoria
* Apos join -> liberar diario
* Apos agregacao -> liberar parcial

### 6.2 Sink Queue

#### Rules

* Maximo 3 sinks simultaneos
* FIFO
* Se fila cheia:
  * Bloquear proxima operacao
  * Manter DF atual em memoria
* Apos termino do sink:
  * Liberar slot
  * Continuar processamento

### 6.3 Memory Tracking

Monitorar:

* RSS via `psutil`
* Threshold configuravel (ex: 70% RAM)
* Logging periodico
* Abort seguro em caso critico

---

## 7. Operational Flow

Para cada data D do mes:

1. Gerar DISP(D) via moving window
2. Carregar diario D (Lazy)
3. Selecionar `[NIO, MUNICIPIO]`
4. Inner join
5. Group by
6. Sink controlado
7. Liberar memoria
8. Prosseguir para proxima data

---

## 8. Module Structure

```
src/
|
|-- enrich_dates.py          (existing)
|-- moving_window.py         (new)
|-- join_daily.py            (new)
|-- sink_manager.py          (new)
|-- memory_monitor.py        (new)
|-- pipeline_orchestrator.py (new)
```

---

## 9. Testing Strategy

### 9.1 Unit Tests

* Moving window correctness
* Join integrity
* Aggregation correctness

### 9.2 Integration Tests

* 1 dia
* 10 dias
* 36 dias

### 9.3 Stress Tests

* Dataset > 10M NIO
* Simulacao de fila cheia
* Memoria proxima do limite

---

## 10. Performance Expectations

| Operation     | Complexity |
| ------------- | ---------- |
| Concat        | O(n)       |
| Moving Window | O(n x 5)  |
| Join Diario   | O(n log n) |
| Group By      | O(n)       |

Pipeline linear por data.

---

## 11. Failure Handling

* Falha em 1 data -> log + continuar proximas
* Diario ausente -> log + skip
* Sink falha -> retry ate N tentativas
* Memoria excedida -> abort seguro

---

## 12. Configuration

Arquivo YAML:

```yaml
target_month: 2026-01
moving_window_days: 5
sink_queue_limit: 3
memory_threshold_percent: 70
output_format: parquet
```

---

## 13. Observability

* Log tempo por etapa
* Log memoria por etapa
* Log tamanho do sink queue
* Log tamanho dos datasets processados

---

## 14. Change Log

### v1.0.0

* Definicao inicial do pipeline
* Moving window OR logic
* Sink queue limitada
* Memory tracking

---

## 15. Future Enhancements

* Paralelizacao por data
* Persistencia incremental delta
* Compressao otimizada
* Telemetria Prometheus
* Cache de diario

---

## 16. Versioning Rules

* Major -> Mudanca estrutural no pipeline
* Minor -> Nova funcionalidade
* Patch -> Correcao de bug / melhoria interna
