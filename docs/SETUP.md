# Setup

This setup is for the Smart Meter Communication Pipeline (not the legacy TUI viewer project).

## 1. Environment

```bash
uv venv
```

Activate venv:

- Windows (cmd):

```cmd
.venv\Scripts\activate
```

- Linux/Mac:

```bash
source .venv/bin/activate
```

Install dependencies:

```bash
uv pip install -r requirements.txt
```

## 2. Configure Target Month

Edit `config/pipeline.yaml`:

- `target_month: "YYYY-MM"`

Examples:

- February 2026: `target_month: "2026-02"`
- March 2026: `target_month: "2026-03"`

## 3. Verify Required Inputs

- `data/raw/ORCA/Dados_Comunicacao.parquet`
- `data/refined/SANPLAT/Dados_Comunicacao_SANPLAT.csv`
- `data/trusted/CIS/Data_Referencia.csv`
- `data/trusted/SANPLAT/Data_Referencia_2.csv`
- `data/refined/CIS/MEDIDORES.parquet`
- `D:/Projects/visualizer-tuis/data/raw/CIS/Diario/Diario_YYYY-MM-DD.parquet`

## 4. Run Pipeline

```bash
python src/pipeline_orchestrator.py
```

Or pass explicit config path:

```bash
python src/pipeline_orchestrator.py config/pipeline.yaml
```

## 5. Outputs to Check

Per-month outputs:

- `data/trusted/ORCA/disp_YYYY-MM.csv`
- `data/trusted/SANPLAT/disp_YYYY-MM.csv`
- `data/trusted/mixed/disp_YYYY-MM.csv`
- `data/trusted/municipio_daily/municipio_YYYY-MM.csv`
- `data/trusted/municipio_daily/municipio_YYYY-MM.parquet`

## 6. Notes

- Enrichment is Phase 1 and runs automatically during orchestrator execution.
- Final availability `DISP` is computed in aggregate as `CONTAGEM_COMM / CONTAGEM_TOT` by `[MUNICIPIO, INTELIGENTE]`.
- MEDIDORES is filtered to target brands: `Hexing`, `Nansen`, `Nansen Ipiranga`.
