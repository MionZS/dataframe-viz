# Quick Reference Card

## Installation & Setup

```bash
# 1. Create virtual environment
uv venv
.venv\Scripts\activate        # Windows
# or
source .venv/bin/activate     # Linux/Mac

# 2. Install dependencies
uv pip install -r requirements.txt

# 3. Verify installation
python main.py
```

## Main Commands

### 📊 View Data Files
```bash
# Interactive browser (choose files)
python main.py view

# View specific file
python main.py view --file data/raw/ORCA/Dados_Comunicacao.parquet

# View directory as single dataset
python main.py view --dir-as-file data/raw/ORCA/
```

### 🔍 Inspect Data
```bash
# Interactive inspector
python main.py inspect

# Inspect specific file
python main.py inspect --file data/trusted/CIS/Data_Referencia.csv
```

### 📅 Enrich with Dates
```bash
# Process all files (ORCA + SANPLAT)
python main.py enrich

# Process specific dataset
python main.py enrich --orca         # ORCA only
python main.py enrich --sanplat      # SANPLAT only

# Process custom files
python main.py enrich --data-file data/my.csv --ref-file data/ref.csv
```

## Direct Tool Access

```bash
# Lazy Frame Viewer
python src/tui/lazy_frame_viewer.py --file data.csv
python src/tui/lazy_frame_viewer.py --dir-as-file data/

# Data Inspector
python src/tui/data_inspector.py --file data.csv

# Date Enrichment Script
python src/enrich_dates.py --orca
python src/enrich_dates.py --sanplat
python src/enrich_dates.py --all
```

## File Locations

| Purpose | Location | Type |
|---------|----------|------|
| ORCA raw data | `data/raw/ORCA/Dados_Comunicacao.parquet` | Input |
| SANPLAT raw data | `data/refined/SANPLAT/Dados_Comunicacao_SANPLAT.csv` | Input |
| ORCA reference | `data/trusted/CIS/Data_Referencia.csv` | Reference |
| SANPLAT reference | `data/trusted/SANPLAT/Data_Referencia_2.csv` | Reference |
| ORCA enriched | `data/trusted/ORCA/Dados_Comunicacao_com_datas.csv` | Output |
| SANPLAT enriched | `data/trusted/SANPLAT/Dados_Comunicacao_SANPLAT_com_datas.csv` | Output |

## Data Structure

### ORCA (Parquet)
- **Format**: Apache Parquet
- **Source**: `data/raw/ORCA/Dados_Comunicacao.parquet`
- **Columns**: 90, 89, 88, ..., 2, 1 (relative days)
- **Reference Date**: 24/02/2026
- **Column 90 = 23/02/2026** (ref_date - 1)
- **Output**: `data/trusted/ORCA/Dados_Comunicacao_com_datas.csv`

### SANPLAT (CSV)
- **Format**: CSV
- **Source**: `data/refined/SANPLAT/Dados_Comunicacao_SANPLAT.csv`
- **Columns**: 91, 90, 89, ..., 2, 1 (relative days)
- **Reference Date**: 23/02/2026
- **Column 91 = 22/02/2026** (ref_date - 1)
- **Output**: `data/trusted/SANPLAT/Dados_Comunicacao_SANPLAT_com_datas.csv`

## Workflow Example

```bash
# 1. Setup
uv venv && source .venv/bin/activate && uv pip install -r requirements.txt

# 2. Check data
python main.py inspect --file data/trusted/CIS/Data_Referencia.csv

# 3. Enrich with dates
python main.py enrich --all

# 4. Verify results
python main.py view --file data/trusted/ORCA/Dados_Comunicacao_com_datas.csv

# 5. Inspect output
python main.py inspect --file data/trusted/ORCA/Dados_Comunicacao_com_datas.csv
```

## Viewer Navigation

Once in the viewer (after `python main.py view`):

| Key | Action |
|-----|--------|
| `n` / `↓` | Next row |
| `p` / `↑` | Previous row |
| `s` | Skip N rows |
| `j` | Jump to specific row |
| `k` | Change search column |
| `c` | Change context size |
| `h` | Show help |
| `q` | Quit |

## Inspector Menu

Once in the inspector (after `python main.py inspect`):

| Option | Action |
|--------|--------|
| `1` | View schema (column types) |
| `2` | Show sample data |
| `3` | Save schema to JSON |
| `4` | Reload file |
| `q` | Quit |

## Troubleshooting

### "ModuleNotFoundError: No module named 'polars'"
→ Activate venv: `.venv\Scripts\activate` or `source .venv/bin/activate`
→ Install: `uv pip install -r requirements.txt`

### "File not found"
→ Check file path exists
→ Use absolute paths if relative paths don't work

### "Could not parse date"
→ Reference CSV must have valid date format
→ Supported: YYYY-MM-DD, DD-MM-YYYY, DD/MM/YYYY

### "No numeric columns found"
→ Data file must have numeric column names (90-1 for ORCA, 91-1 for SANPLAT)

## Documentation

| Document | Purpose |
|----------|---------|
| QUICK_START.md | 30-second setup |
| SETUP.md | Detailed installation |
| README.md | Full documentation |
| ENRICH_GUIDE.md | Date enrichment details |
| PROJECT_SUMMARY.md | Project overview |

## Performance Notes

- **Parquet files**: Metadata-based processing (fast)
- **CSV files**: Streaming processing (memory-efficient)
- **Large files**: Lazy evaluation preserves memory
- **Multiple files**: Directory mode indexes but doesn't load all at once

## Key Features

✨ **Interactive Tools**
- Browse files in TUI
- Navigate data row by row
- Search and filter

📊 **Format Support**
- CSV with auto-detection
- Parquet with PyArrow
- Mixed directory processing

🔄 **Data Processing**
- Lazy loading
- Streaming evaluation
- Batch enrichment

📅 **Date Intelligence**
- Automatic format detection
- Relative to absolute conversion
- Multi-format reference dates

---

**Version**: 2.0.0  
**Last Updated**: 2026-02-24
