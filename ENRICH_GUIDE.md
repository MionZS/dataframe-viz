# Date Reference Enrichment Guide

## Overview

The `enrich_dates.py` script automatically adds a secondary header row with actual dates to communication data files that use relative date numbering.

## Problem Statement

Communication data files often use relative date columns:

- **ORCA parquet**: Columns numbered 90-1, where 90 = reference date - 1
- **SANPLAT CSV**: Columns numbered 91-1, where 91 = reference date - 1

This script converts these relative dates to actual calendar dates in a secondary header row.

## How It Works

### Data Flow

```text
Input Files:
├── Dados_Comunicacao.parquet/csv (with relative date columns)
└── Data_Referencia.csv (with reference date)
        ↓
Process:
├── Read reference date
├── Calculate actual dates for each column
├── Insert date header as row 2
└── Save enriched file
        ↓
Output: [filename]_com_datas.csv (in trusted directory)
```

### Examples

#### ORCA Processing

| Input | Reference Date | Column 90  | Column 89  | ... Column 1   |
| ----- | -------------- | ---------- | ---------- | -------------- |
| Raw   | 24/02/2026     | 23/02      | 22/02      | ... 24/05      |
| After | (unchanged)    | 23/02/2026 | 22/02/2026 | ... 24/05/2026 |

#### SANPLAT Processing

| Input | Reference Date | Column 91  | Column 90  | ... Column 1   |
| ----- | -------------- | ---------- | ---------- | -------------- |
| Raw   | 23/02/2026     | 22/02      | 21/02      | ... 23/05      |
| After | (unchanged)    | 22/02/2026 | 21/02/2026 | ... 23/05/2026 |

## Usage

### Quick Start (Process All)

```bash
# Process both ORCA and SANPLAT
python main.py enrich
```

### Process Specific Data

```bash
# Process only ORCA
python main.py enrich --orca

# Process only SANPLAT
python main.py enrich --sanplat
```

### Custom Files

```bash
# Process custom files
python main.py enrich --data-file path/to/data.csv --ref-file path/to/reference.csv
```

### Direct Script Execution

```bash
# Run directly
python src/enrich_dates.py

# Process ORCA only
python src/enrich_dates.py --orca

# Process SANPLAT only
python src/enrich_dates.py --sanplat
```

## File Locations

### ORCA Configuration

- **Data file**: `data/raw/ORCA/Dados_Comunicacao.parquet`
- **Reference file**: `data/trusted/CIS/Data_Referencia.csv`
- **Output file**: `data/trusted/ORCA/Dados_Comunicacao_com_datas.csv`
- **Column mapping**: 90 (ref-1), 89 (ref-2), ..., 1 (ref-90)

### SANPLAT Configuration

- **Data file**: `data/refined/SANPLAT/Dados_Comunicacao_SANPLAT.csv`
- **Reference file**: `data/trusted/SANPLAT/Data_Referencia_2.csv`
- **Output file**: `data/trusted/SANPLAT/Dados_Comunicacao_SANPLAT_com_datas.csv`
- **Column mapping**: 91 (ref-1), 90 (ref-2), ..., 1 (ref-91)

## Output Format

The enriched files are saved as CSV with the following structure:

```csv
col_name_1,col_name_2,col_name_3,90,89,...,1
col_name_1,col_name_2,col_name_3,23/02/2026,22/02/2026,...,24/05/2026
[original data rows...]
```

The secondary header (dates) is inserted as the first data row.

## Reference Date Formats

The script automatically detects and parses these date formats:

- ISO format: `2026-02-24` (YYYY-MM-DD)
- European format: `24-02-2026` (DD-MM-YYYY)
- Slash format: `24/02/2026` (DD/MM/YYYY)

## Error Handling

The script provides detailed error messages for:

- Missing or invalid data files
- Missing or inaccessible reference files
- Unparseable date formats
- Empty or malformed data

Example error output:

```text
[ERROR] Failed to read reference file: File not found
[ERROR] Data file not found: data/raw/ORCA/Dados_Comunicacao.parquet
[ERROR] Could not parse date: invalid_date_string
```

## Performance

- **ORCA (Parquet)**: Parquet metadata is used for efficient processing
- **SANPLAT (CSV)**: CSV files are parsed with streaming for memory efficiency
- **Memory**: Minimal overhead - only loads data row structure, processes in streaming mode

## Troubleshooting

### Issue: "No numeric columns found in data file"

**Solution**: Ensure the data file has numeric column names (90-1 for ORCA, 91-1 for SANPLAT)

### Issue: "Could not parse date"

**Solution**: Check the reference CSV has a valid date format (YYYY-MM-DD, DD-MM-YYYY, or DD/MM/YYYY)

### Issue: "Output file already exists"

**Solution**: The script overwrites existing files. Backup if needed before running.

### Issue: Permission denied

**Solution**: Ensure the trusted directory is writable and you have file access permissions

## Dependencies

- polars >= 0.20.0
- Python 3.7+
- No external date/time libraries needed (uses built-in datetime)

## Implementation Details

### Date Calculation Algorithm

For each numeric column `N`:

1. Find the maximum column number (90 for ORCA, 91 for SANPLAT)
2. Calculate offset: `days_offset = max_col - N`
3. Calculate date: `actual_date = (ref_date - 1) - days_offset`

This ensures:

- Largest column number = reference_date - 1
- Decreases by 1 day for each column number decrease

### Output Strategy

Files are saved to the trusted directory as CSV for:

- Easy viewing and editing
- Compatibility with downstream tools
- Reduced file size compared to parquet

---

**Question?** Check the main README or inspect individual scripts for more details.
