# GCP Visualizer TUIs Project

Clean, isolated project structure for CSV and Parquet data visualization and inspection tools.

## Tools Included

### 1. LazyFrame Viewer (`lazy_frame_viewer.py`)

Interactive viewer for very large CSV and Parquet files (70M+ lines) with memory-optimized navigation.

**Features:**

- Memory-efficient lazy loading (streaming)
- Supports CSV and Parquet formats
- Works with single files or directories
- Navigate line by line
- Keyword search in columns
- Extract matching records to CSV
- Supports multiple delimiters and encodings

**Usage:**

```bash
# Interactive mode - choose file/directory
python src/tui/lazy_frame_viewer.py

# Open specific CSV or Parquet file
python src/tui/lazy_frame_viewer.py --file /path/to/data.csv
python src/tui/lazy_frame_viewer.py --file /path/to/data.parquet

# Load entire directory as one unified view
python src/tui/lazy_frame_viewer.py --dir-as-file /path/to/data_dir/

# With custom delimiter (CSV only)
python src/tui/lazy_frame_viewer.py --file data.csv --delimiter ";"

# With custom encoding
python src/tui/lazy_frame_viewer.py --file data.csv --encoding "latin-1"
```

**Commands:**

- `n` / `↓` - Next line
- `p` / `↑` - Previous line
- `s` - Skip N lines
- `j` - Jump to specific line
- `f` - Find value in column
- `r` - Extract matching records
- `k` - Change search column
- `c` - Change context (lines above/below)
- `h` - Show help
- `q` - Quit


### 2. Data Inspector (`data_inspector.py`)

Quick inspection tool for CSV and Parquet schema and sample data.

**Features:**

- Auto-detect delimiter and encoding (CSV)
- Show inferred column types
- Display sample rows
- Save schema to JSON
- Web scraper-friendly interface

**Usage:**

```bash
# Interactive mode
python src/tui/data_inspector.py

# Inspect specific CSV or Parquet file
python src/tui/data_inspector.py --file /path/to/data.csv
python src/tui/data_inspector.py --file /path/to/data.parquet

# Check first 10 lines
python src/tui/data_inspector.py --file data.csv --lines 10

# Custom delimiter (CSV only)
python src/tui/data_inspector.py --file data.csv --delimiter ";"
```

**Commands:**

- `1` - View schema (data types)
- `2` - View sample data
- `3` - Save schema to JSON
- `4` - Reload file
- `q` - Quit


## Installation

```bash
# Create virtual environment with uv
uv venv

# Activate virtualenv
# On Windows:
.venv\Scripts\activate
# On Linux/Mac:
source .venv/bin/activate

# Install dependencies
uv pip install -r requirements.txt
```


## Performance Characteristics

- **Single File Mode**: Handles up to 100M rows with 8GB memory
- **Directory Mode**: Lazy loading - indexing is O(n_files), navigation is O(context)
- **Memory Usage**: Minimal during navigation (only display context)
- **Encoding Support**: UTF-8, Latin-1, ISO-8859-1, and more


## Project Structure

```text
visualizer-tuis-project/
├── src/tui/               # TUI executables
│   ├── lazy_frame_viewer.py
│   └── data_inspector.py
├── requirements.txt       # Python dependencies
├── README.md             # This file
└── examples/             # Example usage scripts
    ├── view_csv.sh       # Quick start script
    └── inspect_csv.sh
```


## Examples

### Example 1: View a large CSV file

```bash
python src/tui/lazy_frame_viewer.py --file ~/datasets/sales_data.csv
```

### Example 2: Load directory of sharded CSVs

```bash
python src/tui/lazy_frame_viewer.py --dir-as-file ~/datasets/raw_data/
```

### Example 3: Inspect CSV schema

```bash
python src/tui/data_inspector.py --file ~/datasets/config.csv
```

### Example 4: Find specific record

```bash
# Open viewer
python src/tui/lazy_frame_viewer.py --file data.csv --delimiter ";"

# Use 'f' command to search
# Type 'john@example.com' to find matching records
```


## Troubleshooting

### Issue: "Delimiter not detected"

**Solution:** Specify manually with `--delimiter` flag

```bash
python src/tui/lazy_frame_viewer.py --file data.csv --delimiter "|"
```

### Issue: "Encoding error"

**Solution:** Try different encoding

```bash
python src/tui/lazy_frame_viewer.py --file data.csv --encoding "latin-1"
```

### Issue: "Out of memory"

**Solution:** Use directory mode for large files

```bash
python src/tui/lazy_frame_viewer.py --dir-as-file /path/to/sharded_csvs/
```


## Architecture

Both tools follow these optimization principles:

1. **Lazy Evaluation**: DataFrames are not materialized until needed
2. **Streaming**: Polars' streaming engine processes data in chunks
3. **Minimal Memory**: Only context rows are loaded at any time
4. **Efficient Search**: Vectorized filter expressions instead of Python loops


## Dependencies

- **polars**: Fast DataFrame library with streaming support
- **rich**: Beautiful terminal UI with colors and tables


## License

GCP Project - 2026

---

For issues or feature requests, please refer to the main GCP project.
