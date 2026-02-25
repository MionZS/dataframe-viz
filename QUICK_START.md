# Isolated GCP Visualizer TUIs Project

A standalone project containing clean, memory-optimized terminal UI tools for CSV data visualization and inspection.

## Quick Start

1. **Create venv with uv**

```bash
uv venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
```

1. **Install dependencies**

```bash
uv pip install -r requirements.txt
```

1. **Run tools:**

```bash
# View large CSV or Parquet files with lazy loading
python src/tui/lazy_frame_viewer.py --file data.csv
python src/tui/lazy_frame_viewer.py --file data.parquet

# Inspect CSV/Parquet schema and types
python src/tui/data_inspector.py --file data.csv
python src/tui/data_inspector.py --file data.parquet
```

## What's Included

### Tools

1. **LazyFrame Viewer** - Interactive explorer for CSV and Parquet files (up to 100M+ rows)
   - Memory-optimized lazy loading
   - Works with CSV, Parquet formats
   - Keyword search
   - Export matching records
   - Supports multiple delimiters/encodings

2. **Data Inspector** - Quick schema viewer for CSV and Parquet
   - Auto-detect column types
   - Show sample data
   - Save schema to JSON
   - Type casting

### Project Structure

```text
visualizer-tuis-project/
├── src/tui/             # TUI tools
├── requirements.txt     # Python dependencies
├── README.md            # Full documentation
├── SETUP.md             # Setup instructions
├── QUICK_START.md       # This file
└── examples/            # Helper scripts
```

## Key Features

- ✅ **Memory Efficient**: Lazy loading for huge files
- ✅ **No Dependencies**: Only Polars + Rich (2 packages)
- ✅ **Cross-Platform**: Works on Windows, Linux, Mac
- ✅ **User Friendly**: Interactive menus and navigation
- ✅ **Fast**: Vectorized operations, streaming processing

- Python 3.7+
- 2GB RAM minimum (8GB recommended for 100M+ row files)
- Terminal with UTF-8 support (for rich formatting)

| Operation | Memory | Time |
| --------- | ------ | ---- |
| Index directory | O(n_files) | ~1 sec per 100 files |
| Load single file | Streaming | ~5-30 sec (100M rows) |
| Navigate | O(context) | Instant |
| Search | Lazy filter | Depends on matches |

## Support

For detailed usage, see:

- **README.md** - Full documentation and examples
- **SETUP.md** - Installation and troubleshooting
- Built-in help: Press `h` in the viewer

---

Project created from GCP Comparison Toolchain (2026)
