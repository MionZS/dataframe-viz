# Setup Instructions

## Quick Start (Windows)

1. **Extract the project**

```cmd
tar -xzf visualizer-tuis.zip
cd visualizer-tuis-project
```

1. **Create virtual environment with uv**

```cmd
uv venv
.venv\Scripts\activate
```

1. **Install dependencies**

```cmd
uv pip install -r requirements.txt
```

1. **Run a tool**

```cmd
# View CSV or Parquet files
python src/tui/lazy_frame_viewer.py

# Or use the batch file
examples\view_csv.bat
```


## Quick Start (Linux/Mac)

1. **Extract the project**

```bash
tar -xzf visualizer-tuis.zip
cd visualizer-tuis-project
```

1. **Create virtual environment with uv**

```bash
uv venv
source .venv/bin/activate
```

1. **Install dependencies**

```bash
uv pip install -r requirements.txt
```

1. **Run a tool**

```bash
# View CSV or Parquet files
python src/tui/lazy_frame_viewer.py

# Or use the shell script
bash examples/view_csv.sh
```


## Using Different Python Versions

If you have multiple Python versions installed:

```bash
# Use specific Python version with uv
uv venv --python 3.11
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
uv pip install -r requirements.txt
```



## Troubleshooting Installation

### Issue: "uv: command not found"

- Install uv: [uv Getting Started](https://docs.astral.sh/uv/getting-started/installation/)

### Issue: "uv pip install fails"

Try updating uv first:

```bash
uv pip install --upgrade uv
uv pip install -r requirements.txt
```

### Issue: "ModuleNotFoundError: No module named 'polars'"

The virtual environment might not be activated. Run:

```bash
# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate
```

Then retry:

```bash
uv pip install -r requirements.txt
```

### Issue: "pyarrow import error"

Make sure all dependencies are installed:

```bash
uv pip install --force-reinstall -r requirements.txt
```


## Next Steps

- See **README.md** for tool usage and examples
- Run `python src/tui/lazy_frame_viewer.py --help` for options
- Read the docstrings in the Python files for technical details
