# Setup Instructions

## Quick Start (Windows)

1. **Extract the project**
```cmd
tar -xzf visualizer-tuis.zip
cd visualizer-tuis-project
```

2. **Create virtual environment**
```cmd
python -m venv venv
venv\Scripts\activate
```

3. **Install dependencies**
```cmd
pip install -r requirements.txt
```

4. **Run a tool**
```cmd
# View CSV files
python visualizers\lazy_frame_viewer.py

# Or use the batch file
examples\view_csv.bat
```


## Quick Start (Linux/Mac)

1. **Extract the project**
```bash
tar -xzf visualizer-tuis.zip
cd visualizer-tuis-project
```

2. **Create virtual environment**
```bash
python3 -m venv venv
source venv/bin/activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Run a tool**
```bash
# View CSV files
python visualizers/lazy_frame_viewer.py

# Or use the shell script
bash examples/view_csv.sh
```


## Using Different Python Versions

If you have multiple Python versions installed:

```bash
# Use Python 3.9
python3.9 -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```


## Troubleshooting Installation

### Issue: "python: command not found"
- On Mac: Install via Homebrew `brew install python3`
- On Linux: `sudo apt-get install python3`
- On Windows: Download from [python.org](https://www.python.org)

### Issue: "pip install fails"
Try upgrading pip first:
```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Issue: "ModuleNotFoundError: No module named 'polars'"
The virtual environment might not be activated. Run:
```bash
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

Then retry:
```bash
pip install -r requirements.txt
```


## Next Steps

- See **README.md** for tool usage and examples
- Run `python visualizers/lazy_frame_viewer.py --help` for options
- Read the docstrings in the Python files for technical details
