#!/usr/bin/env python3
"""
Visualizador Interativo de Linhas CSV com Polars LazyFrame.
============================================================

Este script fornece uma TUI (Text User Interface) interativa para navegar
e visualizar linhas de arquivos CSV muito grandes (70M+ linhas) usando
Polars com otimizações de memória.

PROPÓSITO:
----------
- Visualizar linhas individuais de CSVs grandes sem carregar tudo na memória
- Navegar por múltiplos arquivos CSV como se fossem um único arquivo
- Buscar valores específicos em colunas
- Extrair relatórios parciais baseados em filtros

MODOS DE OPERAÇÃO:
------------------
1. **Modo Arquivo Único**: Para CSVs menores (< 10M linhas)
   - Carrega o DataFrame na memória com streaming
   - Acesso rápido a qualquer linha

2. **Modo Diretório (LAZY)**: Para múltiplos CSVs ou arquivos muito grandes
   - NÃO carrega dados na memória
   - Apenas indexa as fronteiras dos arquivos (start_row, end_row)
   - Carrega linhas sob demanda quando o usuário navega

EXEMPLOS DE USO:
----------------
# Abrir navegador de diretórios
python lazy_frame_viewer.py

# Abrir arquivo específico
python lazy_frame_viewer.py --file /path/to/file.csv

# Abrir diretório com múltiplos CSVs como arquivo único
python lazy_frame_viewer.py --dir-as-file /path/to/csv_dir/

# Especificar delimitador e encoding
python lazy_frame_viewer.py --file data.csv --delimiter ";" --encoding "latin-1"

AUTOR: GCP Project Team
VERSÃO: 2.0.0 (Memory-Optimized)
"""

import sys
from pathlib import Path
from typing import Optional
import argparse
import subprocess

import polars as pl
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

console = Console()

# ── String constants ──────────────────────────────────────────────────────────
STYLE_HEADER = "bold magenta"
MSG_PRESS_ENTER = "[yellow]Pressione Enter[/yellow]"
MSG_INVALID_OPTION = "[red]Opção inválida[/red]"
MSG_EXITING = "[yellow]Saindo...[/yellow]"
MSG_INVALID_NUMBER = "[red]Erro: Digite um número válido[/red]"
GLOB_CSV = "*.csv"
GLOB_PARQUET = "*.parquet"
PARQUET_EXTENSIONS = {'.parquet', '.pq'}


def _navigate_to_pasted_path(current: Path) -> Path:
    """Prompt user for a path and navigate to it."""
    pasted_path = Prompt.ask("[cyan]Cole ou digite o caminho completo[/cyan]").strip()
    if pasted_path:
        new_path = Path(pasted_path)
        if new_path.exists() and new_path.is_dir():
            console.print(f"[green]✓ Navegando para: {new_path}[/green]")
            Prompt.ask(MSG_PRESS_ENTER)
            return new_path
        else:
            console.print("[red]Erro: Caminho não encontrado ou não é um diretório[/red]")
            Prompt.ask(MSG_PRESS_ENTER)
    return current


def _handle_directory_choice(choice: str, items: list, current: Path) -> Path:
    """Handle a single choice from directory_picker, returning updated path."""
    if choice == "0":
        return current.parent
    if choice.isdigit() and 1 <= int(choice) <= len(items):
        return items[int(choice) - 1]
    target = current / choice
    if target.exists() and target.is_dir():
        return target
    console.print(MSG_INVALID_OPTION)
    Prompt.ask(MSG_PRESS_ENTER)
    return current


def directory_picker(start_dir: Optional[str] = None) -> str:
    """Interactive directory picker with navigation."""
    current = Path(start_dir) if start_dir else Path.home()
    if not current.exists():
        current = Path(".")
    
    while True:
        console.clear()
        console.print(Panel(f"[bold cyan]Diretório atual: {current}[/]", expand=False))
        
        items = sorted([p for p in current.iterdir() if p.is_dir()])
        table = Table(show_header=True, header_style=STYLE_HEADER)
        table.add_column("#", style="dim")
        table.add_column("Pasta")
        
        table.add_row("0", "[yellow].. (diretório pai)[/]")
        table.add_row("s", "[green]✓ Selecionar este diretório[/]")
        table.add_row("c", "[cyan]📋 Colar caminho completo[/]")
        
        for i, item in enumerate(items, 1):
            table.add_row(str(i), item.name)
        
        console.print(table)
        choice = Prompt.ask("[yellow]Opção (número/s/0/c)[/yellow]")
        
        if choice.lower() == "s":
            return str(current)
        elif choice.lower() == "c":
            current = _navigate_to_pasted_path(current)
        else:
            current = _handle_directory_choice(choice, items, current)


def _build_file_picker_table(show_files: bool, subdirs: list, data_files: list) -> tuple:
    """Build the navigation table for file_picker, returning (table, dir_indices, file_indices)."""
    table = Table(show_header=True, header_style=STYLE_HEADER)
    table.add_column("#", style="dim")
    table.add_column("Tipo", style="dim")
    table.add_column("Nome")
    table.add_column("Tamanho", justify="right")

    table.add_row("0", "[yellow]📁[/yellow]", "[yellow].. (diretório pai)[/]", "")
    table.add_row("t", "[cyan]🔄[/cyan]", "[cyan]Alternar visualização de arquivos[/cyan]", "")
    table.add_row("d", "[green]📂[/green]", "[green]Abrir todos os arquivos deste diretório[/green]", "")
    table.add_row("q", "[red]🚪[/red]", "[red]Sair[/red]", "")

    idx = 1
    dir_indices = {}
    for subdir in subdirs:
        table.add_row(str(idx), "📁", subdir.name, "[dim]pasta[/dim]")
        dir_indices[idx] = subdir
        idx += 1

    file_indices = {}
    if show_files:
        for data_file in data_files:
            size_mb = data_file.stat().st_size / (1024 * 1024)
            file_type = "📊" if data_file.suffix == ".parquet" else "📄"
            table.add_row(str(idx), file_type, data_file.name, f"{size_mb:.2f} MB")
            file_indices[idx] = data_file
            idx += 1

    return table, dir_indices, file_indices


def _handle_open_directory(dir_path: Path):
    """Handle 'd' command – open all data files in directory."""
    all_files = list(dir_path.glob(GLOB_CSV)) + list(dir_path.glob(GLOB_PARQUET))
    if not all_files:
        console.print("[red]Nenhum arquivo de dados (CSV/Parquet) neste diretório[/red]")
        Prompt.ask(MSG_PRESS_ENTER)
        return True  # continue loop
    console.print(f"[cyan]Abrindo {len(all_files)} arquivo(s) em novo terminal...[/cyan]")
    spawn_viewer_terminal(str(dir_path), is_directory=True)
    return Prompt.ask("[yellow]Deseja abrir outro arquivo/diretório? (s/n)[/yellow]").lower() == "s"


def _handle_open_file(file_path: Path):
    """Handle opening a single selected file."""
    console.print(f"[cyan]Abrindo {file_path.name} em novo terminal...[/cyan]")
    spawn_viewer_terminal(str(file_path))
    return Prompt.ask("[yellow]Deseja abrir outro arquivo? (s/n)[/yellow]").lower() == "s"


def _handle_numeric_choice(choice_num: int, dir_indices: dict, file_indices: dict) -> Optional[Path]:
    """Handle numeric choice in file_picker, returning new dir_path or None."""
    if choice_num in dir_indices:
        return dir_indices[choice_num]
    if choice_num in file_indices:
        if not _handle_open_file(file_indices[choice_num]):
            console.print(MSG_EXITING)
            sys.exit(0)
        return None
    console.print(MSG_INVALID_OPTION)
    Prompt.ask(MSG_PRESS_ENTER)
    return None


def _file_picker_render(dir_path: Path, show_files: bool) -> tuple:
    """Render the file picker UI and return (dir_indices, file_indices)."""
    console.clear()
    console.print(Panel(f"[bold cyan]Navegando: {dir_path}[/]", expand=False))

    toggle_status = "[green]✓[/green]" if show_files else "[red]✗[/red]"
    console.print(f"Mostrar arquivos (CSV/Parquet): {toggle_status}  (pressione 't' para alternar)\n")

    data_files = sorted(
        list(dir_path.glob(GLOB_CSV)) + list(dir_path.glob(GLOB_PARQUET))
    ) if show_files else []
    subdirs = sorted([p for p in dir_path.iterdir() if p.is_dir()])

    table, dir_indices, file_indices = _build_file_picker_table(
        show_files, subdirs, data_files
    )
    console.print(table)

    all_data_files = list(dir_path.glob(GLOB_CSV)) + list(dir_path.glob(GLOB_PARQUET))
    if all_data_files:
        total_size = sum(f.stat().st_size for f in all_data_files) / (1024 * 1024)
        console.print(f"\n[dim]Arquivos de dados neste diretório: {len(all_data_files)} arquivos ({total_size:.2f} MB total)[/dim]")

    return dir_indices, file_indices


def _file_picker_dispatch(choice: str, dir_path: Path, show_files: bool,
                          dir_indices: dict, file_indices: dict) -> tuple:
    """Process a choice in file_picker. Returns (new_dir_path, new_show_files)."""
    lc = choice.lower()
    if lc == "q":
        console.print(MSG_EXITING)
        sys.exit(0)
    if lc == "t":
        return dir_path, not show_files
    if lc == "d":
        if not _handle_open_directory(dir_path):
            console.print(MSG_EXITING)
            sys.exit(0)
        return dir_path, show_files
    if choice == "0":
        return dir_path.parent, show_files
    if choice.isdigit():
        new_dir = _handle_numeric_choice(int(choice), dir_indices, file_indices)
        return (new_dir or dir_path), show_files
    console.print(MSG_INVALID_OPTION)
    Prompt.ask(MSG_PRESS_ENTER)
    return dir_path, show_files


def file_picker(directory: str) -> tuple:
    """Interactive file picker for CSV and Parquet files."""
    dir_path = Path(directory)
    show_files = False
    
    while True:
        dir_indices, file_indices = _file_picker_render(dir_path, show_files)
        choice = Prompt.ask("[yellow]Opção[/yellow]")
        dir_path, show_files = _file_picker_dispatch(
            choice, dir_path, show_files, dir_indices, file_indices
        )


def spawn_viewer_terminal(file_path: str, is_directory: bool = False):
    """Spawn a new terminal with the viewer running."""
    script_path = Path(__file__).resolve()
    
    try:
        python_exe = sys.executable
        
        if sys.platform == "win32":
            if is_directory:
                cmd = f'start cmd /k "{python_exe} {script_path} --dir-as-file \"{file_path}\""'
            else:
                cmd = f'start cmd /k "{python_exe} {script_path} --file \"{file_path}\""'
            subprocess.Popen(cmd, shell=True)
        else:
            if is_directory:
                cmd = [python_exe, str(script_path), "--dir-as-file", file_path]
            else:
                cmd = [python_exe, str(script_path), "--file", file_path]
            subprocess.Popen(cmd)
    except Exception as e:
        console.print(f"[red]Erro ao abrir novo terminal: {e}[/red]")
        Prompt.ask(MSG_PRESS_ENTER)


class LazyFrameViewer:
    def __init__(self, file_path: str, delimiter: Optional[str] = None, 
                 encoding: str = "utf-8", infer_schema_length: int = 10000,
                 context_lines: int = 2, is_directory: bool = False):
        """Initialize the viewer with a file path or directory."""
        self.file_path = Path(file_path)
        self.is_directory = is_directory
        self.console = Console()
        self.current_row_idx = 0
        self.total_rows: int = 0
        self.df: Optional[pl.DataFrame] = None
        self.columns: list[str] = []
        self.column_types: dict[str, str] = {}
        self.delimiter = delimiter
        self.encoding = encoding
        self.infer_schema_length = infer_schema_length
        self.context_lines = context_lines
        self.search_column: Optional[str] = None
        self.visible_columns: Optional[list[str]] = None
        self.source_files = []
        self.file_boundaries = []
        self.file_type = None
        
        self._load_file()
    
    def _detect_file_type(self) -> str:
        """Detect file type based on extension."""
        suffix = self.file_path.suffix.lower()
        if suffix == '.csv':
            return 'csv'
        elif suffix in PARQUET_EXTENSIONS:
            return 'parquet'
        else:
            raise ValueError(f"Formato de arquivo não suportado: {suffix}")
    
    def _detect_delimiter(self) -> str:
        """Auto-detect CSV delimiter by reading first line."""
        try:
            with open(self.file_path, 'r', encoding=self.encoding) as f:
                first_line = f.readline().strip()
            
            delimiters = [';', ',', '\t', '|']
            for delim in delimiters:
                if delim in first_line:
                    count = first_line.count(delim)
                    num_fields = count + 1
                    self.console.print(f"[cyan]Delimitador detectado: '{delim}' ({num_fields} campos)[/cyan]")
                    return delim
            
            self.console.print("[yellow]Não foi possível detectar delimitador, usando vírgula[/yellow]")
            return ','
        except Exception as e:
            self.console.print(f"[yellow]Erro ao detectar delimitador: {e}, usando vírgula[/yellow]")
            return ','
    
    def _load_file(self):
        """Load file or directory using lazy evaluation."""
        if not self.file_path.exists():
            self.console.print(f"[red]Erro: Caminho não encontrado: {self.file_path}[/red]")
            sys.exit(1)
        
        if self.is_directory:
            self._load_directory()
            return
        
        # Detect file type
        if not self.is_directory:
            try:
                self.file_type = self._detect_file_type()
            except ValueError as e:
                self.console.print(f"[red]Erro: {e}[/red]")
                sys.exit(1)
        
        self._load_single_file()
    
    def _load_single_file(self):
        """Load a single CSV or Parquet file with streaming."""
        try:
            if self.file_type == 'parquet':
                self._load_single_parquet()
            elif self.file_type == 'csv':
                self._load_single_csv()
            
            if self.total_rows > 10_000_000:
                self.console.print(
                    "[yellow]⚠ Arquivo muito grande! Considere usar --dir-as-file para loading lazy.[/yellow]"
                )
                
        except Exception as e:
            self.console.print(f"[red]Erro ao carregar arquivo: {e}[/red]")
            sys.exit(1)

    def _load_single_parquet(self):
        """Load a single Parquet file."""
        lf = pl.scan_parquet(str(self.file_path))
        schema = lf.collect_schema()
        self.columns = schema.names()
        self.column_types = {name: str(dtype) for name, dtype in schema.items()}
        self.search_column = self.columns[0]
        self._column_indices = {col: i for i, col in enumerate(self.columns)}
        self.df = lf.collect()
        self.total_rows = len(self.df)
        self.console.print(
            f"[green]✓[/green] Arquivo Parquet carregado: {self.file_path.name}"
        )
        self.console.print(
            f"[cyan]Linhas: {self.total_rows} (índices 0-{self.total_rows - 1}) | Colunas: {len(self.columns)}[/cyan]"
        )

    def _load_single_csv(self):
        """Load a single CSV file with streaming."""
        if self.delimiter is None:
            self.delimiter = self._detect_delimiter()
        polars_encoding: str = self.encoding.lower().replace('-', '').replace('_', '')
        if polars_encoding not in ['utf8', 'utf8lossy']:
            polars_encoding = 'utf8'
        self.polars_encoding = polars_encoding
        lf = pl.scan_csv(
            str(self.file_path),
            separator=self.delimiter or ',',
            encoding=polars_encoding,  # type: ignore[arg-type]
            infer_schema_length=self.infer_schema_length,
            ignore_errors=True
        )
        schema = lf.collect_schema()
        self.columns = schema.names()
        self.column_types = {name: str(dtype) for name, dtype in schema.items()}
        self.search_column = self.columns[0]
        self._column_indices = {col: i for i, col in enumerate(self.columns)}
        self.df = lf.collect()
        self.total_rows = len(self.df)
        self.console.print(
            f"[green]✓[/green] Arquivo CSV carregado: {self.file_path.name}"
        )
        self.console.print(
            f"[cyan]Linhas: {self.total_rows} (índices 0-{self.total_rows - 1}) | Colunas: {len(self.columns)}[/cyan]"
        )
    
    def _load_directory(self):
        """Index all CSV and Parquet files from directory - LAZY loading."""
        csv_files = sorted(self.file_path.glob(GLOB_CSV))
        parquet_files = sorted(self.file_path.glob(GLOB_PARQUET)) + sorted(self.file_path.glob("*.pq"))
        data_files = sorted(csv_files + parquet_files)
        
        if not data_files:
            self.console.print(f"[red]Erro: Nenhum arquivo CSV ou Parquet no diretório: {self.file_path}[/red]")
            sys.exit(1)
        
        self.source_files = data_files
        self.console.print(f"[cyan]Encontrados {len(data_files)} arquivo(s) (CSV: {len(csv_files)}, Parquet: {len(parquet_files)})[/cyan]")
        
        self.file_type = 'csv' if csv_files else 'parquet'
        self._detect_directory_delimiter(csv_files)
        self._prepare_encoding()
        
        try:
            self._init_schema_from_first_file(data_files)
            self._index_file_boundaries(data_files)
            self._print_directory_summary(data_files)
        except Exception as e:
            self.console.print(f"[red]Erro ao carregar diretório: {e}[/red]")
            sys.exit(1)
    
    def _detect_directory_delimiter(self, csv_files: list):
        """Auto-detect delimiter from first CSV file in directory."""
        if not csv_files or self.delimiter is not None:
            return
        first_csv = csv_files[0]
        try:
            with open(first_csv, 'r', encoding=self.encoding) as f:
                first_line = f.readline().strip()
            for delim in [';', ',', '\t', '|']:
                if delim in first_line:
                    count = first_line.count(delim)
                    self.console.print(f"[cyan]Delimitador detectado: '{delim}' ({count + 1} campos)[/cyan]")
                    self.delimiter = delim
                    return
            self.delimiter = ','
        except Exception:
            self.delimiter = ','
    
    def _prepare_encoding(self):
        """Normalize encoding string for Polars."""
        self.polars_encoding = self.encoding.lower().replace('-', '').replace('_', '')
        if self.polars_encoding not in ['utf8', 'utf8lossy']:
            self.polars_encoding = 'utf8'
    
    def _init_schema_from_first_file(self, data_files: list):
        """Read schema from the first data file."""
        first_file = data_files[0]
        if first_file.suffix.lower() in PARQUET_EXTENSIONS:
            first_lazy = pl.scan_parquet(str(first_file))
        else:
            first_lazy = pl.scan_csv(
                str(first_file),
                separator=self.delimiter or ',',
                encoding=self.polars_encoding,  # type: ignore[arg-type]
                infer_schema_length=self.infer_schema_length,
                ignore_errors=True
            )
        schema = first_lazy.collect_schema()
        self.columns = schema.names()
        self.column_types = {name: str(dtype) for name, dtype in schema.items()}
        self.search_column = self.columns[0]
        self._column_indices = {col: i for i, col in enumerate(self.columns)}
    
    def _count_file_rows(self, data_file: Path) -> int:
        """Count rows in a single data file."""
        if data_file.suffix.lower() in PARQUET_EXTENSIONS:
            try:
                import pyarrow.parquet as pq
                return pq.read_table(str(data_file)).num_rows
            except Exception:
                return 0
        # CSV: count newlines minus header
        with open(data_file, 'rb') as f:
            line_count = 0
            buf_size = 1024 * 1024
            buf = f.raw.read(buf_size)
            while buf:
                line_count += buf.count(b'\n')
                buf = f.raw.read(buf_size)
        return max(line_count - 1, 0)
    
    def _index_file_boundaries(self, data_files: list):
        """Build cumulative row index for all files."""
        self.file_boundaries = []
        cumulative_rows = 0
        self.console.print("[cyan]Contando linhas...[/cyan]")
        for i, data_file in enumerate(data_files):
            if (i + 1) % 10 == 0 or i == 0:
                self.console.print(f"[dim]Indexando ({i+1}/{len(data_files)})...[/dim]", end="\r")
            file_row_count = self._count_file_rows(data_file)
            start_row = cumulative_rows
            end_row = cumulative_rows + file_row_count - 1 if file_row_count > 0 else cumulative_rows - 1
            self.file_boundaries.append((start_row, end_row, data_file))
            cumulative_rows += file_row_count
        self.console.print(" " * 80, end="\r")
        self.total_rows = cumulative_rows
        self.df = None
    
    def _print_directory_summary(self, data_files: list):
        """Print summary after directory indexing."""
        self.console.print(
            f"[green]✓[/green] Diretório indexado: {self.file_path.name}"
        )
        self.console.print(
            f"[cyan]Arquivos: {len(data_files)} | Linhas totais: {self.total_rows} | Colunas: {len(self.columns)}[/cyan]"
        )
        self.console.print("[green]Modo LAZY: dados carregados sob demanda[/green]")
    
    def _build_lazy_frame(self) -> pl.LazyFrame:
        """Reconstruct a LazyFrame from the original source file(s)."""
        if self.is_directory:
            frames: list[pl.LazyFrame] = []
            for _, _, fpath in self.file_boundaries:
                if fpath.suffix.lower() in PARQUET_EXTENSIONS:
                    frames.append(pl.scan_parquet(str(fpath)))
                else:
                    frames.append(pl.scan_csv(
                        str(fpath),
                        separator=self.delimiter or ',',
                        encoding=self.polars_encoding,  # type: ignore[arg-type]
                        infer_schema_length=self.infer_schema_length,
                        ignore_errors=True
                    ))
            return pl.concat(frames)
        if self.file_type == 'parquet':
            return pl.scan_parquet(str(self.file_path))
        return pl.scan_csv(
            str(self.file_path),
            separator=self.delimiter or ',',
            encoding=self.polars_encoding,  # type: ignore[arg-type]
            infer_schema_length=self.infer_schema_length,
            ignore_errors=True
        )

    def _save_to_csv(self):
        """Save current data (with visible columns) to CSV via lazy sink."""
        cols = self.visible_columns if self.visible_columns else self.columns
        default_name = self.file_path.stem + "_export.csv"
        default_path = str(self.file_path.parent / default_name)
        self.console.print(
            f"\n[bold cyan]Exportar para CSV[/bold cyan] "
            f"({len(cols)} colunas, {self.total_rows} linhas)"
        )
        out_path = self.console.input(
            f"[yellow]Caminho de saída [{default_path}]: [/yellow]"
        ).strip()
        if not out_path:
            out_path = default_path

        try:
            lf = self._build_lazy_frame().select(cols)
            self.console.print("[cyan]Salvando via lazy sink...[/cyan]")
            lf.sink_csv(out_path)
            self.console.print(f"[green]\u2713 Salvo: {out_path}[/green]")
        except Exception as e:
            self.console.print(f"[red]Erro ao salvar: {e}[/red]")

    def _parse_index_range(self, left: str, right: str) -> list[str]:
        """Parse a numeric index range like '1-10' (1-based)."""
        start_i = max(1, int(left))
        end_i = min(len(self.columns), int(right))
        return self.columns[start_i - 1:end_i]

    def _parse_name_range(self, left: str, right: str) -> list[str]:
        """Parse a column name range like 'TIPO-90'."""
        if left not in self.columns or right not in self.columns:
            missing = left if left not in self.columns else right
            self.console.print(f"[red]Coluna não encontrada: {missing}[/red]")
            return []
        si = self.columns.index(left)
        ei = self.columns.index(right)
        lo, hi = min(si, ei), max(si, ei)
        return self.columns[lo:hi + 1]

    def _parse_range_part(self, part: str) -> list[str]:
        """Parse a single range part like '1-10' or 'TIPO-90'."""
        left, right = part.split('-', 1)
        left, right = left.strip(), right.strip()
        if left.isdigit() and right.isdigit():
            return self._parse_index_range(left, right)
        return self._parse_name_range(left, right)

    def _parse_single_part(self, part: str) -> list[str]:
        """Parse a single column spec (index or name)."""
        if part.isdigit():
            idx = int(part)
            if 1 <= idx <= len(self.columns):
                return [self.columns[idx - 1]]
        elif part in self.columns:
            return [part]
        else:
            self.console.print(f"[red]Coluna não encontrada: {part}[/red]")
        return []

    def _parse_column_spec(self, spec: str) -> list[str]:
        """Parse a column specification string into a list of column names.

        Supported formats:
          - Range by index: '1-10' (1-based)
          - Range by name:  'TIPO-90'
          - Comma-separated mix: 'TIPO,1-45,AGENCIA'
        """
        result: list[str] = []
        for part in spec.split(','):
            part = part.strip()
            if not part:
                continue
            if '-' in part:
                result.extend(self._parse_range_part(part))
            else:
                result.extend(self._parse_single_part(part))
        return result

    def _set_column_range(self):
        """Interactive column range selector."""
        total = len(self.columns)
        showing = len(self.visible_columns) if self.visible_columns else total
        self.console.print(f"\n[bold cyan]Colunas ({showing}/{total} visíveis):[/bold cyan]")
        # Show a compact numbered list of all columns
        cols_per_row = 5
        for i in range(0, total, cols_per_row):
            parts = []
            for j in range(i, min(i + cols_per_row, total)):
                col = self.columns[j]
                marker = "[green]✓[/green]" if (self.visible_columns is None or col in self.visible_columns) else " "
                parts.append(f"{marker} [dim]{j+1:>3}[/dim] {col}")
            self.console.print("  ".join(parts))

        self.console.print(
            "\n[yellow]Formato: 1-10, TIPO,1-45, TIPO-90, ou 'all' para todas[/yellow]"
        )
        spec = self.console.input("[yellow]Colunas: [/yellow]").strip()

        if spec.lower() in ('all', 'todos', 'todas', '*', ''):
            self.visible_columns = None
            self.console.print(f"[green]✓ Mostrando todas as {total} colunas[/green]")
        else:
            parsed = self._parse_column_spec(spec)
            if parsed:
                self.visible_columns = parsed
                self.console.print(
                    f"[green]✓ Mostrando {len(parsed)} colunas: "
                    f"{parsed[0]} ... {parsed[-1]}[/green]"
                )
            else:
                self.console.print("[red]Nenhuma coluna válida encontrada[/red]")
        self._display_row(self.current_row_idx)

    def _display_row(self, row_idx: int):
        """Display a specific row as a formatted table."""
        if row_idx < 0 or row_idx >= self.total_rows:
            self.console.print(f"[red]Erro: Índice {row_idx} fora do intervalo (0-{self.total_rows - 1})[/red]")
            return
        
        start_idx = max(0, row_idx - self.context_lines)
        end_idx = min(self.total_rows - 1, row_idx + self.context_lines)
        
        title = f"Linha #{row_idx} de {self.total_rows}"
        
        table = Table(
            title=title,
            show_header=True, 
            header_style=STYLE_HEADER,
            border_style="cyan", 
            padding=(0, 1)
        )
        
        table.add_column("#", style="dim", width=8)
        display_cols = self.visible_columns if self.visible_columns else self.columns
        for col in display_cols:
            table.add_column(col, overflow="fold")
        
        for idx in range(start_idx, end_idx + 1):
            if idx == row_idx:
                line_num = f"[bold yellow]→ {idx}[/bold yellow]"
            else:
                line_num = str(idx)
            
            if self.df is not None and idx < len(self.df):
                row_values = [line_num] + [str(self.df[col][idx]) if self.df[col][idx] is not None else "[gray]NULL[/gray]" for col in display_cols]
                table.add_row(*row_values)
        
        self.console.print(table)
        self.current_row_idx = row_idx
    
    def _show_help(self):
        """Display help menu."""
        cols_total = len(self.columns)
        cols_showing = len(self.visible_columns) if self.visible_columns else cols_total
        help_text = f"""
[bold cyan]Comandos:[/bold cyan]
  [yellow]n[/yellow] ou [yellow]↓[/yellow]     - Próxima linha
  [yellow]p[/yellow] ou [yellow]↑[/yellow]     - Linha anterior
  [yellow]s[/yellow]           - Pular N linhas
  [yellow]j[/yellow]           - Pular para linha específica
  [yellow]f[/yellow]           - Buscar por valor
  [yellow]k[/yellow]           - Mudar coluna de busca (atual: {self.search_column})
  [yellow]r[/yellow]           - Selecionar intervalo de colunas ({cols_showing}/{cols_total} visíveis)
  [yellow]w[/yellow]           - Salvar para CSV (colunas visíveis, lazy sink)
  [yellow]c[/yellow]           - Mudar linhas de contexto
  [yellow]h[/yellow]           - Mostrar ajuda
  [yellow]q[/yellow] ou [yellow]Ctrl+C[/yellow] - Sair

[bold cyan]Contexto:[/bold cyan] {self.context_lines} linhas acima/abaixo
"""
        self.console.print(Panel(help_text, title="[bold]Ajuda[/bold]", border_style="green"))
    
    def _skip_lines(self, direction: str):
        """Skip n lines."""
        try:
            count = int(self.console.input("[yellow]Quantas linhas pular? [/yellow]"))
            offset = count if direction.lower() == 'f' else -count
            self._display_row(self.current_row_idx + offset)
        except ValueError:
            self.console.print(MSG_INVALID_NUMBER)
    
    def _jump_to_line(self):
        """Jump to a specific line."""
        try:
            line_num = int(self.console.input("[yellow]Número da linha: [/yellow]"))
            self._display_row(line_num)
        except ValueError:
            self.console.print(MSG_INVALID_NUMBER)
    
    def _change_context(self):
        """Change context lines."""
        try:
            new_context = int(self.console.input(
                f"[yellow]Linhas acima/abaixo (atual: {self.context_lines}): [/yellow]"
            ))
            if new_context >= 0:
                self.context_lines = new_context
                self._display_row(self.current_row_idx)
        except ValueError:
            self.console.print(MSG_INVALID_NUMBER)
    
    def _change_search_column(self):
        """Change search column."""
        self.console.print("\n[bold cyan]Colunas disponíveis:[/bold cyan]")
        
        table = Table(show_header=True, header_style=STYLE_HEADER)
        table.add_column("#", style="dim")
        table.add_column("Coluna")
        
        for i, col in enumerate(self.columns, 1):
            is_current = "✓" if col == self.search_column else ""
            table.add_row(str(i), f"{col} {is_current}")
        
        self.console.print(table)
        
        try:
            choice = self.console.input("[yellow]Número da coluna: [/yellow]").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(self.columns):
                self.search_column = self.columns[int(choice) - 1]
                self.console.print(f"[green]✓ Coluna alterada para: {self.search_column}[/green]")
        except Exception:
            self.console.print("[red]Erro[/red]")
    
    def _dispatch_command(self, cmd: str):
        """Dispatch a single viewer command."""
        if cmd in ['q', 'quit', 'exit']:
            self.console.print(MSG_EXITING)
            return False
        if cmd in ['n', 'next', 'down']:
            self._display_row(self.current_row_idx + 1)
        elif cmd in ['p', 'prev', 'up']:
            self._display_row(self.current_row_idx - 1)
        elif cmd == 's':
            direction = self.console.input("[yellow]Direção (f=frente/b=atrás)? [/yellow]").strip().lower()
            if direction in ['f', 'frente', 'forward']:
                self._skip_lines('f')
            elif direction in ['b', 'atrás', 'back']:
                self._skip_lines('b')
        elif cmd == 'j':
            self._jump_to_line()
        elif cmd == 'k':
            self._change_search_column()
        elif cmd == 'r':
            self._set_column_range()
        elif cmd == 'w':
            self._save_to_csv()
        elif cmd == 'c':
            self._change_context()
        elif cmd == 'h':
            self._show_help()
        else:
            self.console.print("[red]Comando desconhecido. Digite 'h' para ajuda.[/red]")
        return True

    def run(self):
        """Run the interactive viewer."""
        self.console.clear()
        self.console.print("[bold cyan]═══════════════════════════════════════════[/bold cyan]")
        self.console.print("[bold]Visualizador de Linhas - Polars Lazy Frame[/bold]")
        self.console.print("[bold cyan]═══════════════════════════════════════════[/bold cyan]")
        self.console.print()
        
        self._display_row(0)
        self._show_help()
        
        while True:
            try:
                self.console.print()
                cmd = self.console.input(
                    f"[bold][{self.current_row_idx}/{self.total_rows - 1}][/bold] "
                    "[yellow]Comando (n/p/s/j/h/q): [/yellow]"
                ).strip().lower()
                
                if not self._dispatch_command(cmd):
                    break
            
            except KeyboardInterrupt:
                self.console.print(f"\n{MSG_EXITING}")
                break


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Visualizador interativo de linhas em arquivos CSV"
    )
    parser.add_argument(
        "--file", "-f",
        default=None,
        help="Caminho do arquivo CSV"
    )
    parser.add_argument(
        "--dir-as-file",
        default=None,
        help="Carregar todos CSVs de um diretório"
    )
    parser.add_argument(
        "--delimiter",
        default=None,
        help="Delimitador CSV (padrão: auto-detectar)"
    )
    parser.add_argument(
        "--encoding", "-e",
        default="utf-8",
        help="Encoding do arquivo"
    )
    parser.add_argument(
        "--columns", "-c",
        default=None,
        help="Intervalo de colunas visíveis (ex: 1-10, TIPO,1-45, TIPO-90)"
    )
    
    args = parser.parse_args()
    
    if args.dir_as_file:
        dir_path = Path(args.dir_as_file)
        if not dir_path.exists() or not dir_path.is_dir():
            console.print(f"[red]Erro: Diretório não encontrado: {args.dir_as_file}[/red]")
            sys.exit(1)
        
        viewer = LazyFrameViewer(
            str(dir_path),
            delimiter=args.delimiter,
            encoding=args.encoding,
            is_directory=True
        )
        if args.columns:
            viewer.visible_columns = viewer._parse_column_spec(args.columns)
        viewer.run()
    
    elif args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            console.print(f"[red]Erro: Arquivo não encontrado: {args.file}[/red]")
            sys.exit(1)
        
        viewer = LazyFrameViewer(
            str(file_path),
            delimiter=args.delimiter,
            encoding=args.encoding
        )
        if args.columns:
            viewer.visible_columns = viewer._parse_column_spec(args.columns)
        viewer.run()
    
    else:
        selected_dir = directory_picker()
        if not selected_dir:
            console.print("[red]Nenhum diretório selecionado[/red]")
            sys.exit(1)
        file_picker(selected_dir)


if __name__ == "__main__":
    main()
