#!/usr/bin/env python3
"""
Inspetor de Dados CSV e Parquet - Visualiza tipos e primeiras linhas.

Funcionalidades:
  - Suporta CSV e arquivos Parquet
  - Lê primeiras N linhas de um arquivo
  - Mostra tipos inferidos pelo Polars
  - Permite sobrescrever tipos manualmente
  - Salva schema customizado para uso posterior

EXEMPLOS DE USO:
----------------
# Inspecionar arquivo CSV
python data_inspector.py --file /path/to/file.csv

# Inspecionar arquivo Parquet
python data_inspector.py --file /path/to/file.parquet

# Inspecionar com mais linhas
python data_inspector.py --file data.csv --lines 10

# Especificar delimitador (apenas para CSV)
python data_inspector.py --file data.csv --delimiter ";"

AUTOR: GCP Project Team
VERSÃO: 2.0.0 (Suporte a Parquet adicionado)
"""

import sys
from pathlib import Path
from typing import Optional
import argparse
import json
import csv
from datetime import datetime

import polars as pl
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt


console = Console()

# ── String constants ──────────────────────────────────────────────────
STYLE_HEADER = "bold magenta"
MSG_PRESS_ENTER = "[yellow]Pressione Enter[/yellow]"
MSG_INVALID_OPTION = "[red]Opção inválida[/red]"
MSG_EXITING = "[yellow]Saindo...[/yellow]"
PARQUET_EXTENSIONS = {'.parquet', '.pq'}
GLOB_DATA = ["*.csv", "*.parquet", "*.pq"]


def _list_data_files(dir_path: Path) -> list:
    """List all CSV/Parquet files in a directory."""
    result = []
    for pattern in GLOB_DATA:
        result.extend(dir_path.glob(pattern))
    return sorted(result)


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


def _handle_dir_choice(choice: str, items: list, current: Path) -> Path:
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
    """Interactive directory picker."""
    current = Path(start_dir) if start_dir else Path.home()
    if not current.exists():
        current = Path('.')

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
            current = _handle_dir_choice(choice, items, current)


def _build_inspector_table(subdirs: list, data_files: list, show_files: bool) -> tuple:
    """Build navigation table, returning (table, dir_indices, file_indices)."""
    table = Table(show_header=True, header_style=STYLE_HEADER)
    table.add_column("#", style="dim")
    table.add_column("Tipo", style="dim")
    table.add_column("Nome")
    table.add_column("Tamanho", justify="right")

    table.add_row("0", "[yellow]📁[/yellow]", "[yellow].. (diretório pai)[/]", "")
    table.add_row("t", "[cyan]🔄[/cyan]", "[cyan]Alternar visualização de arquivos[/cyan]", "")
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
            file_type = "📊" if data_file.suffix.lower() in PARQUET_EXTENSIONS else "📄"
            table.add_row(str(idx), file_type, data_file.name, f"{size_mb:.2f} MB")
            file_indices[idx] = data_file
            idx += 1

    return table, dir_indices, file_indices


def _dispatch_file_choice(choice: str, dir_path: Path, show_files: bool,
                          dir_indices: dict, file_indices: dict) -> tuple:
    """Process file_picker choice. Returns (new_dir_path, new_show_files, selected_file)."""
    lc = choice.lower()
    if lc == "q":
        console.print(MSG_EXITING)
        sys.exit(0)
    if lc == "t":
        return dir_path, not show_files, None
    if choice == "0":
        return dir_path.parent, show_files, None
    if choice.isdigit():
        choice_num = int(choice)
        if choice_num in dir_indices:
            return dir_indices[choice_num], show_files, None
        if choice_num in file_indices:
            return dir_path, show_files, str(file_indices[choice_num])
        console.print(MSG_INVALID_OPTION)
        Prompt.ask(MSG_PRESS_ENTER)
        return dir_path, show_files, None
    console.print(MSG_INVALID_OPTION)
    Prompt.ask(MSG_PRESS_ENTER)
    return dir_path, show_files, None


def file_picker(directory: str) -> str:
    """Interactive file picker for CSV and Parquet files."""
    dir_path = Path(directory)
    show_files = False

    while True:
        console.clear()
        console.print(Panel(f"[bold cyan]Navegando: {dir_path}[/]", expand=False))

        toggle_status = "[green]✓[/green]" if show_files else "[red]✗[/red]"
        console.print(f"Mostrar arquivos (CSV/Parquet): {toggle_status}  (pressione 't' para alternar)\n")

        data_files = _list_data_files(dir_path) if show_files else []
        subdirs = sorted([p for p in dir_path.iterdir() if p.is_dir()])

        table, dir_indices, file_indices = _build_inspector_table(subdirs, data_files, show_files)
        console.print(table)

        all_data_files = _list_data_files(dir_path)
        if all_data_files:
            total_size = sum(f.stat().st_size for f in all_data_files) / (1024 * 1024)
            console.print(f"\n[dim]Arquivos de dados neste diretório: {len(all_data_files)} arquivos ({total_size:.2f} MB total)[/dim]")

        choice = Prompt.ask("[yellow]Opção[/yellow]")
        dir_path, show_files, selected = _dispatch_file_choice(
            choice, dir_path, show_files, dir_indices, file_indices
        )
        if selected is not None:
            return selected


POLARS_TYPE_MAP = {
    "Int8": "Inteiro (8-bit)",
    "Int16": "Inteiro (16-bit)",
    "Int32": "Inteiro (32-bit)",
    "Int64": "Inteiro (64-bit)",
    "Float32": "Decimal (32-bit)",
    "Float64": "Decimal (64-bit)",
    "Boolean": "Booleano",
    "Utf8": "Texto",
    "String": "Texto",
    "Date": "Data",
    "Datetime": "Data e Hora",
}

AVAILABLE_TYPES = [
    ("1", "String/Utf8", pl.Utf8),
    ("2", "Int64", pl.Int64),
    ("3", "Float64", pl.Float64),
    ("4", "Boolean", pl.Boolean),
    ("5", "Date", pl.Date),
    ("6", "Datetime", pl.Datetime),
]


def detect_delimiter(file_path: Path, encoding: str = "utf-8") -> str:
    """Auto-detect CSV delimiter."""
    try:
        with open(file_path, 'r', encoding=encoding, errors='replace') as f:
            sample = f.read(8192)
            try:
                dialect = csv.Sniffer().sniff(sample)
                return dialect.delimiter
            except Exception:
                delimiters = [';', ',', '\t', '|']
                for delim in delimiters:
                    if delim in sample.split('\n')[0]:
                        return delim
                return ','
    except Exception:
        return ','


def get_type_name(dtype) -> str:
    """Get friendly name for a Polars data type."""
    dtype_str = str(dtype)
    base_type = dtype_str.split('(')[0]
    return POLARS_TYPE_MAP.get(base_type, dtype_str)


class DataInspector:
    def __init__(self, file_path: str, delimiter: Optional[str] = None,
                 encoding: str = "utf-8", num_lines: int = 5):
        self.file_path = Path(file_path)
        self.encoding = encoding
        self.num_lines = num_lines
        self.console = Console()
        
        # Detect file type
        self.file_type = self._detect_file_type()
        
        if self.file_type == 'csv':
            if delimiter is None:
                self.delimiter = detect_delimiter(self.file_path, encoding)
                self.console.print(f"[cyan]Delimitador detectado: '{self.delimiter}'[/cyan]")
            else:
                self.delimiter = delimiter
        else:
            self.delimiter = None
        
        self.polars_encoding = encoding.lower().replace('-', '').replace('_', '')
        if self.polars_encoding not in ['utf8', 'utf8lossy']:
            self.polars_encoding = 'utf8'
        
        self.df: Optional[pl.DataFrame] = None
        self.columns: list[str] = []
        self.dtypes: dict = {}
        self.custom_dtypes = {}
        
        self._load_sample()
    
    def _detect_file_type(self) -> str:
        """Detect file type based on extension."""
        suffix = self.file_path.suffix.lower()
        if suffix == '.csv':
            return 'csv'
        elif suffix in PARQUET_EXTENSIONS:
            return 'parquet'
        else:
            raise ValueError(f"Formato de arquivo não suportado: {suffix}")
    
    def _load_sample(self):
        """Load sample rows from the file."""
        try:
            if self.file_type == 'parquet':
                lf = pl.scan_parquet(str(self.file_path))
                schema = lf.collect_schema()
                self.columns = schema.names()
                self.dtypes = dict(schema.items())
                self.df = lf.head(self.num_lines).collect()
                self.console.print(f"[green]✓[/green] Arquivo Parquet carregado: {self.file_path.name}")
            else:
                lf = pl.scan_csv(
                    str(self.file_path),
                    separator=self.delimiter or ',',
                    encoding=self.polars_encoding,  # type: ignore[arg-type]
                    ignore_errors=True
                )
                schema = lf.collect_schema()
                self.columns = schema.names()
                self.dtypes = dict(schema.items())
                self.df = lf.head(self.num_lines).collect()
                self.console.print(f"[green]✓[/green] Arquivo CSV carregado: {self.file_path.name}")

            self.console.print(f"[cyan]Colunas: {len(self.columns)} | Amostra: {len(self.df)} linhas[/cyan]")
            
        except Exception as e:
            self.console.print(f"[red]Erro ao carregar arquivo: {e}[/red]")
            sys.exit(1)
    
    def show_schema(self):
        """Display the inferred schema."""
        self.console.print("\n")
        
        table = Table(
            title=f"Schema do Arquivo: {self.file_path.name}",
            show_header=True,
            header_style=STYLE_HEADER,
            border_style="cyan"
        )
        
        table.add_column("#", style="dim", width=4)
        table.add_column("Coluna", style="bold")
        table.add_column("Tipo Inferido", style="cyan")
        table.add_column("Tipo Amigável", style="green")
        
        for i, col in enumerate(self.columns, 1):
            dtype = self.dtypes[col]
            dtype_str = str(dtype)
            friendly_name = get_type_name(dtype)
            
            table.add_row(
                str(i),
                col,
                dtype_str,
                friendly_name
            )
        
        self.console.print(table)
    
    def show_sample_data(self):
        """Display sample rows in a table."""
        if self.df is None:
            self.console.print("[red]Nenhum dado carregado.[/red]")
            return
        
        self.console.print("\n")
        
        table = Table(
            title=f"Primeiras {len(self.df)} Linhas",
            show_header=True,
            header_style=STYLE_HEADER,
            border_style="cyan"
        )
        
        table.add_column("#", style="dim", width=4)
        for col in self.columns:
            dtype = self.dtypes[col]
            table.add_column(f"{col}\n[dim]{dtype}[/dim]", overflow="fold")
        
        for row_idx in range(len(self.df)):
            row_values = [str(row_idx + 1)]
            for col in self.columns:
                val = self.df[col][row_idx]
                val_str = str(val) if val is not None else "[gray]NULL[/gray]"
                row_values.append(val_str)
            table.add_row(*row_values)
        
        self.console.print(table)
    
    def save_schema(self):
        """Save the current schema to a JSON file."""
        schema_data = {
            "file": self.file_path.name,
            "delimiter": self.delimiter,
            "encoding": self.encoding,
            "columns": {}
        }
        
        for col in self.columns:
            dtype = self.dtypes[col]
            schema_data["columns"][col] = {
                "type": str(dtype),
                "custom": col in self.custom_dtypes
            }
        
        schema_path = self.file_path.with_suffix('.schema.json')
        
        try:
            with open(schema_path, 'w', encoding='utf-8') as f:
                json.dump(schema_data, f, indent=2, ensure_ascii=False)
            self.console.print(f"[green]✓ Schema salvo em: {schema_path}[/green]")
        except Exception as e:
            self.console.print(f"[red]Erro ao salvar schema: {e}[/red]")
    
    def run(self):
        """Run the interactive inspector."""
        while True:
            self.console.print("\n")
            self.console.print(Panel(
                "[bold cyan]Inspetor de Dados[/bold cyan]\n\n"
                f"Arquivo: {self.file_path.name}\n"
                f"Colunas: {len(self.columns)} | Amostra: {len(self.df) if self.df is not None else 0} linhas",
                expand=False
            ))
            
            menu = Table(show_header=False, box=None)
            menu.add_column("Opção", style="yellow")
            menu.add_column("Descrição")
            
            menu.add_row("1", "Ver schema (tipos das colunas)")
            menu.add_row("2", "Ver dados de amostra")
            menu.add_row("3", "Salvar schema para JSON")
            menu.add_row("4", "Recarregar arquivo")
            menu.add_row("q", "Sair")
            
            self.console.print(menu)
            
            choice = self.console.input("\n[yellow]Opção: [/yellow]").strip().lower()
            
            if choice == "1":
                self.show_schema()
            elif choice == "2":
                self.show_sample_data()
            elif choice == "3":
                self.save_schema()
            elif choice == "4":
                self._load_sample()
                self.console.print("[green]✓ Arquivo recarregado[/green]")
            elif choice in ["q", "quit", "exit"]:
                console.print(MSG_EXITING)
                break
            else:
                console.print(MSG_INVALID_OPTION)


def main():
    parser = argparse.ArgumentParser(
        description="Inspetor de Dados (CSV e Parquet)"
    )
    parser.add_argument(
        "--file", "-f",
        type=str,
        help="Caminho para o arquivo (CSV ou Parquet)"
    )
    parser.add_argument(
        "--delimiter", "-d",
        type=str,
        default=None,
        help="Delimitador do CSV (ignorado para parquet)"
    )
    parser.add_argument(
        "--encoding", "-e",
        type=str,
        default="utf-8",
        help="Encoding do arquivo"
    )
    parser.add_argument(
        "--lines", "-l",
        type=int,
        default=5,
        help="Número de linhas de amostra"
    )
    
    args = parser.parse_args()
    
    if args.file:
        file_path = args.file
    else:
        console.print("[bold cyan]Inspetor de Dados - Explorer[/bold cyan]")
        start_dir = Path.cwd()
        picked_dir = directory_picker(str(start_dir))
        file_path = file_picker(picked_dir)

    p = Path(file_path)
    if not p.exists() or not p.is_file():
        console.print(f"[red]Erro: Arquivo não encontrado: {file_path}[/red]")
        sys.exit(1)
    
    inspector = DataInspector(
        file_path=file_path,
        delimiter=args.delimiter,
        encoding=args.encoding,
        num_lines=args.lines,
    )
    
    inspector.run()


if __name__ == "__main__":
    main()
