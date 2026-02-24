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


def directory_picker(start_dir: str = None) -> str:
    """Interactive directory picker with navigation."""
    if start_dir:
        current = Path(start_dir)
    else:
        current = Path.home()
    
    if not current.exists():
        current = Path(".")
    
    while True:
        console.clear()
        console.print(Panel(f"[bold cyan]Diretório atual: {current}[/]", expand=False))
        
        items = sorted([p for p in current.iterdir() if p.is_dir()])
        table = Table(show_header=True, header_style="bold magenta")
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
            pasted_path = Prompt.ask("[cyan]Cole ou digite o caminho completo[/cyan]").strip()
            if pasted_path:
                new_path = Path(pasted_path)
                if new_path.exists() and new_path.is_dir():
                    current = new_path
                    console.print(f"[green]✓ Navegando para: {current}[/green]")
                    Prompt.ask("[yellow]Pressione Enter[/yellow]")
                else:
                    console.print("[red]Erro: Caminho não encontrado ou não é um diretório[/red]")
                    Prompt.ask("[yellow]Pressione Enter[/yellow]")
        elif choice == "0":
            current = current.parent
        elif choice.isdigit() and 1 <= int(choice) <= len(items):
            current = items[int(choice) - 1]
        else:
            target = current / choice
            if target.exists() and target.is_dir():
                current = target
            else:
                console.print("[red]Opção inválida[/red]")
                Prompt.ask("[yellow]Pressione Enter[/yellow]")


def file_picker(directory: str) -> tuple:
    """Interactive file picker for CSV and Parquet files."""
    dir_path = Path(directory)
    show_files = False
    
    while True:
        console.clear()
        console.print(Panel(f"[bold cyan]Navegando: {dir_path}[/]", expand=False))
        
        toggle_status = "[green]✓[/green]" if show_files else "[red]✗[/red]"
        console.print(f"Mostrar arquivos (CSV/Parquet): {toggle_status}  (pressione 't' para alternar)\n")
        
        data_files = sorted(
            list(dir_path.glob("*.csv")) + list(dir_path.glob("*.parquet"))
        ) if show_files else []
        subdirs = sorted([p for p in dir_path.iterdir() if p.is_dir()])
        
        table = Table(show_header=True, header_style="bold magenta")
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
            for csv_file in csv_files:
                size_mb = csv_file.stat().st_size / (1024 * 1024)
                table.add_row(str(idx), "📄", csv_file.name, f"{size_mb:.2f} MB")
                file_indices[idx] = csv_file
                idx += 1
        
        console.print(table)
        
        all_csvs = list(dir_path.glob("*.csv"))
        if all_csvs:
            total_size = sum(f.stat().st_size for f in all_csvs) / (1024 * 1024)
            console.print(f"\n[dim]CSVs neste diretório: {len(all_csvs)} arquivos ({total_size:.2f} MB total)[/dim]")
        
        choice = Prompt.ask("[yellow]Opção[/yellow]")
        
        if choice.lower() == "q":
            console.print("[yellow]Saindo...[/yellow]")
            sys.exit(0)
        elif choice.lower() == "t":
            show_files = not show_files
        elif choice.lower() == "d":
            all_csvs = list(dir_path.glob("*.csv"))
            if not all_csvs:
                console.print("[red]Nenhum arquivo CSV neste diretório[/red]")
                Prompt.ask("[yellow]Pressione Enter[/yellow]")
            else:
                console.print(f"[cyan]Abrindo {len(all_csvs)} arquivo(s) CSV em novo terminal...[/cyan]")
                spawn_viewer_terminal(str(dir_path), is_directory=True)
                
                continue_choice = Prompt.ask("[yellow]Deseja abrir outro arquivo/diretório? (s/n)[/yellow]")
                if continue_choice.lower() != "s":
                    console.print("[yellow]Saindo...[/yellow]")
                    sys.exit(0)
        elif choice == "0":
            dir_path = dir_path.parent
        elif choice.isdigit():
            choice_num = int(choice)
            if choice_num in dir_indices:
                dir_path = dir_indices[choice_num]
            elif choice_num in file_indices:
                selected_file = str(file_indices[choice_num])
                console.print(f"[cyan]Abrindo {file_indices[choice_num].name} em novo terminal...[/cyan]")
                spawn_viewer_terminal(selected_file)
                
                continue_choice = Prompt.ask("[yellow]Deseja abrir outro arquivo? (s/n)[/yellow]")
                if continue_choice.lower() != "s":
                    console.print("[yellow]Saindo...[/yellow]")
                    sys.exit(0)
            else:
                console.print("[red]Opção inválida[/red]")
                Prompt.ask("[yellow]Pressione Enter[/yellow]")
        else:
            console.print("[red]Opção inválida[/red]")
            Prompt.ask("[yellow]Pressione Enter[/yellow]")


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
        Prompt.ask("[yellow]Pressione Enter[/yellow]")


class LazyFrameViewer:
    def __init__(self, file_path: str, delimiter: Optional[str] = None, 
                 encoding: str = "utf-8", infer_schema_length: int = 10000,
                 context_lines: int = 2, is_directory: bool = False):
        """Initialize the viewer with a file path or directory."""
        self.file_path = Path(file_path)
        self.is_directory = is_directory
        self.console = Console()
        self.current_row_idx = 0
        self.total_rows = None
        self.df = None
        self.columns = None
        self.column_types = {}
        self.delimiter = delimiter
        self.encoding = encoding
        self.infer_schema_length = infer_schema_length
        self.context_lines = context_lines
        self.search_column = None
        self.source_files = []
        self.file_boundaries = []
        
        self._load_file()
    
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
        
        if self.delimiter is None:
            self.delimiter = self._detect_delimiter()
        
        self._load_single_file()
    
    def _load_single_file(self):
        """Load a single CSV file with streaming."""
        polars_encoding = self.encoding.lower().replace('-', '').replace('_', '')
        if polars_encoding not in ['utf8', 'utf8lossy']:
            polars_encoding = 'utf8'
        
        self.polars_encoding = polars_encoding
        
        try:
            lf = pl.scan_csv(
                str(self.file_path),
                separator=self.delimiter,
                encoding=polars_encoding,
                infer_schema_length=self.infer_schema_length,
                ignore_errors=True
            )
            
            schema = lf.collect_schema()
            self.columns = schema.names()
            self.column_types = {name: str(dtype) for name, dtype in schema.items()}
            self.search_column = self.columns[0]
            
            self._column_indices = {col: i for i, col in enumerate(self.columns)}
            
            self.df = lf.collect(streaming=True)
            self.total_rows = len(self.df)
            
            self.console.print(
                f"[green]✓[/green] Arquivo carregado: {self.file_path.name}"
            )
            self.console.print(
                f"[cyan]Linhas: {self.total_rows} (índices 0-{self.total_rows - 1}) | Colunas: {len(self.columns)}[/cyan]"
            )
            
            if self.total_rows > 10_000_000:
                self.console.print(
                    "[yellow]⚠ Arquivo muito grande! Considere usar --dir-as-file para loading lazy.[/yellow]"
                )
                
        except Exception as e:
            self.console.print(f"[red]Erro ao carregar arquivo: {e}[/red]")
            sys.exit(1)
    
    def _load_directory(self):
        """Index all CSV files from directory - LAZY loading."""
        csv_files = sorted(self.file_path.glob("*.csv"))
        
        if not csv_files:
            self.console.print(f"[red]Erro: Nenhum arquivo CSV no diretório: {self.file_path}[/red]")
            sys.exit(1)
        
        self.source_files = csv_files
        self.console.print(f"[cyan]Encontrados {len(csv_files)} arquivo(s) CSV[/cyan]")
        
        if self.delimiter is None:
            first_file = csv_files[0]
            try:
                with open(first_file, 'r', encoding=self.encoding) as f:
                    first_line = f.readline().strip()
                
                delimiters = [';', ',', '\t', '|']
                for delim in delimiters:
                    if delim in first_line:
                        count = first_line.count(delim)
                        num_fields = count + 1
                        self.console.print(f"[cyan]Delimitador detectado: '{delim}' ({num_fields} campos)[/cyan]")
                        self.delimiter = delim
                        break
                else:
                    self.delimiter = ','
            except Exception:
                self.delimiter = ','
        
        self.polars_encoding = self.encoding.lower().replace('-', '').replace('_', '')
        if self.polars_encoding not in ['utf8', 'utf8lossy']:
            self.polars_encoding = 'utf8'
        
        try:
            first_lazy = pl.scan_csv(
                str(csv_files[0]),
                separator=self.delimiter,
                encoding=self.polars_encoding,
                infer_schema_length=self.infer_schema_length,
                ignore_errors=True
            )
            schema = first_lazy.collect_schema()
            self.columns = schema.names()
            self.column_types = {name: str(dtype) for name, dtype in schema.items()}
            self.search_column = self.columns[0]
            
            self._column_indices = {col: i for i, col in enumerate(self.columns)}
            
            self.file_boundaries = []
            cumulative_rows = 0
            
            self.console.print("[cyan]Contando linhas...[/cyan]")
            
            for i, csv_file in enumerate(csv_files):
                if (i + 1) % 10 == 0 or i == 0:
                    self.console.print(f"[dim]Indexando ({i+1}/{len(csv_files)})...[/dim]", end="\r")
                
                with open(csv_file, 'rb') as f:
                    line_count = 0
                    buf_size = 1024 * 1024
                    buf = f.raw.read(buf_size)
                    while buf:
                        line_count += buf.count(b'\n')
                        buf = f.raw.read(buf_size)
                
                if i == 0:
                    file_row_count = line_count - 1
                else:
                    file_row_count = line_count - 1
                
                if file_row_count < 0:
                    file_row_count = 0
                
                start_row = cumulative_rows
                end_row = cumulative_rows + file_row_count - 1 if file_row_count > 0 else cumulative_rows - 1
                self.file_boundaries.append((start_row, end_row, csv_file))
                cumulative_rows += file_row_count
            
            self.console.print(" " * 80, end="\r")
            
            self.total_rows = cumulative_rows
            self.df = None
            
            self.console.print(
                f"[green]✓[/green] Diretório indexado: {self.file_path.name}"
            )
            self.console.print(
                f"[cyan]Arquivos: {len(csv_files)} | Linhas totais: {self.total_rows} | Colunas: {len(self.columns)}[/cyan]"
            )
            self.console.print("[green]Modo LAZY: dados carregados sob demanda[/green]")
                
        except Exception as e:
            self.console.print(f"[red]Erro ao carregar diretório: {e}[/red]")
            sys.exit(1)
    
    def _display_row(self, row_idx: int):
        """Display a specific row as a formatted table."""
        if row_idx < 0 or row_idx >= self.total_rows:
            self.console.print(f"[red]Erro: Índice {row_idx} fora do intervalo (0-{self.total_rows - 1})[/red]")
            return
        
        start_idx = max(0, row_idx - self.context_lines)
        end_idx = min(self.total_rows - 1, row_idx + self.context_lines)
        
        if self.df is not None:
            rows_data = {i: {col: self.df[col][i] for col in self.columns} for i in range(start_idx, end_idx + 1)}
        else:
            rows_data = {}
            for i in range(start_idx, end_idx + 1):
                if i < len(self.df) if self.df else False:
                    rows_data[i] = {col: self.df[col][i] for col in self.columns}
        
        title = f"Linha #{row_idx} de {self.total_rows}"
        
        table = Table(
            title=title,
            show_header=True, 
            header_style="bold magenta",
            border_style="cyan", 
            padding=(0, 1)
        )
        
        table.add_column("#", style="dim", width=8)
        for col in self.columns:
            table.add_column(col, overflow="fold")
        
        for idx in range(start_idx, end_idx + 1):
            if idx == row_idx:
                line_num = f"[bold yellow]→ {idx}[/bold yellow]"
            else:
                line_num = str(idx)
            
            if self.df is not None and idx < len(self.df):
                row_values = [line_num] + [str(self.df[col][idx]) if self.df[col][idx] is not None else "[gray]NULL[/gray]" for col in self.columns]
                table.add_row(*row_values)
        
        self.console.print(table)
        self.current_row_idx = row_idx
    
    def _show_help(self):
        """Display help menu."""
        help_text = f"""
[bold cyan]Comandos:[/bold cyan]
  [yellow]n[/yellow] ou [yellow]↓[/yellow]     - Próxima linha
  [yellow]p[/yellow] ou [yellow]↑[/yellow]     - Linha anterior
  [yellow]s[/yellow]           - Pular N linhas
  [yellow]j[/yellow]           - Pular para linha específica
  [yellow]f[/yellow]           - Buscar por valor
  [yellow]k[/yellow]           - Mudar coluna de busca (atual: {self.search_column})
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
            if direction.lower() == 'f':
                new_idx = self.current_row_idx + count
            else:
                new_idx = self.current_row_idx - count
            self._display_row(new_idx)
        except ValueError:
            self.console.print("[red]Erro: Digite um número válido[/red]")
    
    def _jump_to_line(self):
        """Jump to a specific line."""
        try:
            line_num = int(self.console.input("[yellow]Número da linha: [/yellow]"))
            self._display_row(line_num)
        except ValueError:
            self.console.print("[red]Erro: Digite um número válido[/red]")
    
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
            self.console.print("[red]Erro: Digite um número válido[/red]")
    
    def _change_search_column(self):
        """Change search column."""
        self.console.print("\n[bold cyan]Colunas disponíveis:[/bold cyan]")
        
        table = Table(show_header=True, header_style="bold magenta")
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
                
                if cmd in ['q', 'quit', 'exit']:
                    self.console.print("[yellow]Saindo...[/yellow]")
                    break
                elif cmd in ['n', 'next', 'down']:
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
                elif cmd == 'c':
                    self._change_context()
                elif cmd == 'h':
                    self._show_help()
                else:
                    self.console.print("[red]Comando desconhecido. Digite 'h' para ajuda.[/red]")
            
            except KeyboardInterrupt:
                self.console.print("\n[yellow]Saindo...[/yellow]")
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
        viewer.run()
    
    else:
        selected_dir = directory_picker()
        if not selected_dir:
            console.print("[red]Nenhum diretório selecionado[/red]")
            sys.exit(1)
        file_picker(selected_dir)


if __name__ == "__main__":
    main()
