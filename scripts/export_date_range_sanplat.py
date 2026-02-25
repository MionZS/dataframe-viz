from pathlib import Path
from datetime import datetime
import polars as pl

# Parameters
enriched_file = Path("data/trusted/SANPLAT/Dados_Comunicacao_SANPLAT_com_datas.csv")
output_file = Path("data/trusted/SANPLAT/Dados_Comunicacao_20251227_20260131.csv")
from_date = datetime(2025, 12, 27)
to_date = datetime(2026, 1, 31)

if not enriched_file.exists():
    raise SystemExit(f"Enriched file not found: {enriched_file}")

# Read only first data row to get date values (small)
first_row = pl.read_csv(str(enriched_file), n_rows=1)
cols = first_row.columns

selected_date_cols = []
other_cols = []

for c in cols:
    val = first_row[c][0]
    # Keep non-date columns (non-numeric names) as "other"
    if not c.isdigit():
        other_cols.append(c)
        continue
    # Try to parse date string in dd/mm/YYYY or other formats
    parsed = None
    if isinstance(val, str):
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(val, fmt)
                break
            except Exception:
                parsed = None
    if parsed is not None and from_date <= parsed <= to_date:
        selected_date_cols.append(c)

select_cols = other_cols + selected_date_cols

print(f"Selecting {len(select_cols)} columns ({len(other_cols)} non-date + {len(selected_date_cols)} date cols)")
print(f"Writing to: {output_file}")

# Use lazy scan and sink to write without loading full data
lf = pl.scan_csv(str(enriched_file), ignore_errors=True)
lf.select(select_cols).sink_csv(str(output_file))
print("Done")
