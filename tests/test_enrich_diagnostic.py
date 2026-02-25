"""Diagnostic test: compare original vs enriched CSV files to detect fractional values."""
from pathlib import Path
from typing import Optional
import polars as pl
import pytest


def read_sample(path: str, n_rows: int = 5) -> Optional[pl.DataFrame]:
    """Read first n_rows from CSV or Parquet file."""
    p = Path(path)
    if not p.exists():
        pytest.skip(f"File not found: {p}")
    if p.suffix.lower() in (".csv", ".txt"):
        return pl.read_csv(str(p), n_rows=n_rows)
    return pl.read_parquet(str(p)).head(n_rows)


def test_sanplat_enrichment_preserves_integer_values():
    """Verify SANPLAT enriched file preserves original integer values (no fractional like 0.75, 0.5, 0.25)."""
    original_path = "data/refined/SANPLAT/Dados_Comunicacao_SANPLAT.csv"
    enriched_path = "data/trusted/SANPLAT/Dados_Comunicacao_SANPLAT_com_datas.csv"
    
    original = read_sample(original_path, n_rows=100)
    enriched = read_sample(enriched_path, n_rows=100)
    
    assert original is not None
    assert enriched is not None
    
    print(f"\nOriginal columns: {original.columns}")
    print(f"Enriched columns: {enriched.columns}")
    
    # Find numeric-named date columns
    date_cols = [c for c in original.columns if c.isdigit()]
    print(f"Date columns detected: {date_cols[:10]}...")
    
    fractional_violations: list[tuple[str, list]] = []
    
    for col in date_cols[:20]:  # Check first 20 date columns
        if col not in enriched.columns:
            print(f"[WARN] Column {col} missing in enriched")
            continue
        
        orig_series = original[col]
        enr_series = enriched[col]
        
        orig_unique = orig_series.unique().sort().to_list()
        enr_unique = enr_series.unique().sort().to_list()
        
        print(f"\nColumn {col}:")
        print(f"  Original unique values: {orig_unique[:10]}")
        print(f"  Enriched unique values: {enr_unique[:10]}")
        
        # Check for fractional values in enriched
        fractional_in_enriched = []
        for val in enr_unique:
            val_str = str(val)
            if isinstance(val, float) and val != int(val):
                fractional_in_enriched.append(val)
            elif '.' in val_str and val_str not in ['0.0', '1.0']:
                fractional_in_enriched.append(val)
        
        if fractional_in_enriched:
            fractional_violations.append((col, fractional_in_enriched))
            print(f"  ❌ FRACTIONAL VALUES DETECTED: {fractional_in_enriched}")
        else:
            print(f"  ✓ No fractional values")
        
        # Show first few differences
        orig_vals_set = set(orig_unique)
        enr_vals_set = set(str(v) for v in enr_unique if isinstance(v, (int, float)))
        diff = enr_vals_set - orig_vals_set
        if diff:
            print(f"  ⚠ New/changed values in enriched: {list(diff)[:5]}")
    
    if fractional_violations:
        msg = f"Found fractional values in {len(fractional_violations)} columns:\n"
        for col, vals in fractional_violations[:5]:
            msg += f"  Column {col}: {vals}\n"
        pytest.fail(msg)


def test_orca_enrichment_preserves_integer_values():
    """Verify ORCA enriched file preserves original integer values."""
    original_path = "data/raw/ORCA/Dados_Comunicacao.parquet"
    enriched_path = "data/trusted/ORCA/Dados_Comunicacao_com_datas.csv"
    
    original = read_sample(original_path, n_rows=100)
    enriched = read_sample(enriched_path, n_rows=100)
    
    assert original is not None
    assert enriched is not None
    
    print(f"\nOriginal columns: {len(original.columns)}")
    print(f"Enriched columns: {len(enriched.columns)}")
    
    # Find numeric-named date columns
    date_cols = [c for c in original.columns if c.isdigit()]
    print(f"Date columns detected: {len(date_cols)}")
    
    fractional_violations: list[tuple[str, list]] = []
    
    for col in date_cols[:20]:  # Check first 20 date columns
        if col not in enriched.columns:
            print(f"[WARN] Column {col} missing in enriched")
            continue
        
        orig_series = original[col]
        enr_series = enriched[col]
        
        orig_unique = orig_series.unique().sort().to_list()
        enr_unique = enr_series.unique().sort().to_list()
        
        print(f"\nColumn {col}:")
        print(f"  Original unique: {orig_unique[:5]}")
        print(f"  Enriched unique: {enr_unique[:5]}")
        
        # Check for fractional values in enriched
        fractional_in_enriched = []
        for val in enr_unique:
            val_str = str(val)
            if isinstance(val, float) and val != int(val):
                fractional_in_enriched.append(val)
            elif '.' in val_str and val_str not in ['0.0', '1.0']:
                fractional_in_enriched.append(val)
        
        if fractional_in_enriched:
            fractional_violations.append((col, fractional_in_enriched))
            print(f"  ❌ FRACTIONAL VALUES: {fractional_in_enriched}")
        else:
            print(f"  ✓ No fractional values")
    
    if fractional_violations:
        msg = f"Found fractional values in {len(fractional_violations)} columns:\n"
        for col, vals in fractional_violations[:5]:
            msg += f"  Column {col}: {vals}\n"
        pytest.fail(msg)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
