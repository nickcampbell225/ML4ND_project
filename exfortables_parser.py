from __future__ import annotations

import re
from pathlib import Path
from typing import Optional, Dict, Any, List

import pandas as pd


HEADER_Z_RE = re.compile(r"^\s*#\s*Z:\s*(\d+)\s*$", re.IGNORECASE)
HEADER_A_RE = re.compile(r"^\s*#\s*A:\s*(\d+)\s*$", re.IGNORECASE)
HEADER_MF_MT_RE = re.compile(r"^\s*#\s*MF-MT\s*number:\s*(\d+)\s*-\s*(\d+)\s*$", re.IGNORECASE)


def parse_exfor_like_file(fp: Path) -> pd.DataFrame:
    """
    Parse one EXFOR-like txt file:
    - Header lines start with '#'
    - One commented column header line like '#  E_in(MeV) ...'
    - Data rows are whitespace-separated floats
    Returns a DataFrame with columns from the data table plus extracted header metadata.
    """
    text = fp.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()

    # ---- Extract metadata from header ----
    z: Optional[int] = None
    a: Optional[int] = None
    mt: Optional[int] = None

    # Also try to capture column names from the commented header line
    colnames: Optional[List[str]] = None

    for ln in lines:
        if not ln.lstrip().startswith("#"):
            continue

        m = HEADER_Z_RE.match(ln)
        if m:
            z = int(m.group(1))
            continue

        m = HEADER_A_RE.match(ln)
        if m:
            a = int(m.group(1))
            continue

        m = HEADER_MF_MT_RE.match(ln)
        if m:
            # MF is group(1); MT is group(2)
            mt = int(m.group(2))
            continue

        # Column header line (commented) usually contains parentheses and multiple spaces
        # Example: "#       E_in(MeV)         dE_in(MeV)        XS(B)             dXS(B)"
        if "E_in" in ln and "XS" in ln:
            # Strip leading "#", then split on 2+ spaces to preserve tokens
            header_part = ln.lstrip("#").strip()
            tokens = re.split(r"\s{2,}", header_part)
            # Normalize to safe column names
            def norm(tok: str) -> str:
                tok = tok.strip()
                tok = tok.replace("(", "_").replace(")", "").replace("/", "_per_")
                tok = tok.replace("-", "_").replace("__", "_")
                return tok

            colnames = [norm(t) for t in tokens if t.strip()]

    # ---- Read numeric block ----
    data_rows: List[List[float]] = []
    for ln in lines:
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        # Must contain at least two numeric fields (energy, xs)
        parts = s.split()
        try:
            nums = [float(x) for x in parts]
        except ValueError:
            continue
        data_rows.append(nums)

    if not data_rows:
        # Return empty DF with expected columns
        out = pd.DataFrame(columns=["E_in_MeV", "XS_b", "nuclide", "Z", "A", "MT", "source_file"])
        return out

    ncols = len(data_rows[0])
    if any(len(r) != ncols for r in data_rows):
        # ragged rows -> pad with NaN
        maxc = max(len(r) for r in data_rows)
        data_rows = [r + [float("nan")] * (maxc - len(r)) for r in data_rows]
        ncols = maxc

    df = pd.DataFrame(data_rows)

    # Apply parsed column names if they match; else fall back to sensible defaults
    if colnames and len(colnames) == ncols:
        df.columns = colnames
    else:
        # Typical EXFOR-like layout: E, dE, XS, dXS (but sometimes fewer)
        default = ["E_in_MeV", "dE_in_MeV", "XS_b", "dXS_b"]
        df.columns = default[:ncols] + [f"col_{i}" for i in range(len(default), ncols)]

    # ---- Attach metadata ----
    df["Z"] = z
    df["A"] = a
    df["MT"] = mt
    df["source_file"] = str(fp)

    return df


def build_big_dataframe(root: str | Path) -> pd.DataFrame:
    """
    Walk root/nuclide/reaction/quantity and collect only quantity == 'xs'.
    Adds 'nuclide' from the path segment (root/<nuclide>/<reaction>/xs/...)
    """
    root = Path(root)
    all_dfs: List[pd.DataFrame] = []

    # Find all txt files under any xs folder (quantity == xs)
    # Pattern: root/*/*/xs/*.txt (but allow deeper just in case)
    for fp in root.glob("*/*/xs/**/*.txt"):
        # Get nuclide name from path: root/<nuclide>/<reaction>/xs/<file>
        try:
            nuclide = fp.relative_to(root).parts[0]
        except ValueError:
            nuclide = fp.parts[-4]  # fallback guess

        df = parse_exfor_like_file(fp)
        if not df.empty:
            df["nuclide"] = nuclide
            all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame(columns=["nuclide", "Z", "A", "MT", "E_in_MeV", "XS_b", "source_file"])

    big = pd.concat(all_dfs, ignore_index=True)

    # Ensure common canonical names exist (in case header-derived names were used)
    # If the file used "E_in_MeV" already, nothing changes.
    # If it used something like "E_in_MeV" vs "E_inMeV", you can map here.
    # For now, we just keep whatever was parsed.

    # Put key columns first if they exist
    preferred = ["nuclide", "Z", "A", "MT", "E_in_MeV", "XS_b", "dE_in_MeV", "dXS_b", "source_file"]
    cols = [c for c in preferred if c in big.columns] + [c for c in big.columns if c not in preferred]
    big = big[cols]

    return big


# Example usage:
root_dir = "/Users/nickcampbell/exfortables_py/n"
df_all = build_big_dataframe(root_dir)
print(df_all.head())
df_all.to_csv("exfortables.csv", index=False)
