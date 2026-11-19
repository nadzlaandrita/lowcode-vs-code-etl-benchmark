"""
etl_pipeline.py
----------------
Code-first (pandas) re-implementation of the Pentaho ETL pipeline described in:
"Pentaho-Based ETL Pipeline and Star Schema Modeling for Global Economic-Social
Stability Analytics" (Hasnawi, Norman, Kurniawan).

Purpose: serve as the comparison pipeline for a low-code (Pentaho) vs. code-first
(Python/pandas) ETL benchmark. Reproduces the same ingestion, cleaning, join,
filtering, and star-schema output logic as the original .ktr/.kjb transformations,
with timing instrumentation added at each stage for direct comparison.

Usage:
    python etl_pipeline.py --raw-dir /path/to/csvs --out-dir /path/to/output
"""

import argparse
import platform
import statistics
import time
from pathlib import Path

import pandas as pd

try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False

YEAR_START, YEAR_END = 2008, 2022


def print_environment_info():
    """Prints machine/software specs so they can be copy-pasted straight into the paper."""
    print("=== Environment ===")
    print(f"OS: {platform.system()} {platform.release()} ({platform.version()})")
    print(f"Processor: {platform.processor() or 'unknown'}")
    print(f"Python: {platform.python_version()}")
    print(f"pandas: {pd.__version__}")
    if _HAS_PSUTIL:
        print(f"CPU cores (logical): {psutil.cpu_count(logical=True)}")
        total_ram_gb = psutil.virtual_memory().total / (1024 ** 3)
        print(f"RAM: {total_ram_gb:.1f} GB")
    else:
        print("CPU/RAM: install 'psutil' (pip install psutil) for automatic detection")
    print()


class _PeakMemoryTracker:
    """Samples this process's memory usage in the background while the pipeline runs."""

    def __init__(self, interval=0.02):
        self.interval = interval
        self.peak_mb = 0.0
        self._running = False
        self._thread = None

    def _poll(self):
        import threading
        proc = psutil.Process()
        while self._running:
            try:
                mem = proc.memory_info()
                # Windows exposes peak_wset; other platforms fall back to rss.
                current = getattr(mem, "peak_wset", None) or mem.rss
                self.peak_mb = max(self.peak_mb, current / (1024 ** 2))
            except Exception:
                pass
            time.sleep(self.interval)

    def __enter__(self):
        if not _HAS_PSUTIL:
            return self
        import threading
        self._running = True
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=1)


class StageTimer:
    """Collects per-stage wall-clock timings for the benchmark report."""

    def __init__(self):
        self.records = []

    def stage(self, name):
        return _StageContext(self, name)

    def report(self):
        total = sum(r["seconds"] for r in self.records)
        lines = [f"{'Stage':<28}{'Seconds':>10}{'Rows out':>12}"]
        lines.append("-" * 50)
        for r in self.records:
            lines.append(f"{r['name']:<28}{r['seconds']:>10.4f}{r['rows']:>12}")
        lines.append("-" * 50)
        lines.append(f"{'TOTAL':<28}{total:>10.4f}")
        return "\n".join(lines)


class _StageContext:
    def __init__(self, timer, name):
        self.timer = timer
        self.name = name
        self.rows = None

    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def set_rows(self, n):
        self.rows = n

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.perf_counter() - self.t0
        self.timer.records.append(
            {"name": self.name, "seconds": elapsed, "rows": self.rows if self.rows is not None else "-"}
        )


# ---------------------------------------------------------------------------
# 1. INGESTION  (mirrors Pentaho "CSV File Input" + "Column Definition" steps)
# ---------------------------------------------------------------------------

def ingest_inflation(path: Path) -> pd.DataFrame:
    """Wide-format CSV (years as columns) -> long format, 2008-2018 only."""
    df = pd.read_csv(path, encoding="latin1")
    # Source contains 5 CPI series per country (Headline, Food, Energy, Core,
    # Producer). Keep Headline CPI only, matching the single inflation_rate
    # column produced by the original pipeline.
    df = df[df["Series Name"] == "Headline Consumer Price Inflation"]
    year_cols = [str(y) for y in range(YEAR_START, YEAR_END + 1)]
    id_cols = ["Country Code", "Country"]
    long = df[id_cols + year_cols].melt(
        id_vars=id_cols, var_name="year", value_name="inflation_rate"
    )
    long["year"] = long["year"].astype(int)
    long = long.rename(columns={"Country Code": "iso3c"})
    long["inflation_rate"] = pd.to_numeric(long["inflation_rate"], errors="coerce")
    return long[["iso3c", "year", "inflation_rate"]]


def ingest_peace(path: Path) -> pd.DataFrame:
    """Semicolon-delimited, decimal-comma CSV -> long format, 2008-2018 only."""
    df = pd.read_csv(path, sep=";", encoding="utf-8-sig")
    year_cols = [str(y) for y in range(YEAR_START, YEAR_END + 1)]
    long = df[["Country", "iso3c"] + year_cols].melt(
        id_vars=["Country", "iso3c"], var_name="year", value_name="peace_index"
    )
    long["year"] = long["year"].astype(int)
    # decimal comma -> dot (mirrors the "Replace in String" cleaning step)
    long["peace_index"] = (
        long["peace_index"].astype(str).str.replace(",", ".", regex=False)
    )
    long["peace_index"] = pd.to_numeric(long["peace_index"], errors="coerce")
    return long[["iso3c", "year", "peace_index"]]


def ingest_income(wider_path: Path, wdi_gini_path: Path) -> pd.DataFrame:
    """Income/inequality sources -> Gini Index + GDP per capita.

    The case-study pipeline draws Gini/GDP from TWO raw sources (confirmed via
    the Pentaho run's Step Metrics: 11,827 rows from Raw_WIDER_data + 3,413
    rows from WDI_GINI_data are read, then unioned before deduplication).
    Both share an identical column layout (c3, year, gini_reported,
    gdp_ppp_pc_usd2011), so they are concatenated prior to cleaning.
    """
    frames = []
    for path in (wider_path, wdi_gini_path):
        df = pd.read_csv(path, encoding="latin1")
        df = df.rename(columns={"c3": "iso3c"})
        df = df[(df["year"] >= YEAR_START) & (df["year"] <= YEAR_END)]
        keep = ["iso3c", "country", "year", "gini_reported", "gdp_ppp_pc_usd2011"]
        frames.append(
            df[keep].rename(columns={"gini_reported": "gini_index", "gdp_ppp_pc_usd2011": "gdp_val"})
        )
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# 2. CLEANING  (mirrors "Filter Rows", "Unique Rows", "Select Values" steps)
# ---------------------------------------------------------------------------

def clean_income(df: pd.DataFrame, dedup_mode: str = "business-key") -> pd.DataFrame:
    """dedup_mode controls how duplicate income rows are removed.

    "business-key"  : keep the first record per (iso3c, year). Default behaviour.
    "consecutive-key": drop a row only when it repeats the (iso3c, year) key of the
                      row directly above it. This mirrors Pentaho's 'Unique Rows' step
                      on unsorted input, which only detects consecutive duplicates.
    """
    df = df.dropna(subset=["gini_index", "gdp_val"]).copy()
    # Leading-zero / format cleanup: numeric coercion already strips these;
    # explicit strip kept here for parity with the original "#.#" mask logic.
    df["gini_index"] = pd.to_numeric(df["gini_index"], errors="coerce")
    df["gdp_val"] = pd.to_numeric(df["gdp_val"], errors="coerce")
    # Multiple survey sources per country-year exist in the raw WIDER export;
    # the original pipeline's "Unique Rows" step keeps the first record
    # encountered per (country, year) key.
    if dedup_mode == "consecutive-key":
        key = df[["iso3c", "year"]]
        df = df[~(key == key.shift()).all(axis=1)]
    else:
        df = df.drop_duplicates(subset=["iso3c", "year"], keep="first")
    return df


def clean_peace(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(subset=["peace_index"])


def clean_inflation(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(subset=["inflation_rate"])


# ---------------------------------------------------------------------------
# 3. JOIN  (mirrors "Merge Join" on Country + Year)
# ---------------------------------------------------------------------------

def join_all(income: pd.DataFrame, peace: pd.DataFrame, inflation: pd.DataFrame) -> pd.DataFrame:
    merged = income.merge(peace, on=["iso3c", "year"], how="inner")
    merged = merged.merge(inflation, on=["iso3c", "year"], how="inner")
    merged = merged.dropna(subset=["peace_index", "inflation_rate", "gini_index", "gdp_val"])
    return merged


# ---------------------------------------------------------------------------
# 4. STAR SCHEMA OUTPUT  (mirrors the "Dummy" branching step)
# ---------------------------------------------------------------------------

def build_fact_table(merged: pd.DataFrame) -> pd.DataFrame:
    fact = merged[["iso3c", "year", "peace_index", "inflation_rate", "gini_index", "gdp_val"]]
    fact = fact.sort_values(["iso3c", "year"]).reset_index(drop=True)
    return fact


def build_dim_country(merged: pd.DataFrame) -> pd.DataFrame:
    dim = merged[["iso3c", "country"]].drop_duplicates().rename(columns={"country": "Country"})
    dim["Country"] = dim["Country"].str.upper()
    dim = dim.sort_values("Country").drop_duplicates(subset=["iso3c"]).reset_index(drop=True)
    return dim


def build_dim_time(fact: pd.DataFrame) -> pd.DataFrame:
    """Distinct years present in the final fact table (mirrors Pentaho's Dim_Time)."""
    years = sorted(fact["year"].unique())
    return pd.DataFrame({"year": years})


def build_abt(fact: pd.DataFrame, dim: pd.DataFrame) -> pd.DataFrame:
    abt = fact.merge(dim, on="iso3c", how="left")
    cols = ["Country", "iso3c", "year", "peace_index", "inflation_rate", "gini_index", "gdp_val"]
    return abt[cols].sort_values(["Country", "year"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------------

def _find_inflation_file(raw_dir: Path) -> Path:
    """Accept either 'Global_Dataset_of_Inflation.csv' or 'Global Dataset of Inflation.csv'."""
    for name in ("Global_Dataset_of_Inflation.csv", "Global Dataset of Inflation.csv"):
        if (raw_dir / name).exists():
            return raw_dir / name
    return raw_dir / "Global_Dataset_of_Inflation.csv"


def run_pipeline(raw_dir: Path, out_dir: Path, dedup_mode: str = "business-key") -> StageTimer:
    timer = StageTimer()
    out_dir.mkdir(parents=True, exist_ok=True)

    with timer.stage("Ingest: Inflation") as s:
        inflation = ingest_inflation(_find_inflation_file(raw_dir))
        s.set_rows(len(inflation))

    with timer.stage("Ingest: Peace Index") as s:
        peace = ingest_peace(raw_dir / "peace_index.csv")
        s.set_rows(len(peace))

    with timer.stage("Ingest: Income (WIDER + WDI_GINI)") as s:
        income = ingest_income(raw_dir / "Raw_WIDER_data.csv", raw_dir / "WDI_GINI_data.csv")
        s.set_rows(len(income))

    with timer.stage("Clean: Inflation") as s:
        inflation = clean_inflation(inflation)
        s.set_rows(len(inflation))

    with timer.stage("Clean: Peace Index") as s:
        peace = clean_peace(peace)
        s.set_rows(len(peace))

    with timer.stage("Clean: Income") as s:
        income = clean_income(income, dedup_mode)
        s.set_rows(len(income))

    with timer.stage("Join (Country + Year)") as s:
        merged = join_all(income, peace, inflation)
        s.set_rows(len(merged))

    with timer.stage("Build Fact_Economy_Peace") as s:
        fact = build_fact_table(merged)
        s.set_rows(len(fact))

    with timer.stage("Build Dim_Country") as s:
        dim = build_dim_country(merged)
        s.set_rows(len(dim))

    with timer.stage("Build Dim_Time") as s:
        dim_time = build_dim_time(fact)
        s.set_rows(len(dim_time))

    with timer.stage("Build ABT + Export CSV") as s:
        abt = build_abt(fact, dim)
        fact.to_csv(out_dir / "Fact_Economy_Peace.csv", index=False)
        dim.to_csv(out_dir / "Dim_Country.csv", index=False)
        dim_time.to_csv(out_dir / "Dim_Time.csv", index=False)
        abt.to_csv(out_dir / "Analytical_Base_Table.csv", index=False)
        s.set_rows(len(abt))

    return timer


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Code-first ETL pipeline (Pentaho comparison)")
    parser.add_argument("--raw-dir", type=Path, default=Path("."), help="Folder with the 4 raw CSVs")
    parser.add_argument("--out-dir", type=Path, default=Path("./output"), help="Folder for pipeline outputs")
    parser.add_argument("--runs", type=int, default=10, help="Repeat N times (>=10 recommended for mean/SD reporting)")
    parser.add_argument("--dedup-mode", choices=["business-key", "consecutive-key"], default="business-key",
                        help="business-key = dedupe on (iso3c, year); consecutive-key = mimic Pentaho Unique Rows on unsorted input")
    args = parser.parse_args()

    print_environment_info()
    print(f"Dedup mode: {args.dedup_mode}\n")
    if not _HAS_PSUTIL:
        print("NOTE: peak memory will not be measured. Run 'pip install psutil' and re-run for that number.\n")

    all_totals = []
    all_peaks = []
    for i in range(args.runs):
        with _PeakMemoryTracker() as tracker:
            timer = run_pipeline(args.raw_dir, args.out_dir, args.dedup_mode)
        total = sum(r["seconds"] for r in timer.records)
        all_totals.append(total)
        print(f"\n=== Run {i + 1}/{args.runs} ===")
        print(timer.report())
        if _HAS_PSUTIL:
            print(f"Peak memory this run: {tracker.peak_mb:.1f} MB")
            all_peaks.append(tracker.peak_mb)

    if args.runs > 1:
        mean_t = statistics.mean(all_totals)
        sd_t = statistics.stdev(all_totals) if len(all_totals) > 1 else 0.0
        print(f"\n=== Summary over {args.runs} runs ===")
        print(f"Runtime:      mean = {mean_t:.4f}s   SD = {sd_t:.4f}s")
        if all_peaks:
            mean_m = statistics.mean(all_peaks)
            sd_m = statistics.stdev(all_peaks) if len(all_peaks) > 1 else 0.0
            print(f"Peak memory:  mean = {mean_m:.1f} MB   SD = {sd_m:.1f} MB   max = {max(all_peaks):.1f} MB")
        print("\nCopy the 'mean +/- SD' figures above directly into the paper's results table.")

    print("\nOutputs written to:", args.out_dir.resolve())
