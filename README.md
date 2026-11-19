# Low-code vs programmatic ETL benchmark (Pentaho and Python/pandas)

Repository https://github.com/nadzlaandrita/lowcode-vs-code-etl-benchmark

Code and benchmark logs for the paper "A Comparative Study of Low-Code and
Programmatic ETL Pipeline Architectures for Multi-Source Data Integration".

The study replicates a Pentaho Data Integration pipeline, built in an existing student project, in Python/pandas and
compares both on performance, maintainability, auditability, and reproducibility.
The case study integrates three data categories (income and inequality, peace,
inflation) from four raw CSV files into a star schema with one fact table
(`Fact_Economy_Peace`) and two dimension tables (`Dim_Country`, `Dim_Time`).

## Repository layout

    etl_pipeline.py        Python pipeline (Pipeline B) with per-stage timing and peak memory
    pentaho/               Pentaho .ktr/.kjb files (Pipeline A)
    pentaho_output/        Output folder of the Pentaho jobs
    data/                  Place the four raw CSV files here (see data/README.md)
    results/               Raw console output of the benchmark runs reported in the paper
    requirements.txt

## Quick start

    pip install -r requirements.txt
    python etl_pipeline.py --raw-dir data --out-dir output --runs 10

The script prints environment details, a per-stage table (seconds and rows out) for
each run, and the mean and standard deviation of runtime and peak memory.

## Deduplication modes

The income data contains several records per country and year. The two modes differ
only in how `clean_income()` removes duplicates.

| Mode | Behavior | Rows after dedup |
|---|---|---|
| `business-key` (default) | Keep the first row per (iso3c, year) | 927 |
| `consecutive-key` | Drop a row only if it repeats the (iso3c, year) key of the row directly above it | 1,845 |

`consecutive-key` mimics the Pentaho Unique Rows step, which receives unsorted input
and therefore only detects adjacent duplicates. It reproduces the Pentaho
intermediate count exactly (1,845).

    python etl_pipeline.py --raw-dir data --out-dir output_bk --runs 10 --dedup-mode business-key
    python etl_pipeline.py --raw-dir data --out-dir output_ck --runs 10 --dedup-mode consecutive-key

## Row counts at key stages

| Stage | Pentaho | Python business-key | Python consecutive-key |
|---|---|---|---|
| After range/null filtering | 4,211 | 4,211 | 4,211 |
| After deduplication | 1,845 | 927 | 1,845 |
| Final fact table rows | 746 | 845 | 1,683 |
| Final countries | 130 | 148 | 148 |
| Final distinct years | 11 | 11 | 11 |

Pentaho counts exclude the header line that Step Metrics adds to text output steps.

## Why the final fact tables differ

The Pentaho Merge Join steps join on country name and year, while the Python pipeline
joins on the ISO code (`iso3c`). `results/reconciliation_fact_keys.csv` lists the
(iso3c, year) keys that exist in only one fact table. There are 737 shared keys,
108 keys only in Python, and 9 keys only in Pentaho (845 - 108 + 9 = 746). The 108
Python-only keys belong to 19 countries whose names are spelled differently across the
source files (CIV, COD, COG, CZE, EGY, GMB, IRN, KGZ, KOR, LAO, MKD, PSE, RUS, SVK,
SWZ, TUR, USA, VEN, YEM). The 9 Pentaho-only keys belong to Kosovo, which carries three
different codes in the sources (KSV, XXK, XKX).

## Benchmark results reported in the paper

| Pipeline | Runs | Mean runtime (s) | SD (s) | Peak memory (MB) |
|---|---|---|---|---|
| Pentaho, complete Kitchen process (aligned job) | 10 | 8.794 | 0.275 | 605.0 (SD 24.1, max 663.7) |
| Pentaho, empty job (startup) | 10 | 6.779 | 0.259 | not measured |
| Pentaho, job time (difference) | 10 | 2.015 | 0.275 | not applicable |
| Python business-key (in-script) | 10 | 0.287 | 0.071 | 98.6 (SD 1.3, max 100.1) |
| Python consecutive-key (in-script) | 10 | 0.261 | 0.029 | 98.9 (SD 1.3, max 99.8) |

Pentaho and Python were run on two different machines. See Section 4.2 of the paper
for the hardware details. Pentaho time is the wall-clock time of a full Kitchen process,
and Pentaho memory is the summed working set of the Java processes. Python time is the
sum of in-script stage timers, which excludes interpreter startup, and Python memory is
the process working set read with psutil. Because the Python peak is tracked inside one
process across repeated runs, later runs cannot report a lower value than earlier ones.

Raw logs are in `results/`.

| File | Content |
|---|---|
| `python_*_10runs.txt` | Console output of the ten Python runs per mode |
| `pentaho_aligned_10runs.csv` | Run, total seconds, peak MB for the aligned Pentaho job |
| `pentaho_empty_job_startup_10runs.csv` | Run, seconds for the empty job (Job_kosong.kjb) |
| `reconciliation_fact_keys.csv` | Keys present in only one of the two fact tables |

## Reproducing the Pentaho side

See `pentaho/README.md`.

## License

MIT. See `LICENSE`. The datasets keep the licenses of their original publishers.
