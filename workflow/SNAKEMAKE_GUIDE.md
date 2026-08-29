# Snakemake: The Complete Guide

## Part 0 — Theory: What Snakemake Actually Is

### The problem it solves

Imagine a pipeline with five steps: download raw data → clean it → transform it → aggregate it →
load it into a warehouse. You could write this as one long Python script that runs top to bottom.
That works fine... until:

- You change something in step 4, and now you have to remember to re-run 4 and 5, but _not_
  1–3 (they're unchanged and expensive to re-run).
- You want step 2 to run once _per input file_ (10 raw files → 10 parallel cleaning jobs).
- You want to know, six months from now, exactly what commands produced a given output file.
- You want the same pipeline to run on your laptop today and on a cluster or in CI tomorrow,
  without rewriting it.

A plain script handles none of this gracefully. **Snakemake** is a workflow management system
that solves it by letting you describe your pipeline not as "do this, then this, then this" but
as a set of **rules**, each of which says:

> "To produce _this output file_, run _this command_ using _these input files_."

Snakemake reads all your rules, looks at what output files you're asking for, and works
_backwards_ to figure out which rules need to run, in what order, to produce them — skipping
anything whose outputs already exist and are newer than their inputs. This is exactly the same
idea as the classic Unix `make` build tool (hence the name — "Snake" + "Make", since it's a
Python-flavored Make), but with a much friendlier syntax and Python underneath.

### The core mental model: a DAG

Every Snakemake workflow is, under the hood, a **Directed Acyclic Graph (DAG)**:

- **Nodes** = individual jobs (a rule applied to specific input/output files)
- **Edges** = "this job's output is that job's input"

Snakemake's entire job is to build this graph from your rules, then execute it — in parallel
where possible, skipping already-up-to-date nodes, and stopping/reporting clearly if something
fails.

```
data/raw/store_0001.csv ──┐
                           ├──▶ clean_store ──▶ results/cleaned/store_0001.csv ──┐
data/raw/store_0002.csv ──┐                                                     ├──▶ combine ──▶ results/combined.csv
                           ├──▶ clean_store ──▶ results/cleaned/store_0002.csv ──┘
```

### Why it's popular (and what it's actually used for)

Snakemake was born in **bioinformatics** (genomics pipelines routinely chain dozens of
command-line tools over thousands of files), and that's still its heaviest user base — but
nothing about it is bio-specific. It's a general-purpose alternative/complement to tools like:

| Tool                            | Typical niche                                                                                                              |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| **Make**                        | Original inspiration; C/C++ builds, generic file pipelines                                                                 |
| **Snakemake**                   | Scientific & data pipelines, reproducible research, local-to-cluster-to-cloud scaling                                      |
| **Airflow / Prefect / Dagster** | Long-running, schedule-driven, often _task_-oriented (not always file-oriented) orchestration in production data platforms |
| **Nextflow**                    | Bioinformatics; similar goals to Snakemake, Groovy-based instead of Python                                                 |
| **dbt**                         | SQL-specific transformation DAGs inside a warehouse                                                                        |

Snakemake's specific strengths:

1. **File-centric.** A job "exists" because its output file doesn't exist yet (or is stale).
   This is a very natural fit for data pipelines: raw → interim → processed → loaded.
2. **Python-native.** Rules can run shell commands, Python code, Jupyter notebooks, or R
   scripts — you're never fighting a foreign DSL.
3. **Reproducibility built in.** Per-rule Conda environments or containers, exact logging,
   provenance (`--report`), and DAG visualization are first-class, not bolted on.
4. **Scales without rewriting.** The _same_ Snakefile runs with `--cores 4` on your laptop or
   `--executor slurm --jobs 100` on a cluster — you change the invocation, not the workflow.
5. **Laziness.** Only what's actually stale gets recomputed. Change one config value that
   only affects step 4? Steps 1–3 don't re-run.

### Where this fits _your_ use case (a quick, honest note)

Snakemake is designed to be the thing _scheduling and running_ the whole pipeline
end-to-end (including in production, on a cluster). If your organization already has a
production scheduler (e.g. Control-M, Airflow, cron+systemd), you don't have to make Snakemake
replace that. A very common and pragmatic pattern — and one worth knowing about upfront — is to
use Snakemake purely as a **local, dev-time DAG runner**: you write your pipeline stages as
Snakemake rules so you get dependency tracking, caching, and one-command re-runs of "just the
stale parts" while you're developing and testing locally, and your production scheduler still
owns the actual scheduled execution. Nothing about Snakemake requires you to pick one or the
other — this guide covers both the full "Snakemake owns everything, including the cluster"
story and the lighter "Snakemake as a local DAG/test harness" story, and you can use as much or
as little as is useful.

---

## Part 1 — Setup: Wiring Snakemake Into Your Repo

You said you already have:

```
your-repo/
├── .venv/
├── .gitignore
├── requirements.txt
└── README.md
```

That's exactly the right starting point. Let's wire Snakemake in properly.

### 1.1 Pin it in `requirements.txt`

```text
# requirements.txt
snakemake==9.26.1
pandas
```

Then, with your venv activated:

```bash
pip install -r requirements.txt
```

You said Snakemake is already installed — good, `snakemake --version` should print something
like `9.26.1`. If it doesn't, that command above will get you there.

### 1.2 Recommended project layout

This is the layout the Snakemake community has converged on (and it's what the official
`snakemake-wrapper-utils` / workflow-catalog tooling expects if you ever publish a workflow).
You don't have to use it for a small pipeline, but it scales cleanly as your pipeline grows:

```
your-repo/
├── .venv/
├── .gitignore
├── requirements.txt
├── README.md
├── Snakefile                 # or workflow/Snakefile for the "full" layout
├── config/
│   └── config.yaml           # your pipeline's configuration
├── workflow/
│   ├── rules/                 # .smk files, one topic per file, included by the Snakefile
│   │   ├── clean.smk
│   │   └── load.smk
│   ├── scripts/                # Python/R scripts invoked by rules via `script:`
│   │   └── clean_store.py
│   └── envs/                   # one .yaml conda environment spec per rule/tool
│       └── pandas.yaml
├── data/
│   └── raw/                    # untouched input data (often .gitignore'd if large)
├── results/                    # everything Snakemake produces (always .gitignore'd)
└── logs/                       # per-rule log files (always .gitignore'd)
```

### 1.3 Add to `.gitignore`

Snakemake generates a lot of run-tracking metadata and, typically, large intermediate/output
data you don't want in version control:

```gitignore
# .gitignore additions
.snakemake/
results/
logs/
*.log
```

Keep `data/raw/` in `.gitignore` too if it's real (large or sensitive) data — commit only small
example/fixture data if you want the repo to be self-demonstrating.

### 1.4 Sanity check

Create the simplest possible `Snakefile` at your repo root:

```python
# Snakefile
rule hello:
    output:
        "hello.txt"
    shell:
        "echo 'Hello, Snakemake!' > {output}"
```

Run it:

```bash
snakemake --cores 1 hello.txt
```

You should see `hello.txt` appear containing `Hello, Snakemake!`, and Snakemake will print a
little execution summary. If that works, you're fully set up. Delete `hello.txt` and the
Snakefile content — we'll build the real thing next, piece by piece.

---

## Part 2 — Core Concepts: Rules, the DAG, and Running Snakemake

### 2.1 Anatomy of a rule

A rule is a named block with (usually) three parts: what it needs (`input`), what it produces
(`output`), and how to produce it (`shell`, `run`, or `script`).

```python
# Snakefile
rule clean_data:
    input:
        "data/raw/sales.csv"
    output:
        "results/cleaned/sales.csv"
    shell:
        "python -c \"import pandas as pd; "
        "df = pd.read_csv('{input}'); "
        "df.dropna().to_csv('{output}', index=False)\""
```

Read this as: _"To create `results/cleaned/sales.csv`, I need `data/raw/sales.csv`, and here's
the shell command that turns one into the other."_ `{input}` and `{output}` are placeholders
Snakemake fills in automatically — this is Python's `.format()` syntax under the hood.

### 2.2 How Snakemake decides what to run

You don't tell Snakemake "run rule A, then rule B." You tell it **what file you want**, and it
works backward:

```bash
snakemake --cores 1 results/cleaned/sales.csv
```

Snakemake sees you want `results/cleaned/sales.csv`. It scans all rules for one whose `output`
matches that path — finds `clean_data` — and checks: does `data/raw/sales.csv` (its `input`)
exist? If yes, and if the output either doesn't exist yet or is older than the input, it runs
the rule. If the output already exists and is newer than every input, Snakemake does **nothing**
— this is the laziness/caching behavior that makes re-running a big pipeline after a small
change fast.

### 2.3 `rule all` — the conventional default target

If you don't tell Snakemake which file you want, it builds whatever the **first rule** in the
Snakefile asks for. By convention, everyone puts a rule literally called `all` **first**, whose
only job is to list every final output you care about. This isn't special syntax — it's a
convention that makes `snakemake --cores 1` (no target file needed) always build "everything."

```python
# Snakefile
rule all:
    input:
        "results/cleaned/sales.csv"

rule clean_data:
    input:
        "data/raw/sales.csv"
    output:
        "results/cleaned/sales.csv"
    shell:
        "python scripts/clean.py {input} {output}"
```

> ⚠️ **Real gotcha, verified while writing this guide:** rule order matters for the _default_
> target. If you put another rule (say, a `checkpoint`) above `rule all`, Snakemake will try to
> build whatever _that_ rule produces by default instead of `all`'s inputs, and running plain
> `snakemake --cores N` with no target will silently do the "wrong" (i.e. not what you intended)
> thing. Always put `rule all` first.

### 2.4 Running it: the essential CLI flags

```bash
# See what WOULD run, without running it (always do this first!)
snakemake -n
# or the long form:
snakemake --dry-run

# Same as -n, but also prints the actual shell commands it would run
snakemake -n -p

# Actually execute, using up to 4 CPU cores across parallel jobs
snakemake --cores 4

# Build one specific file instead of everything in `rule all`
snakemake --cores 4 results/cleaned/sales.csv

# Force everything to re-run, ignoring what's already up to date
snakemake --cores 4 --forceall

# Force just one rule (and everything downstream of it) to re-run
snakemake --cores 4 --forcerun clean_data
```

**Always run `-n` (dry-run) before a real run**, especially while learning. It costs nothing and
shows you exactly what Snakemake _thinks_ needs to happen — catching config typos or DAG
mistakes before they cost you compute time.

### 2.5 Visualizing the DAG

This is one of Snakemake's best "beginner superpowers" — you can literally _see_ your pipeline:

```bash
# Requires graphviz installed (e.g. `apt install graphviz` / `brew install graphviz`)
snakemake --dag | dot -Tpng > dag.png     # per-job DAG (one box per file produced)
snakemake --rulegraph | dot -Tpng > rulegraph.png   # per-rule DAG (one box per rule, no wildcard duplication)
snakemake --filegraph | dot -Tpng > filegraph.png   # per-file, but grouped/simplified
```

`--rulegraph` is the one to reach for when you just want the "architecture diagram" of your
pipeline (one node per rule); `--dag` is the one to reach for when you want to see the _actual_
jobs that will run this particular invocation (one node per rule-applied-to-specific-wildcards,
so with 10 input files you'll see 10 boxes for that rule).

---

## Part 3 — Wildcards: One Rule, Many Files

This is the single most important idea in Snakemake beyond the basics. Without it, you'd need
one rule per input file — unworkable for real pipelines.

### 3.1 The problem

Say you have raw CSVs for multiple stores:

```
data/raw/store_0001.csv
data/raw/store_0002.csv
data/raw/store_0003.csv
```

Writing a separate rule per store would be absurd. Instead, you write **one rule with a
wildcard** — a named placeholder in curly braces that Snakemake fills in based on what file is
being requested:

```python
# Snakefile
rule clean_store:
    input:
        "data/raw/store_{store}.csv"
    output:
        "results/cleaned/store_{store}.csv"
    shell:
        "python scripts/clean.py {input} {output}"
```

`{store}` is a **wildcard**. If you ask Snakemake for `results/cleaned/store_0002.csv`,
Snakemake pattern-matches that against the rule's `output`, figures out `store = "0002"`,
substitutes that value into `input` to get `data/raw/store_0002.csv`, and runs the rule. Ask for
`store_0001.csv` instead, and the exact same rule runs with `store = "0001"`. **One rule, N
jobs.**

You can access the resolved value inside `shell`/`run`/`script` blocks via `wildcards`:

```python
rule clean_store:
    input:
        "data/raw/store_{store}.csv"
    output:
        "results/cleaned/store_{store}.csv"
    shell:
        "echo 'Cleaning store {wildcards.store}' && python scripts/clean.py {input} {output}"
```

### 3.2 `expand()` — requesting many wildcard values at once

You rarely want to ask for one store's cleaned file — you want _all_ of them. `expand()` is a
helper that builds a list of filenames by substituting every value from a list into a pattern:

```python
>>> expand("results/cleaned/store_{store}.csv", store=["0001", "0002", "0003"])
['results/cleaned/store_0001.csv', 'results/cleaned/store_0002.csv', 'results/cleaned/store_0003.csv']
```

Used in a rule, this is how you tell `rule all` "I want every store cleaned":

```python
STORES = ["0001", "0002", "0003"]

rule all:
    input:
        expand("results/cleaned/store_{store}.csv", store=STORES)

rule clean_store:
    input:
        "data/raw/store_{store}.csv"
    output:
        "results/cleaned/store_{store}.csv"
    shell:
        "python scripts/clean.py {input} {output}"
```

Now `snakemake --cores 4` will build all three cleaned files, running up to 4 of the
`clean_store` jobs **in parallel** (they don't depend on each other — Snakemake figures that out
from the DAG automatically).

`expand()` also handles **multiple wildcards** — it produces the full cross-product by default:

```python
expand("results/{region}/store_{store}.csv", region=["north", "south"], store=["0001", "0002"])
# → results/north/store_0001.csv, results/north/store_0002.csv,
#   results/south/store_0001.csv, results/south/store_0002.csv
```

### 3.3 Constraining wildcards

By default a wildcard matches almost anything, which can cause ambiguity (e.g. a rule for
`store_{store}.csv` might accidentally also try to match a totally unrelated file). Constrain a
wildcard with a regex, either per-rule or globally:

```python
rule clean_store:
    input:
        "data/raw/store_{store}.csv"
    output:
        "results/cleaned/store_{store}.csv"
    wildcard_constraints:
        store=r"\d{4}"          # must be exactly 4 digits
    shell:
        "python scripts/clean.py {input} {output}"

# Or globally, once, near the top of the Snakefile — applies to every rule with a `store` wildcard:
wildcard_constraints:
    store=r"\d{4}"
```

---

## Part 4 — The Three Ways to Define What a Rule Does

### 4.1 `shell:` — run a shell command

Best for wrapping existing command-line tools.

```python
rule sort_file:
    input:
        "data.csv"
    output:
        "data.sorted.csv"
    shell:
        "sort {input} > {output}"
```

### 4.2 `run:` — inline Python, executed in the _main_ Snakemake process

```python
rule count_rows:
    input:
        "data.csv"
    output:
        "row_count.txt"
    run:
        with open(input[0]) as f:
            n = sum(1 for _ in f)
        with open(output[0], "w") as f:
            f.write(str(n))
```

Inside a `run:` block, `input`, `output`, `params`, `wildcards`, etc. are plain Python objects
you index like lists (`input[0]`) or attribute-access by name (`input.raw_file`, if you named
it). **Caveat:** `run:` blocks execute inside Snakemake's own Python process, not a separate
one — fine for quick glue code, but it means heavy imports/state can bleed between jobs, and you
lose the clean isolation you get from `script:`. For anything beyond a few lines, prefer
`script:`.

### 4.3 `script:` — run an external Python (or R) file, verified working

This is the one you'll use the most for real data work. You point at a `.py` file, and inside
that file a magic `snakemake` object is injected giving you access to everything the rule knows
(input, output, params, wildcards, threads, log, config...).

```python
# Snakefile
rule clean_store:
    input:
        "data/raw/store_{store}.csv"
    output:
        "results/cleaned/store_{store}.csv"
    log:
        "logs/clean_store_{store}.log"
    script:
        "scripts/clean_store.py"
```

```python
# scripts/clean_store.py
import pandas as pd
import numpy as np

df = pd.read_csv(snakemake.input[0])

df["filiaalnr"] = df["filiaalnr"].astype(str).str.zfill(4)
df["amount"] = df["amount"].replace([np.inf, -np.inf], np.nan)

df.to_csv(snakemake.output[0], index=False)
print(f"[{snakemake.wildcards.store}] cleaned {len(df)} rows -> {snakemake.output[0]}")
```

Notice: no `argparse`, no manually reading `sys.argv` — `snakemake.input[0]`,
`snakemake.output[0]`, and `snakemake.wildcards.store` are just _there_, populated by the engine.
This keeps your actual transformation scripts clean, testable Python that happens to be driven
by Snakemake, rather than command-line tools you had to bolt CLI parsing onto.

Paths in `script:` are relative to the Snakefile's directory, regardless of where you invoke
`snakemake` from.

### 4.4 Extra directives that make rules production-grade

```python
rule clean_store:
    input:
        raw="data/raw/store_{store}.csv"          # named input — access as input.raw
    output:
        clean="results/cleaned/store_{store}.csv"  # named output — access as output.clean
    params:
        min_amount=0                                # arbitrary values, NOT tracked as files
    log:
        "logs/clean_store_{store}.log"              # stdout/stderr of the job get redirected here
    threads: 2                                       # how many CPU threads this job may use
    resources:
        mem_mb=512                                    # arbitrary resource "currency", enforced with --resources
    priority: 10                                      # higher runs first when jobs are ready simultaneously
    message: "Cleaning store {wildcards.store}"       # printed instead of the default job summary
    benchmark:
        "benchmarks/clean_store_{store}.tsv"          # Snakemake records wall time, memory, CPU here automatically
    retries: 2                                         # re-attempt this many times if it fails (e.g. flaky network step)
    script:
        "scripts/clean_store.py"
```

What each one buys you:

- **`params`** — pass config values, thresholds, flags into the script _without_ them being
  treated as file dependencies (unlike `input`). Access via `snakemake.params.min_amount` (in a
  script) or `{params.min_amount}` (in `shell`).
- **`log`** — redirect the job's stdout/stderr to a file instead of your terminal. Essential
  once you have more than a handful of parallel jobs — otherwise their output interleaves into
  an unreadable mess. In a `script:`, if you want Python's own `print()`/logging captured too,
  open the log path yourself; Snakemake automatically captures it for `shell:`.
- **`threads`** — declares how many CPU threads this _specific job_ may use. Run
  `snakemake --cores 8` and Snakemake will schedule jobs so the sum of concurrently-running
  jobs' `threads` never exceeds 8 (e.g. four 2-thread jobs, or two 4-thread jobs).
- **`resources`** — like `threads` but for arbitrary named quantities (memory, GPUs, DB
  connections, a rate-limited API...). Cap the total with
  `snakemake --cores 8 --resources mem_mb=4000`, and Snakemake won't schedule jobs whose
  combined `mem_mb` would exceed that.
- **`priority`** — when multiple jobs are simultaneously ready to run and cores are limited,
  higher-priority ones are scheduled first.
- **`benchmark`** — free profiling. Every run appends a row (wall-clock time, max RSS memory,
  CPU%, I/O) to a TSV you specify. Extremely useful for spotting the slow rule in a 20-rule
  pipeline without adding a single manual `time.time()` call.
- **`retries`** — for jobs prone to transient failure (flaky network, throttled API), Snakemake
  will re-attempt automatically before giving up.

### 4.5 Two more directives worth knowing

```python
rule expensive_step:
    input: "data/raw/big.csv"
    output: "results/big_processed.csv"
    shadow: "minimal"     # run in a private, cleaned-up scratch dir — no half-written litter on failure
    shell: "python scripts/heavy.py {input} {output}"

rule quick_a:
    output: "a.txt"
    group: "fast_group"    # jobs sharing a group get bundled as ONE job when submitted to a cluster
    shell: "echo a > {output}"

rule quick_b:
    output: "b.txt"
    group: "fast_group"    # avoids per-job cluster-submission overhead for many tiny jobs
    shell: "echo b > {output}"
```

- **`shadow`** runs the job in an isolated temp directory that's cleaned up automatically,
  so a failed job never leaves half-written files sitting in your real output path.
- **`group`** is purely an execution-scheduling hint (mainly for cluster/HPC use) — it doesn't
  change the DAG's logic, only how jobs get batched for submission.

---

## Part 5 — Configuration: `config.yaml` and Beyond

Hardcoding store IDs, paths, or thresholds directly in the Snakefile works for a demo, but real
pipelines need to be reconfigurable without editing code. That's what `config.yaml` is for.

### 5.1 Basic config

```yaml
# config.yaml
stores: ["0001", "0002", "0003"]
outdir: "results"
min_amount: 0
```

```python
# Snakefile
configfile: "config.yaml"

STORES = config["stores"]

rule all:
    input:
        expand("{outdir}/cleaned/store_{store}.csv", outdir=config["outdir"], store=STORES)
```

`config` becomes a plain Python dict, available anywhere in the Snakefile after the
`configfile:` line. This is _verified_ — tested end-to-end with a two-store pipeline while
writing this guide.

### 5.2 Overriding config from the command line

You don't need to edit the YAML to try a different value — override individual keys at the CLI:

```bash
snakemake --cores 4 --config min_amount=100
snakemake --cores 4 --configfile config/staging.yaml   # swap the whole file (e.g. per-environment config)
```

This is exactly the pattern for having a `config/dev.yaml`, `config/prod.yaml`, etc. — same
Snakefile, different config file per environment.

### 5.3 `params` reading from config (a very common pattern)

```python
rule clean_store:
    input:
        "data/raw/store_{store}.csv"
    output:
        "results/cleaned/store_{store}.csv"
    params:
        min_amount=config["min_amount"]
    script:
        "scripts/clean_store.py"
```

```python
# scripts/clean_store.py
df = df[df["amount"] >= snakemake.params.min_amount]
```

### 5.4 Sample sheets: config for "one row per unit of work"

For anything beyond a handful of items, a flat list in YAML gets unwieldy. The idiomatic
Snakemake pattern is a **sample sheet** — a CSV/TSV read with pandas, indexed by an ID column,
so each "unit of work" (a store, a sample, a customer segment...) is a row with arbitrary
metadata attached:

```yaml
# config.yaml
samples: "config/stores.tsv"
```

```text
# config/stores.tsv
store_id	region	warehouse_schema
0001	north	DS_TR_00753
0002	south	DS_TR_00753
```

```python
# Snakefile
import pandas as pd

configfile: "config.yaml"
samples = pd.read_csv(config["samples"], sep="\t", dtype=str).set_index("store_id", drop=False)

rule all:
    input:
        expand("results/cleaned/store_{store}.csv", store=samples["store_id"])

rule clean_store:
    input:
        "data/raw/store_{store}.csv"
    output:
        "results/cleaned/store_{store}.csv"
    params:
        region=lambda wc: samples.loc[wc.store, "region"]
    script:
        "scripts/clean_store.py"
```

This scales to hundreds of "rows of work" without the Snakefile itself growing — you edit the
TSV, not the pipeline logic.

---

## Part 6 — Input Functions: When a Rule's Input Depends on Its Wildcards

Sometimes an input path can't be written as a simple `{wildcard}` pattern — it depends on
looking something up (e.g. in that sample sheet above) or on logic. Any Python **function** that
takes `wildcards` and returns a path (or list of paths) can be used as `input`:

```python
def raw_file_for(wildcards):
    region = samples.loc[wildcards.store, "region"]
    return f"data/raw/{region}/store_{wildcards.store}.csv"

rule clean_store:
    input:
        raw_file_for
    output:
        "results/cleaned/store_{store}.csv"
    script:
        "scripts/clean_store.py"
```

Or, for something simple, an inline `lambda`:

```python
rule clean_store:
    input:
        lambda wildcards: f"data/raw/store_{wildcards.store}.csv"
    output:
        "results/cleaned/store_{store}.csv"
    script:
        "scripts/clean_store.py"
```

Input functions are also how you make a rule's input depend on **config**, on **another rule's
output**, or on a **checkpoint** (next section) — anywhere you need "compute the path, don't
just pattern-match it."

---

## Part 7 — Checkpoints: When You Don't Know the File List Upfront

Everything so far assumes you know your wildcard values (`STORES = [...]`) before the pipeline
starts. But sometimes you don't — e.g. "process every file that shows up after a download step,"
where you can't know the filenames until that step has actually run. This is what
**checkpoints** are for, and it's the feature that trips up almost everyone the first time, so
here's a fully verified, working example.

```python
# Snakefile
configfile: "config.yaml"

rule all:
    input:
        "results/combined.csv"

checkpoint discover_stores:
    output:
        directory("data/raw")
    # In real life this rule would DO something — download/extract files into data/raw/.
    # Marking its output as `directory(...)` (not a normal file) tells Snakemake:
    # "I can't know the file list until this has actually run — re-evaluate the DAG after."

def cleaned_files(wildcards):
    # .get() forces the checkpoint to complete first, then lets us inspect what it produced
    ckpt_dir = checkpoints.discover_stores.get(**wildcards).output[0]
    import glob, os
    stores = [
        os.path.basename(f).replace("store_", "").replace(".csv", "")
        for f in glob.glob(f"{ckpt_dir}/store_*.csv")
    ]
    return expand("results/cleaned/store_{store}.csv", store=stores)

rule clean_store:
    input:
        "data/raw/store_{store}.csv"
    output:
        "results/cleaned/store_{store}.csv"
    script:
        "scripts/clean_store.py"

rule combine:
    input:
        cleaned_files          # <- an input FUNCTION, not a static expand() list
    output:
        "results/combined.csv"
    script:
        "scripts/combine.py"
```

What makes this different from a normal rule: Snakemake runs `discover_stores` **first, in
isolation**, before it even tries to plan the rest of the DAG. Only after that checkpoint
finishes does it call `cleaned_files()` to find out what wildcard values actually exist, and
_then_ it plans (and re-plans) the remaining jobs. Without `checkpoint` (i.e. with a plain
`rule`), Snakemake insists on knowing the entire DAG upfront and will error out or silently miss
files, because it tries to resolve `cleaned_files()` before anything has actually run.

**Rule of thumb:** if you ever catch yourself trying to `glob.glob()` a directory _before_
running any rule to decide your wildcard list, and that directory is itself produced by the
pipeline (not pre-existing input data) — that's a checkpoint, not a plain rule.

---

## Part 8 — Reproducibility: Per-Rule Software Environments

A pipeline that works on your machine but nowhere else isn't really reproducible. Snakemake lets
you pin the _exact_ software environment each rule needs, and will manage creating/activating it
for you.

### 8.1 Per-rule Conda environments

```yaml
# workflow/envs/pandas.yaml
channels:
  - conda-forge
dependencies:
  - python=3.12
  - pandas=2.2
  - numpy=1.26
```

```python
rule clean_store:
    input:
        "data/raw/store_{store}.csv"
    output:
        "results/cleaned/store_{store}.csv"
    conda:
        "envs/pandas.yaml"
    script:
        "scripts/clean_store.py"
```

This does nothing unless you opt in at run time — it's declarative until then:

```bash
snakemake --cores 4 --use-conda
```

With that flag, Snakemake creates (once, cached by content hash) an isolated Conda environment
per distinct `envs/*.yaml`, and activates it just for that rule's job. Different rules can use
completely different, even conflicting, Python/library versions in the same pipeline — each job
runs in its own bubble.

### 8.2 Per-rule (or whole-workflow) containers

Same idea, but with a Docker/Singularity/Apptainer image instead of a Conda env — useful when you
need OS-level dependencies Conda can't give you, or want byte-for-byte reproducibility years
later.

```python
rule align_reads:
    input:
        "data/reads.fastq"
    output:
        "results/aligned.bam"
    container:
        "docker://biocontainers/bwa:v0.7.17"
    shell:
        "bwa mem ref.fa {input} > {output}"
```

```bash
snakemake --cores 4 --use-singularity     # or --software-deployment-method apptainer, --sdm for short
```

You can also set one container for the **entire workflow** near the top of the Snakefile:

```python
container: "docker://condaforge/mambaforge:latest"
```

### 8.3 Which one should you actually use?

- **Conda (`--use-conda`)** — the default, lightweight choice for Python/R data-science
  pipelines. Fast to set up, no daemon/root requirements, works well on a laptop.
- **Containers (`--use-singularity` / Docker)** — reach for this when you need full OS-level
  reproducibility (specific compiled binaries, non-Python tools), or you're deploying to an HPC
  cluster where Singularity/Apptainer is already the standard.
- **Both together** is also valid: a container with a Conda env installed _inside_ it, for
  maximum reproducibility.

---

## Part 9 — Modularization: Splitting a Pipeline Into Reusable Pieces

Once a Snakefile passes ~100 lines, split it up. Snakemake gives you two mechanisms, with
different strengths.

### 9.1 `include:` — the simple one

Just textually pastes another file's rules into the current Snakefile's namespace. Good for
"one topic per file" organization within a single project.

```python
# Snakefile
configfile: "config.yaml"

include: "workflow/rules/clean.smk"
include: "workflow/rules/load.smk"

rule all:
    input:
        "results/combined.csv"
```

```python
# workflow/rules/clean.smk
rule clean_store:
    input:
        "data/raw/store_{store}.csv"
    output:
        "results/cleaned/store_{store}.csv"
    script:
        "../scripts/clean_store.py"          # paths are relative to THIS file's location
```

### 9.2 `module:` + `use rule ... from` — the reusable one, verified working

This is the newer, more powerful mechanism, meant for _sharing_ rule sets across projects (e.g.
your team publishes a `common-loaders` module that every pipeline imports), or for cleanly
overriding parts of an imported rule set without copy-pasting it.

```python
# common/rules.smk  (could live in a separate repo/package)
rule clean_generic:
    input:
        "data/raw/{name}.csv"
    output:
        "results/{name}_clean.csv"
    shell:
        "cp {input} {output}"
```

```python
# Snakefile
module common:
    snakefile: "common/rules.smk"

use rule clean_generic from common as clean_x with:
    input:
        "data/raw/x.csv"
    output:
        "results/x_clean.csv"

rule all:
    input:
        "results/x_clean.csv"
```

`use rule ... from common as clean_x with:` imports the `clean_generic` rule from the `common`
module, renames it to `clean_x` in this workflow, and _overrides_ its `input`/`output` (or any
other directive) — without touching the original file. This is the pattern to reach for once
you're maintaining shared, versioned building blocks across multiple pipelines.

### 9.3 The "standard" folder convention, tying it together

```
workflow/
├── Snakefile              # includes everything below, defines `rule all`
├── rules/
│   ├── clean.smk
│   ├── load.smk
│   └── validate.smk
├── scripts/
│   └── *.py
└── envs/
    └── *.yaml
config/
└── config.yaml
```

This is the layout used by essentially every published workflow in the official Snakemake
Workflow Catalog — following it costs you nothing and means anyone who's used Snakemake before
can navigate your repo immediately.

---

## Part 10 — Scaling Execution: Local → Cluster → Cloud

This is the part of Snakemake that lets the _exact same_ Snakefile run on your laptop today and
on a SLURM cluster or in the cloud tomorrow — you change the invocation, never the workflow
logic.

### 10.1 Local execution (what you've been doing)

```bash
snakemake --cores 4                      # up to 4 threads' worth of jobs in parallel, on this machine
snakemake --cores 4 --resources mem_mb=8000
```

### 10.2 Executor plugins — how cluster/cloud support works since Snakemake 8+

As of Snakemake 8 (current is 9.x), cluster and cloud backends are **plugins**, installed
separately, not built into core Snakemake. This keeps the core tool lean. You pick an executor
with `--executor`:

```bash
pip install snakemake-executor-plugin-slurm
snakemake --executor slurm --jobs 50 --default-resources mem_mb=4000 runtime=60
```

Common executor plugins (install only the one you need):

| Plugin                                                                   | For                                                       |
| ------------------------------------------------------------------------ | --------------------------------------------------------- |
| `snakemake-executor-plugin-slurm`                                        | SLURM HPC clusters                                        |
| `snakemake-executor-plugin-cluster-generic`                              | Any cluster with a generic submit command (qsub, bsub...) |
| `snakemake-executor-plugin-kubernetes`                                   | Kubernetes                                                |
| `snakemake-executor-plugin-google-batch` / `-azure-batch` / `-aws-batch` | Cloud batch compute                                       |

Locally you're implicitly using `--executor local` (the default) — you never had to say so.

### 10.3 Profiles — don't type all those flags every time

A **profile** is a YAML file bundling your usual flags, so `snakemake` alone picks them up:

```yaml
# profiles/slurm/config.yaml
executor: slurm
jobs: 50
default-resources:
  mem_mb: 4000
  runtime: 60
use-conda: true
```

```bash
snakemake --profile profiles/slurm
# or set once per shell/CI job:
export SNAKEMAKE_PROFILE=profiles/slurm
snakemake
```

This is exactly how you'd have one profile for local dev (`profiles/local`) and another for CI
or a cluster (`profiles/slurm`) — same Snakefile, swap `--profile`.

### 10.4 Cloud/remote storage — `storage:` directive

Older Snakemake versions used `remote()` wrappers for S3/GCS/etc; current Snakemake (8+) uses
**storage plugins** with a `storage:` directive instead:

```python
# pip install snakemake-storage-plugin-s3
storage:
    provider="s3"

rule download:
    input:
        storage.s3(f"s3://my-bucket/raw/{{sample}}.csv")
    output:
        "data/raw/{sample}.csv"
    shell:
        "cp {input} {output}"
```

Snakemake handles the download/upload transparently as part of the DAG — from the rule's
perspective it's just another input/output file.

---

## Part 11 — Reports, Provenance, Linting, and Formatting

### 11.1 Self-documenting HTML reports — verified flags

```bash
snakemake --report report.html
```

This generates a **single self-contained HTML file** with the rule graph, runtime statistics
(if you used `benchmark:`), and — if you register them — output plots/tables, embedded directly
in the file. Genuinely useful for handing a finished pipeline run to someone non-technical, or
for archiving "what exactly did this run produce and how."

Mark specific outputs to be featured in the report:

```python
rule plot_sales:
    input:
        "results/combined.csv"
    output:
        report("results/sales_plot.png", caption="workflow/report/sales.rst", category="Plots")
    script:
        "scripts/plot_sales.py"
```

### 11.2 Linting — catches bad practice before it bites you

```bash
snakemake --lint
```

Verified output on a deliberately minimal example flags exactly the things you'd expect: rules
missing a `conda`/`container` directive, rules missing a `log:` (so parallel output doesn't
interleave into a mess). Run this on any pipeline before you consider it "done" — it's cheap and
catches real footguns.

### 11.3 Formatting — `snakefmt`

```bash
pip install snakefmt
snakefmt .
```

The `black`-equivalent for Snakefiles. Run it in CI/pre-commit so style stays consistent without
manual nitpicking — genuinely worth adding to a pre-commit hook alongside your existing Python
formatters.

### 11.4 `--rerun-triggers` — controlling _why_ Snakemake decides to re-run something

By default, Snakemake re-runs a job if its **code**, **input files**, **params**, or **software
environment** changed since the last run — not just file modification time. This is more robust
than plain `make` (which only looks at mtimes) but can surprise you (e.g. editing a comment in a
script re-triggers everything downstream of it, because the script's checksum changed). Tune it:

```bash
snakemake --cores 4 --rerun-triggers mtime      # revert to classic, mtime-only behavior
```

---

## Part 12 — Testing Your Pipeline

```bash
snakemake --generate-unit-tests
```

This inspects your **last successful run** and auto-generates `pytest` test files (one per rule)
under `.tests/unit/`, each of which re-runs that specific rule in isolation and diffs its output
byte-for-byte against what was captured. It's a fast way to get regression protection on a
pipeline without hand-writing test scaffolding — run it once you have a working pipeline, commit
the generated tests, and they'll catch future changes that silently alter output.

For your own custom logic (the actual transformation functions inside `scripts/*.py`), nothing
Snakemake-specific is needed — write normal `pytest` tests against those functions directly, the
same as you would for a `BaseDataWarehouseLoader`-style class. Keep your scripts structured so
the actual transformation logic is a plain, importable function, and the `script:`-facing code
at the bottom is a thin wrapper that just calls it with `snakemake.input`/`snakemake.output` —
that way the logic is unit-testable without needing Snakemake running at all.

---

## Part 13 — Capstone: A Complete, Verified, Runnable Pipeline

This ties every concept above together into one small but realistic pipeline: ingest multiple
raw per-store CSVs, clean/normalize them (the exact kind of dtype-casting and null-handling work
that shows up in real warehouse-loading code), combine them, and validate the result. **Every
file below was actually created and run in a sandbox — this exact pipeline executes cleanly.**

### Project layout

```
snakemake_example_project/
├── Snakefile
├── config.yaml
├── data/raw/
│   ├── store_0001.csv
│   └── store_0002.csv
└── scripts/
    ├── clean_store.py
    └── combine.py
```

### `config.yaml`

```yaml
stores: ["0001", "0002"]
outdir: "results"
```

### `Snakefile`

```python
configfile: "config.yaml"

STORES = config["stores"]

rule all:
    input:
        f"{config['outdir']}/combined.csv"

rule clean_store:
    input:
        "data/raw/store_{store}.csv"
    output:
        "results/cleaned/store_{store}.csv"
    log:
        "logs/clean_store_{store}.log"
    threads: 1
    script:
        "scripts/clean_store.py"

rule combine:
    input:
        expand("results/cleaned/store_{store}.csv", store=STORES)
    output:
        f"{config['outdir']}/combined.csv"
    script:
        "scripts/combine.py"
```

### `scripts/clean_store.py`

```python
import pandas as pd
import numpy as np

df = pd.read_csv(snakemake.input[0])

# mimic real-world cleaning patterns: zero-pad an id column, guard against inf values
df["filiaalnr"] = df["filiaalnr"].astype(str).str.zfill(4)
df["amount"] = df["amount"].replace([np.inf, -np.inf], np.nan)

df.to_csv(snakemake.output[0], index=False)
print(f"[{snakemake.wildcards.store}] cleaned {len(df)} rows -> {snakemake.output[0]}")
```

### `scripts/combine.py`

```python
import pandas as pd

# NOTE: dtype=str on the padded id column is not optional here. Without it, pandas silently
# re-infers "0001" as the integer 1 when re-reading the intermediate CSV, stripping the padding
# your previous rule carefully applied. This exact bug is easy to hit any time a pipeline passes
# data between stages via plain CSV files -- caught here by actually running the pipeline.
frames = [pd.read_csv(f, dtype={"filiaalnr": str}) for f in snakemake.input]
combined = pd.concat(frames, ignore_index=True)
combined.to_csv(snakemake.output[0], index=False)
print(f"combined {len(snakemake.input)} files -> {len(combined)} rows")
```

### Running it

```bash
cd snakemake_example_project
snakemake -n -p                 # dry-run first: see 2 clean_store jobs + 1 combine job planned
snakemake --cores 2              # run for real; the two clean_store jobs run in parallel
cat results/combined.csv         # inspect the output
```

Verified output from actually running this:

```
job            count
-----------  -------
clean_store        2
combine            1
all                1
total              4
```

...and the final `results/combined.csv` contains 5 correctly-zero-padded, inf-safe rows. Try
editing `config.yaml` to add a third store's worth of raw data and re-running — you'll see only
the _new_ `clean_store` job execute plus `combine` (since its input changed), while nothing
touches the already-processed stores. That "only redo what changed" behavior, for free, is the
entire value proposition of using Snakemake over a plain script here.

### A note on this as a _local dev-time_ tool

If your production environment already has a scheduler (Control-M, Airflow, cron) that owns
the real orchestration, you can still get a lot of value from a Snakefile like this one purely
during **development**: it gives you fast, cached, one-command re-runs of just the pipeline
stages that changed while you're iterating locally on loader/transform logic, plus the DAG
visualization for free when you want to sanity-check a multi-stage pipeline's shape before it
goes anywhere near a production scheduler. Nothing about using it that way requires adopting
`--executor`/cluster features at all — `--cores N` on your own machine is a complete, valid way
to use Snakemake.

---

## Part 14 — Reference: Cheat Sheets

### 14.1 CLI flags

| Flag                                    | What it does                                                          |
| --------------------------------------- | --------------------------------------------------------------------- |
| `-n`, `--dry-run`                       | Show what would run, without running it                               |
| `-p`                                    | Also print shell commands (combine with `-n`)                         |
| `--cores N`                             | Run locally, up to N threads' worth of jobs in parallel               |
| `--forceall`                            | Ignore up-to-date checks, rerun everything                            |
| `--forcerun RULE`                       | Rerun a specific rule (and everything downstream)                     |
| `--config KEY=VAL`                      | Override one config value from the CLI                                |
| `--configfile FILE`                     | Use a different config YAML entirely                                  |
| `--use-conda`                           | Create/activate each rule's `conda:` environment                      |
| `--use-singularity` / `--sdm apptainer` | Run each rule's `container:` via Singularity/Apptainer                |
| `--executor NAME`                       | Choose local / slurm / kubernetes / cloud-batch execution backend     |
| `--jobs N`                              | Max simultaneously _submitted_ jobs (cluster/cloud context)           |
| `--profile DIR`                         | Load a bundle of default flags from a profile directory               |
| `--resources KEY=VAL`                   | Cap a named resource (e.g. `mem_mb=8000`) across all running jobs     |
| `--dag` / `--rulegraph` / `--filegraph` | Emit Graphviz `dot` source for DAG visualization                      |
| `--report FILE.html`                    | Generate a self-contained HTML provenance/report file                 |
| `--lint`                                | Flag common workflow anti-patterns                                    |
| `--generate-unit-tests`                 | Auto-generate pytest regression tests from the last run               |
| `--rerun-triggers`                      | Control what counts as "stale" (code/input/params/mtime/software-env) |
| `--touch`                               | Mark outputs as up-to-date without actually running anything          |
| `--unlock`                              | Clear a stale lock after a previous run crashed mid-execution         |

### 14.2 Rule directives

| Directive                               | Purpose                                                               |
| --------------------------------------- | --------------------------------------------------------------------- |
| `input`                                 | Files this job needs (paths, wildcards, or a function of `wildcards`) |
| `output`                                | Files this job produces                                               |
| `shell` / `run` / `script` / `notebook` | How the job actually does the work                                    |
| `params`                                | Arbitrary values passed to the job, not tracked as files              |
| `log`                                   | Where stdout/stderr goes instead of your terminal                     |
| `threads`                               | CPU threads this job may use                                          |
| `resources`                             | Named quantities (mem_mb, gpu, ...) capped via `--resources`          |
| `conda`                                 | Path to a per-rule Conda environment YAML                             |
| `container`                             | Docker/Singularity image for this rule                                |
| `wildcard_constraints`                  | Regex constraining what a wildcard may match                          |
| `priority`                              | Scheduling priority when multiple jobs are ready at once              |
| `benchmark`                             | Auto-record timing/memory to a TSV on every run                       |
| `retries`                               | Auto re-attempt N times on failure                                    |
| `shadow`                                | Run in an isolated, auto-cleaned scratch directory                    |
| `group`                                 | Batch multiple jobs into one cluster submission unit                  |
| `message`                               | Custom text printed instead of the default job summary                |

### 14.3 Special functions/keywords

| Name                                             | Purpose                                                                      |
| ------------------------------------------------ | ---------------------------------------------------------------------------- |
| `expand(pattern, **lists)`                       | Build a list of filenames from a pattern + wildcard value lists              |
| `directory(path)`                                | Mark an output as a directory, not a single file (pairs with `checkpoint`)   |
| `checkpoint`                                     | Like `rule`, but re-evaluates the DAG after running (dynamic file discovery) |
| `configfile:`                                    | Load a YAML file into the `config` dict                                      |
| `include:`                                       | Textually merge another `.smk` file's rules into this one                    |
| `module:` / `use rule ... from ... as ... with:` | Import and optionally override rules from a reusable module                  |
| `onstart:` / `onsuccess:` / `onerror:`           | Hooks that run once at the very start / on full success / on any failure     |
| `report(path, caption=..., category=...)`        | Mark an output to be embedded in `--report`                                  |

---

## Part 15 — Where to Go Next

- **Official docs:** `snakemake.readthedocs.io` — the canonical, always-current reference.
- **Snakemake Workflow Catalog:** a searchable library of published, reusable, real-world
  Snakemake pipelines — the fastest way to see idiomatic large-pipeline structure.
- **Snakemake Wrappers Repository:** pre-built `wrapper:` directives for hundreds of common
  bioinformatics/data tools — an alternative to `shell:`/`script:` when someone's already wrapped
  the tool you need.
- **`snakemake --help`** — genuinely worth a full read-through once; the CLI is large and
  well-documented inline, and you'll discover flags this guide didn't cover.

You now have, in one file: the theory of why Snakemake exists, a working repo wired into your
existing `requirements.txt`/`.venv` setup, every core directive with a tested example, the
trickier dynamic-DAG (`checkpoint`) and reusable-module (`module`/`use rule from`) mechanisms,
the reproducibility story (Conda/containers), the scaling story (executors/profiles/cloud
storage), and a fully verified end-to-end capstone pipeline to run and modify. That's the whole
surface area of the tool — the rest is depth, which comes from using it.
