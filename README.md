# Snakemake-Workflow

Snakemake is a workflow management system that allows you to define and execute complex data pipelines in a reproducible and scalable manner.

---

## The DAG (Directed Acyclic Graph)

Every Snakemake workflow under the hood is a Directed Acyclic Graph:

- **Nodes**: individual jobs (a rule applied to specific input/output files)
- **Edges**: "this job's output is that job's input"

Snakemake's entire job is to **build** this graph from your **rules**, then **execute** it — in **parallel** where possible, **skipping** already-up-to-date nodes, and **stopping/reporting** clearly if something fails.

---

## Strengths

- **File Centric**: A job only exisits because it's output file does not exist or is stale.
- **Generalizable**: Rules can be a Python script, a shell command, or any executable.
- **Reproducible**: Logging, reporting, DAG visualization, and consistent execution ensure that workflows can be reliably reproduced.
- **Scalable**: Snakemake can efficiently manage workflows from a single machine to large computing clusters.
- **Laziness**: Jobs are only executed when their output is required, avoiding unnecessary computation.

---

## Anatomy of a rule

```Python
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

To be read as - To create _'results/cleaned/sales.csv'_ the workflow requires _'data/raw/sales.csv'_,
and here is the shell command that turns one into the other. Here {input} and {output} are placeholders that Snakemake fills in automatically (Python's .format() syntax under the hood).

Snakemake sees you want _'results/cleaned/sales.csv'_. It scans all rules for one whose output matches that path — finds **clean_data** — and checks: does _'data/raw/sales.csv'_ (its input) exist? If yes, and if the output either doesn't exist yet or is older than the input, it runs the rule. If the output already exists and is newer than every input, Snakemake does nothing — this is the laziness/caching behavior that makes re-running a big pipeline after a small change fast.

### rule all

If you do not mention which file you want, Snakemake **builds the first rule** in the Snakefile by default. To explicitly specify the final targets of your workflow, you can define a special rule called **rule all**. This rule lists all the files that should be generated when the workflow is complete. For example:

```Python
# Example of rule all
rule all:
    input:
        "results/cleaned/sales.csv"
# Other final targets can be added here as needed
```

### Essential - CLI Flags

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

### Visualize the DAG

```bash
# Requires graphviz installed (e.g. `apt install graphviz` / `brew install graphviz`)
snakemake --dag | dot -Tpng > dag.png     # per-job DAG (one box per file produced)
snakemake --rulegraph | dot -Tpng > rulegraph.png   # per-rule DAG (one box per rule, no wildcard duplication)
snakemake --filegraph | dot -Tpng > filegraph.png   # per-file, but grouped/simplified
```

---
