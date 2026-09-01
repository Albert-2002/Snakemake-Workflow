rule all:
    input: "data/processed/clean_transactions.csv"

rule clean_data:
    input: "data/raw/raw_transactions.csv"
    output: "data/processed/clean_transactions.csv"
    shell: "python workflow/scripts/clean.py {input} {output}"
