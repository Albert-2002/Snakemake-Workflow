import pandas as pd

def categories_from_clean_data(wildcards):
    clean_path = checkpoints.clean_data.get(**wildcards).output[0]
    df = pd.read_csv(clean_path)
    categories = sorted(df["product"].unique())
    return expand(
        "data/processed/category_sales_{article_category}.csv",
        article_category=categories,
    )

rule all:
    input: categories_from_clean_data

checkpoint clean_data:
    input: "data/raw/raw_transactions.csv"
    output: "data/processed/clean_transactions.csv"
    shell: "python workflow/scripts/clean.py {input} {output}"

rule category_sales:
    input: "data/processed/clean_transactions.csv"
    output: "data/processed/category_sales_{article_category}.csv"
    shell: "python workflow/scripts/aggregate.py {input} {output} {wildcards.article_category}"
