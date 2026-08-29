# Snakefile
rule hello:
    output:
        "hello.txt",
    shell:
        "echo 'Hello, world!' > {output}"
