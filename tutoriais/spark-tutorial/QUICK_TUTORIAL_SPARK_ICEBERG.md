# Quick Tutorial: Lakehouse com Spark + Iceberg

> So os comandos. Explicacoes: `TUTORIAL_SPARK_ICEBERG.md`.
> Pre-requisito: ambiente pronto e validado para Iceberg
> (`QUICK_TUTORIAL_AMBIENTE_SPARK.md`, secao 3).

---

## 1. Versao interativa (pyspark)

Na pasta `spark-tutorial/`, com o venv ativado:

```bash
pyspark \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=warehouse
```

```python
from pyspark.sql import functions as F
spark.sparkContext.setLogLevel("ERROR")

# ETL (mesmo do tutorial DataFrame)
def padronizar(df):
    return (
        df
        .withColumnRenamed("Rank", "rank")
        .withColumnRenamed("Full Journal Title", "titulo")
        .withColumnRenamed("Abbreviated Title", "titulo_abreviado")
        .withColumnRenamed("ISSN", "issn")
        .withColumnRenamed("Total Cites", "total_citacoes")
        .withColumnRenamed("Journal Impact Factor", "fator_impacto")
        .withColumn("issn", F.trim(F.col("issn")))
        .withColumn("total_citacoes", F.col("total_citacoes").cast("int"))
        .withColumn("fator_impacto", F.col("fator_impacto").cast("double"))
        .dropna(subset=["fator_impacto"])
    )

df_jcr = padronizar(spark.read.csv("data/jcr.csv", header=True, inferSchema=True))
df_scimago = padronizar(spark.read.csv("data/scimago.csv", header=True, inferSchema=True))
df_periodicos = (
    df_jcr.alias("j").join(df_scimago.alias("s"), on="issn", how="inner")
    .select(
        F.col("issn"),
        F.col("j.titulo").alias("titulo"),
        F.col("j.total_citacoes").alias("citacoes_jcr"),
        F.col("s.total_citacoes").alias("citacoes_scimago"),
        F.col("j.fator_impacto").alias("impacto_jcr"),
        F.col("s.fator_impacto").alias("impacto_scimago"),
    )
)

# Criar tabela Iceberg
df_periodicos.writeTo("local.db.periodicos").createOrReplace()
spark.sql("SELECT COUNT(*) FROM local.db.periodicos").show()    # 29808

# UPDATE
spark.sql("""UPDATE local.db.periodicos SET impacto_jcr = 999.9
             WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'""")
spark.sql("""SELECT titulo, impacto_jcr FROM local.db.periodicos
             WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'""").show(truncate=False)
# 999.9 (2 linhas)

# Snapshots
spark.sql("""SELECT snapshot_id, committed_at, operation
             FROM local.db.periodicos.snapshots ORDER BY committed_at""").show(truncate=False)
# 2 snapshots (criacao + update)

# Time travel
snap_criacao = spark.sql("""SELECT snapshot_id FROM local.db.periodicos.snapshots
                            ORDER BY committed_at LIMIT 1""").first()["snapshot_id"]
spark.sql(f"""SELECT titulo, impacto_jcr
              FROM local.db.periodicos VERSION AS OF {snap_criacao}
              WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'""").show(truncate=False)
# 503.1 (valor ANTES do update)
exit()
```

Arquivos no disco:

```bash
ls warehouse/db/periodicos/data/ warehouse/db/periodicos/metadata/
```

---

## 2. Versao script

Script em: `spark-tutorial/scripts/exercicio_iceberg.py`

```bash
# spark-submit (CLI oficial):
spark-submit scripts/exercicio_iceberg.py

# ou via Python do venv:
python scripts/exercicio_iceberg.py
```

> Se voce acabou de rodar a versao interativa no mesmo warehouse, o historico mostrara
> 4 snapshots (2 de la + 2 do script) - o script recria a tabela, mas os snapshots
> anteriores permanecem.

**Linhas-chave esperadas** (resumo - a saida real mostra tabelas completas):
```
Linhas a gravar na tabela Iceberg: 29808
Antes do UPDATE: 503.1 | Depois do UPDATE: 999.9
2 snapshots | TIME TRAVEL -> 503.1 | atual -> 999.9
Exercicio Iceberg concluido!
```

---

## 3. Via Docker

```bash
docker exec spark /opt/spark/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2 \
  scripts/exercicio_iceberg.py

ls docker/warehouse/db/periodicos/metadata/
```

---

## 4. Via Jupyter Notebook

No Jupyter, o Iceberg e configurado via `SparkSession.builder` (sem flags `--packages`).

```bash
pip install jupyter   # so na primeira vez
jupyter notebook      # abre http://localhost:8888
```

Novo notebook > **New > Python 3** > execute celula a celula (**Shift+Enter**):

```python
# Celula 1 — setup com Iceberg
from pyspark.sql import SparkSession, functions as F
spark = (SparkSession.builder.appName("IcebergNB").master("local[*]")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.driver.host", "127.0.0.1")
         .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.local.type", "hadoop")
         .config("spark.sql.catalog.local.warehouse", "warehouse")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print(f"Spark {spark.version} + Iceberg prontos!")
# Na 1a execucao: download do Iceberg aparece no terminal do jupyter (~100 MB)
```

```python
# Celula 2 — ETL e criacao da tabela Iceberg
def padronizar(df):
    return (df.withColumnRenamed("Rank","rank").withColumnRenamed("Full Journal Title","titulo")
              .withColumnRenamed("Abbreviated Title","titulo_abreviado").withColumnRenamed("ISSN","issn")
              .withColumnRenamed("Total Cites","total_citacoes").withColumnRenamed("Journal Impact Factor","fator_impacto")
              .withColumn("issn", F.trim(F.col("issn")))
              .withColumn("total_citacoes", F.col("total_citacoes").cast("int"))
              .withColumn("fator_impacto", F.col("fator_impacto").cast("double"))
              .dropna(subset=["fator_impacto"]))

df_jcr = padronizar(spark.read.csv("data/jcr.csv", header=True, inferSchema=True))
df_scimago = padronizar(spark.read.csv("data/scimago.csv", header=True, inferSchema=True))
df_periodicos = (df_jcr.alias("j").join(df_scimago.alias("s"), on="issn", how="inner")
                 .select("issn", F.col("j.titulo").alias("titulo"),
                         F.col("j.fator_impacto").alias("impacto_jcr"),
                         F.col("s.fator_impacto").alias("impacto_scimago")))
df_periodicos.writeTo("local.db.periodicos_nb").createOrReplace()
spark.sql("SELECT COUNT(*) FROM local.db.periodicos_nb").show()  # 29808
```

```python
# Celula 3 — UPDATE, snapshots e time travel
spark.sql("UPDATE local.db.periodicos_nb SET impacto_jcr = 999.9 WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'")
spark.sql("SELECT snapshot_id, operation FROM local.db.periodicos_nb.snapshots ORDER BY committed_at").show()
snap = spark.sql("SELECT snapshot_id FROM local.db.periodicos_nb.snapshots ORDER BY committed_at LIMIT 1").first()["snapshot_id"]
spark.sql(f"SELECT titulo, impacto_jcr FROM local.db.periodicos_nb VERSION AS OF {snap} WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'").show(truncate=False)
# ^ deve mostrar 503.1 (valor antes do UPDATE)
```

```python
# Celula 4 — encerrar
spark.stop()
```
