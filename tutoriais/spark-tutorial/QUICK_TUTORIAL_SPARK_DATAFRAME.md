# Quick Tutorial: ETL com Spark DataFrames (JCR x Scimago)

> So os comandos. Explicacoes: `TUTORIAL_SPARK_DATAFRAME.md`.
> Pre-requisito: ambiente pronto (`QUICK_TUTORIAL_AMBIENTE_SPARK.md`).

---

## 1. Versao interativa (pyspark)

Na pasta `spark-tutorial/`, com o venv ativado:

```bash
pyspark
```

```python
from pyspark.sql import functions as F
spark.sparkContext.setLogLevel("ERROR")

# Leitura
df_jcr = spark.read.csv("data/jcr.csv", header=True, inferSchema=True)
df_scimago = spark.read.csv("data/scimago.csv", header=True, inferSchema=True)
df_jcr.count(), df_scimago.count()               # (38501, 46905)

# Padronizacao
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
    )

df_jcr = padronizar(df_jcr)
df_scimago = padronizar(df_scimago)

# Nulos
df_scimago.filter(F.col("fator_impacto").isNull()).count()   # 310
df_scimago = df_scimago.dropna(subset=["fator_impacto"])
df_scimago.count()                                            # 46595

# Union
df_uniao = (
    df_jcr.withColumn("fonte", F.lit("jcr"))
    .unionByName(df_scimago.withColumn("fonte", F.lit("scimago")))
)
df_uniao.count()                                              # 85096

# Join por ISSN
df_join = (
    df_jcr.alias("j")
    .join(df_scimago.alias("s"), on="issn", how="inner")
    .select(
        F.col("issn"),
        F.col("j.titulo").alias("titulo"),
        F.col("j.total_citacoes").alias("citacoes_jcr"),
        F.col("s.total_citacoes").alias("citacoes_scimago"),
        F.col("j.fator_impacto").alias("impacto_jcr"),
        F.col("s.fator_impacto").alias("impacto_scimago"),
    )
)
df_join.count()                                               # 29808

# Agregacoes
df_uniao.groupBy("fonte").agg(
    F.count("*").alias("qtd_linhas"),
    F.round(F.avg("fator_impacto"), 2).alias("impacto_medio"),
).show()
# jcr 38501 / 2.29   |   scimago 46595 / 0.66

# SQL
df_join.createOrReplaceTempView("periodicos")
spark.sql("""
    SELECT titulo, ABS(citacoes_jcr - citacoes_scimago) AS diferenca
    FROM periodicos ORDER BY diferenca DESC LIMIT 5
""").show(truncate=False)
# NATURE 1337 ... SCIENCE 1286 ...

# Escrita
df_join.write.mode("overwrite").parquet("output/periodicos_parquet")
df_join.write.mode("overwrite").option("header", True).csv("output/periodicos_csv")
spark.read.parquet("output/periodicos_parquet").count()       # 29808
exit()
```

---

## 2. Versao script

Script em: `spark-tutorial/scripts/exercicio_dataframe.py`

```bash
# spark-submit (CLI oficial):
spark-submit scripts/exercicio_dataframe.py

# ou via Python do venv:
python scripts/exercicio_dataframe.py
```

**Linhas-chave esperadas**: `38501 / 46905 / 310 / 46595 / 85096 / 29808 / 29808`.

```bash
ls output/periodicos_parquet/ output/periodicos_csv/
```

---

## 3. Via Docker

```bash
docker exec spark /opt/spark/bin/spark-submit scripts/exercicio_dataframe.py
ls docker/output/periodicos_parquet/
```

---

## 4. Via Jupyter Notebook

```bash
pip install jupyter   # so na primeira vez
jupyter notebook      # abre http://localhost:8888
```

Novo notebook > **New > Python 3** > execute celula a celula (**Shift+Enter**):

```python
# Celula 1 — setup
from pyspark.sql import SparkSession, functions as F
spark = (SparkSession.builder.appName("DataFrameNB").master("local[*]")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.driver.host", "127.0.0.1").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
```

```python
# Celula 2 — leitura e padronizacao
def padronizar(df):
    return (df.withColumnRenamed("Rank","rank").withColumnRenamed("Full Journal Title","titulo")
              .withColumnRenamed("Abbreviated Title","titulo_abreviado").withColumnRenamed("ISSN","issn")
              .withColumnRenamed("Total Cites","total_citacoes").withColumnRenamed("Journal Impact Factor","fator_impacto")
              .withColumn("issn", F.trim(F.col("issn")))
              .withColumn("total_citacoes", F.col("total_citacoes").cast("int"))
              .withColumn("fator_impacto", F.col("fator_impacto").cast("double")))

df_jcr = padronizar(spark.read.csv("data/jcr.csv", header=True, inferSchema=True))
df_scimago = padronizar(spark.read.csv("data/scimago.csv", header=True, inferSchema=True))
print(df_jcr.count(), df_scimago.count())   # 38501  46905
```

```python
# Celula 3 — nulos, union e join
df_scimago = df_scimago.dropna(subset=["fator_impacto"])
df_uniao = df_jcr.withColumn("fonte",F.lit("jcr")).unionByName(df_scimago.withColumn("fonte",F.lit("scimago")))
print(df_uniao.count())   # 85096
df_join = (df_jcr.alias("j").join(df_scimago.alias("s"), on="issn", how="inner")
           .select("issn", F.col("j.titulo").alias("titulo"),
                   F.col("j.fator_impacto").alias("impacto_jcr"),
                   F.col("s.fator_impacto").alias("impacto_scimago")))
print(df_join.count())    # 29808
df_join.show(3, truncate=False)
```

```python
# Celula 4 — gravar e encerrar
df_join.write.mode("overwrite").parquet("output/periodicos_parquet_nb")
print("Salvo em output/periodicos_parquet_nb/")
spark.stop()
```
