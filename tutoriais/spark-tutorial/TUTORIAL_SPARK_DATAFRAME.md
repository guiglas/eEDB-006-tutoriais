# Tutorial: ETL com Spark DataFrames (JCR x Scimago)

> Exercicio de engenharia de dados com a **API DataFrame** do Spark: ler dois rankings
> de periodicos cientificos (`jcr.csv` e `scimago.csv`), tratar e padronizar os dados,
> combinar as duas fontes (union e join), responder perguntas com agregacoes e SQL, e
> gravar o resultado em Parquet e CSV.

**Pre-requisito**: ambiente montado conforme o `TUTORIAL_AMBIENTE_SPARK.md`.
Recomendado: ter feito o `TUTORIAL_SPARK_RDD.md` antes.

---

## Sumario

1. [Conceitos: DataFrame vs RDD](#1-conceitos-dataframe-vs-rdd)
2. [Conhecendo os dados](#2-conhecendo-os-dados)
3. [Passo a passo no shell interativo](#3-passo-a-passo-no-shell-interativo)
   - [3.1 Leitura dos CSVs](#31-leitura-dos-csvs)
   - [3.2 Explorando schema e dados](#32-explorando-schema-e-dados)
   - [3.3 Padronizacao de colunas e tipos](#33-padronizacao-de-colunas-e-tipos)
   - [3.4 Tratamento de nulos](#34-tratamento-de-nulos)
   - [3.5 Union - empilhando as fontes](#35-union---empilhando-as-fontes)
   - [3.6 Join - cruzando as fontes pelo ISSN](#36-join---cruzando-as-fontes-pelo-issn)
   - [3.7 Agregacoes](#37-agregacoes)
   - [3.8 Spark SQL](#38-spark-sql)
   - [3.9 Gravando em Parquet e CSV](#39-gravando-em-parquet-e-csv)
4. [O script completo (spark-submit)](#4-o-script-completo-spark-submit)
5. [Executando no Docker](#5-executando-no-docker)
6. [Via Jupyter Notebook](#6-via-jupyter-notebook)
7. [Desafios extras](#7-desafios-extras)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Conceitos: DataFrame vs RDD

Um **DataFrame** e uma colecao distribuida de dados organizada em **colunas nomeadas e
tipadas** - como uma tabela de banco de dados, porem particionada e processada em
paralelo (por baixo, continua sendo um RDD).

| | RDD | DataFrame |
|---|---|---|
| Estrutura | objetos Python soltos | linhas com schema (colunas + tipos) |
| Otimizacao | nenhuma - o Spark executa o que voce escreveu | **Catalyst** reescreve e otimiza seu plano |
| Sintaxe | funcional (`map`, `reduce`, lambdas) | declarativa (`select`, `groupBy`, SQL) |
| Performance em Python | mais lenta (serializa objetos Python) | rapida (execucao fica na JVM) |
| Quando usar | dados nao estruturados, controle fino | dados tabulares - **regra geral: prefira DataFrame** |

> **Conceito - Catalyst**: o otimizador do Spark. Como o DataFrame conhece o schema e as
> operacoes sao declarativas ("o que" e nao "como"), o Spark reordena filtros, elimina
> colunas nao usadas e escolhe algoritmos de join - igual a um banco de dados faz.
> A avaliacao continua **lazy**: `show()`, `count()` e `write` sao as acoes que disparam
> a execucao.

---

## 2. Conhecendo os dados

Dois rankings de periodicos cientificos (ano-base 2020), ja padronizados com as mesmas
colunas:

| Arquivo | Fonte | Linhas | O que e |
|---|---|---|---|
| `data/jcr.csv` | JCR - Journal Citation Reports (Clarivate) | 38.501 | Ranking com **Journal Impact Factor** |
| `data/scimago.csv` | Scimago Journal Rank (Scopus/Elsevier) | 46.905 | Ranking com indicador **SJR** |

Colunas (iguais nos dois): `Rank`, `Full Journal Title`, `Abbreviated Title`, `ISSN`,
`Total Cites`, `Journal Impact Factor`.

```bash
head -3 data/jcr.csv
```

**Resultado esperado**:
```
Rank,Full Journal Title,Abbreviated Title,ISSN,Total Cites,Journal Impact Factor
1,CA-A CANCER JOURNAL FOR CLINICIANS,CA-CANCER J CLIN,00079235,297,503.1
1,CA-A CANCER JOURNAL FOR CLINICIANS,CA-CANCER J CLIN,15424863,297,503.1
```

Repare: o MESMO periodico aparece em mais de uma linha, uma para cada **ISSN** (um
periodico pode ter ISSN impresso e eletronico). O ISSN sera nossa chave de cruzamento.

> **Pergunta de negocio do exercicio**: como os dois rankings "enxergam" os mesmos
> periodicos? Quais periodicos estao nas duas bases e quao diferentes sao seus
> indicadores?

---

## 3. Passo a passo no shell interativo

Na pasta `spark-tutorial/`, com o venv ativado, abra o shell:

```bash
pyspark
```

> No Docker: `docker exec -it spark /opt/spark/bin/pyspark`

E importe as funcoes de coluna (usadas o tempo todo com DataFrames):

```python
from pyspark.sql import functions as F
spark.sparkContext.setLogLevel("ERROR")
```

### 3.1 Leitura dos CSVs

```python
df_jcr = spark.read.csv("data/jcr.csv", header=True, inferSchema=True)
df_scimago = spark.read.csv("data/scimago.csv", header=True, inferSchema=True)
```

> **Conceitos**:
> - `header=True`: a primeira linha do arquivo vira o nome das colunas.
> - `inferSchema=True`: o Spark le o arquivo uma vez so para descobrir os tipos
>   (int, double, string). Sem isso, TUDO vira string. (Em producao, com arquivos
>   grandes, prefira declarar o schema na mao - evita essa leitura extra.)

### 3.2 Explorando schema e dados

```python
df_jcr.printSchema()
```

**Resultado esperado**:
```
root
 |-- Rank: integer (nullable = true)
 |-- Full Journal Title: string (nullable = true)
 |-- Abbreviated Title: string (nullable = true)
 |-- ISSN: string (nullable = true)
 |-- Total Cites: double (nullable = true)
 |-- Journal Impact Factor: double (nullable = true)
```

> Repare que `Total Cites` foi inferido como **double** - o inferSchema decide pelo
> conteudo e nem sempre acerta o tipo "ideal". Vamos corrigir na padronizacao.

```python
df_jcr.count(), df_scimago.count()
```

**Resultado esperado**:
```
(38501, 46905)
```

```python
df_jcr.show(3)
```

**Resultado esperado**:
```
+----+--------------------+-------------------+--------+-----------+---------------------+
|Rank|  Full Journal Title|  Abbreviated Title|    ISSN|Total Cites|Journal Impact Factor|
+----+--------------------+-------------------+--------+-----------+---------------------+
|   1|CA-A CANCER JOURN...|   CA-CANCER J CLIN|00079235|      297.0|                503.1|
|   1|CA-A CANCER JOURN...|   CA-CANCER J CLIN|15424863|      297.0|                503.1|
|   2|NATURE REVIEWS DR...|NAT REV DRUG DISCOV|14741776|      114.9|                122.7|
+----+--------------------+-------------------+--------+-----------+---------------------+
only showing top 3 rows
```

### 3.3 Padronizacao de colunas e tipos

Nomes com espaco ("Full Journal Title") obrigam aspas/backticks em todo lugar e quebram
em alguns formatos. Padrao de mercado: **snake_case**. Como as duas fontes tem as mesmas
colunas, criamos UMA funcao e aplicamos nas duas:

```python
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
df_jcr.printSchema()
```

**Resultado esperado**:
```
root
 |-- rank: integer (nullable = true)
 |-- titulo: string (nullable = true)
 |-- titulo_abreviado: string (nullable = true)
 |-- issn: string (nullable = true)
 |-- total_citacoes: integer (nullable = true)
 |-- fator_impacto: double (nullable = true)
```

> **Conceitos**:
> - `withColumnRenamed(velho, novo)`: renomeia uma coluna.
> - `withColumn(nome, expressao)`: cria OU substitui uma coluna a partir de uma expressao.
> - `F.col("x")`: referencia a coluna "x" dentro de expressoes.
> - `F.trim(...)`: remove espacos nas pontas (o scimago tem ISSNs como `" 00079235"`).
> - `.cast("int")`: converte o tipo - o `total_citacoes` volta a ser inteiro.
> - DataFrames sao **imutaveis**: cada operacao retorna um DataFrame NOVO, por isso
>   reatribuimos (`df_jcr = padronizar(df_jcr)`).

### 3.4 Tratamento de nulos

Nem todo periodico do Scimago tem fator de impacto calculado. Quantos sao?

```python
df_scimago.filter(F.col("fator_impacto").isNull()).count()
```

**Resultado esperado**:
```
310
```

Para este exercicio, linhas sem o indicador nao servem - descartamos:

```python
df_scimago = df_scimago.dropna(subset=["fator_impacto"])
df_scimago.count()
```

**Resultado esperado**:
```
46595
```

(46.905 - 310 = 46.595.)

> **Conceito**: tratar nulos e decisao de negocio, nao tecnica: poderiamos preencher com
> um valor (`fillna`), manter, ou descartar (`dropna`). Aqui descartar e o correto,
> pois um "fator de impacto nulo" nao significa zero - significa "nao avaliado".

### 3.5 Union - empilhando as fontes

**Union** empilha DataFrames de mesmo schema (mais linhas). Para nao perder a origem de
cada linha, criamos antes a coluna `fonte`:

```python
df_uniao = (
    df_jcr.withColumn("fonte", F.lit("jcr"))
    .unionByName(df_scimago.withColumn("fonte", F.lit("scimago")))
)
df_uniao.count()
```

**Resultado esperado**:
```
85096
```

(38.501 + 46.595 = 85.096.)

> **Conceitos**:
> - `F.lit(valor)`: cria uma coluna constante ("literal").
> - `unionByName`: alinha as colunas PELO NOME. O `union` simples alinha por POSICAO -
>   se a ordem das colunas diferir, ele mistura dados silenciosamente. Prefira sempre
>   `unionByName`.

```python
df_uniao.groupBy("fonte").count().show()
```

**Resultado esperado**:
```
+-------+-----+
|  fonte|count|
+-------+-----+
|    jcr|38501|
|scimago|46595|
+-------+-----+
```

### 3.6 Join - cruzando as fontes pelo ISSN

**Join** combina DataFrames lado a lado por uma chave (mais colunas). Queremos os
periodicos que estao NAS DUAS bases - `inner` join:

```python
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
df_join.count()
```

**Resultado esperado**:
```
29808
```

> **Conceitos**:
> - `alias("j")`: apelido para o DataFrame - necessario porque as duas fontes tem
>   colunas com o MESMO nome (`titulo`, `fator_impacto`...), e precisamos dizer de qual
>   lado vem cada uma (`j.titulo` vs `s.titulo`).
> - `how="inner"`: so linhas com ISSN presente nos dois lados. Outros tipos: `left`
>   (mantem tudo da esquerda), `full` (mantem tudo de ambos), `anti` (so o que NAO cruza).

```python
df_join.show(3, truncate=False)
```

**Resultado esperado**:
```
+--------+------------------------------------------+------------+----------------+-----------+---------------+
|issn    |titulo                                    |citacoes_jcr|citacoes_scimago|impacto_jcr|impacto_scimago|
+--------+------------------------------------------+------------+----------------+-----------+---------------+
|15424863|CA-A CANCER JOURNAL FOR CLINICIANS        |297         |211             |503.1      |106.094        |
|00079235|CA-A CANCER JOURNAL FOR CLINICIANS        |297         |211             |503.1      |106.094        |
|19358245|Foundations and Trends in Machine Learning|70          |39              |65.3       |37.044         |
+--------+------------------------------------------+------------+----------------+-----------+---------------+
```

> Interessante: os indicadores discordam bastante entre as fontes (503.1 vs 106.094) -
> cada ranking tem metodologia propria. E exatamente o tipo de coisa que um engenheiro
> de dados descobre ao cruzar fontes.

### 3.7 Agregacoes

**Top 5 periodicos por fator de impacto no JCR** (com `dropDuplicates` para o mesmo
titulo nao aparecer 2x por ter 2 ISSNs):

```python
(
    df_jcr.orderBy(F.col("fator_impacto").desc())
    .select("titulo", "fator_impacto", "total_citacoes")
    .dropDuplicates(["titulo"])
    .orderBy(F.col("fator_impacto").desc())
    .show(5, truncate=False)
)
```

**Resultado esperado**:
```
+----------------------------------+-------------+--------------+
|titulo                            |fator_impacto|total_citacoes|
+----------------------------------+-------------+--------------+
|CA-A CANCER JOURNAL FOR CLINICIANS|503.1        |297           |
|NATURE REVIEWS DRUG DISCOVERY     |122.7        |114           |
|LANCET                            |98.4         |106           |
|NEW ENGLAND JOURNAL OF MEDICINE   |96.2         |94            |
|BMJ-British Medical Journal       |93.6         |69            |
+----------------------------------+-------------+--------------+
only showing top 5 rows
```

**Estatisticas por fonte** (groupBy + funcoes de agregacao):

```python
(
    df_uniao.groupBy("fonte")
    .agg(
        F.count("*").alias("qtd_linhas"),
        F.round(F.avg("fator_impacto"), 2).alias("impacto_medio"),
        F.round(F.max("fator_impacto"), 2).alias("impacto_maximo"),
    )
    .show()
)
```

**Resultado esperado**:
```
+-------+----------+-------------+--------------+
|  fonte|qtd_linhas|impacto_medio|impacto_maximo|
+-------+----------+-------------+--------------+
|    jcr|     38501|         2.29|         503.1|
|scimago|     46595|         0.66|        106.09|
+-------+----------+-------------+--------------+
```

> **Conceito**: `groupBy` provoca um SHUFFLE (igual ao `reduceByKey` dos RDDs) - os dados
> da mesma chave precisam se encontrar para serem agregados. Na Spark UI voce ve o stage
> extra que isso cria.

### 3.8 Spark SQL

Todo DataFrame pode virar uma "tabela" temporaria e ser consultado com SQL puro - util
para quem vem do mundo de banco de dados (as duas APIs geram o MESMO plano otimizado):

```python
df_join.createOrReplaceTempView("periodicos")
spark.sql("""
    SELECT titulo,
           citacoes_jcr,
           citacoes_scimago,
           ABS(citacoes_jcr - citacoes_scimago) AS diferenca
    FROM periodicos
    ORDER BY diferenca DESC
    LIMIT 5
""").show(truncate=False)
```

**Resultado esperado**:
```
+-------------------------------+------------+----------------+---------+
|titulo                         |citacoes_jcr|citacoes_scimago|diferenca|
+-------------------------------+------------+----------------+---------+
|NATURE                         |54          |1391            |1337     |
|NATURE                         |54          |1391            |1337     |
|SCIENCE                        |50          |1336            |1286     |
|SCIENCE                        |50          |1336            |1286     |
|NEW ENGLAND JOURNAL OF MEDICINE|94          |1184            |1090     |
+-------------------------------+------------+----------------+---------+
```

(Os titulos duplicados sao os 2 ISSNs do mesmo periodico - desafio 2 da secao 6.)

### 3.9 Gravando em Parquet e CSV

```python
df_join.write.mode("overwrite").parquet("output/periodicos_parquet")
df_join.write.mode("overwrite").option("header", True).csv("output/periodicos_csv")
```

> **Conceitos**:
> - **Parquet** e o formato padrao de Data Lakes: colunar (le so as colunas da consulta),
>   comprimido e com schema embutido. CSV e texto: bom para interoperar, ruim de
>   performance e sem tipos.
> - `mode("overwrite")`: sobrescreve a pasta se existir (o write de DataFrame ACEITA
>   overwrite, diferente do `saveAsTextFile` dos RDDs).

Confira a releitura (o Parquet preserva o schema):

```python
spark.read.parquet("output/periodicos_parquet").count()
exit()
```

**Resultado esperado**:
```
29808
```

---

## 4. O script completo (spark-submit)

Todo o fluxo acima esta consolidado em `scripts/exercicio_dataframe.py` (mesma logica,
com caminhos absolutos e prints de progresso).

> **Onde salvar**: salve o arquivo em `spark-tutorial/scripts/exercicio_dataframe.py`
> (o arquivo ja existe se voce clonou o repositorio).

Execute (na pasta `spark-tutorial/`, com o venv ativado):

```bash
# Forma recomendada — spark-submit e a CLI oficial do Spark:
spark-submit scripts/exercicio_dataframe.py

# Equivalente com o Python do venv (identico em modo local):
python scripts/exercicio_dataframe.py
```

**Resultado esperado** (resumo das linhas-chave):
```
Linhas JCR     : 38501
Linhas Scimago : 46905
Linhas sem fator de impacto no Scimago: 310
Linhas Scimago apos limpeza: 46595
Linhas apos union (jcr + scimago): 85096
Periodicos presentes nas duas fontes (join por ISSN): 29808
...
Resultado gravado em:
  .../output/periodicos_parquet
  .../output/periodicos_csv
Conferencia - linhas no Parquet gravado: 29808
```

Valide os arquivos gerados:

```bash
ls output/periodicos_parquet/ | head -3
head -2 output/periodicos_csv/part-*.csv | head -4
```

**Resultado esperado**: arquivos `part-*.snappy.parquet` + `_SUCCESS`, e no CSV o header
`issn,titulo,citacoes_jcr,...` seguido dos dados.

---

## 5. Executando no Docker

Com o container rodando (`docker compose up -d` na pasta `docker/`):

```bash
docker exec spark /opt/spark/bin/spark-submit scripts/exercicio_dataframe.py
```

**Resultado esperado**: mesma saida da secao 4. Os arquivos aparecem em
`docker/output/periodicos_parquet/` e `docker/output/periodicos_csv/` na sua maquina.

---

## 6. Via Jupyter Notebook

Rodando o ETL completo no navegador, etapa por etapa — cada celula mostra o
resultado intermediario, o que facilita muito o entendimento em sala de aula.

> **Pre-requisito**: `pip install jupyter` (veja a secao 8 do
> `TUTORIAL_AMBIENTE_SPARK.md`).

Na pasta `spark-tutorial/`, com o venv ativado:

```bash
jupyter notebook
```

Crie um novo notebook (**New > Python 3 (ipykernel)**) e execute com **Shift+Enter**:

**Celula 1 — Setup:**
```python
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("DataFrameNotebook")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print(f"Spark {spark.version} pronto!")
```

**Celula 2 — Leitura e padronizacao:**
```python
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

df_jcr = padronizar(spark.read.csv("data/jcr.csv", header=True, inferSchema=True))
df_scimago = padronizar(spark.read.csv("data/scimago.csv", header=True, inferSchema=True))
print(f"JCR: {df_jcr.count()} linhas | Scimago: {df_scimago.count()} linhas")
df_jcr.printSchema()
```

**Celula 3 — Limpeza de nulos e union:**
```python
print(f"Nulos em fator_impacto (Scimago): {df_scimago.filter(F.col('fator_impacto').isNull()).count()}")
df_scimago = df_scimago.dropna(subset=["fator_impacto"])

df_uniao = (
    df_jcr.withColumn("fonte", F.lit("jcr"))
    .unionByName(df_scimago.withColumn("fonte", F.lit("scimago")))
)
print(f"Apos union: {df_uniao.count()} linhas")
df_uniao.groupBy("fonte").count().show()
```

**Celula 4 — Join por ISSN:**
```python
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
print(f"Periodicos em ambas as fontes (inner join): {df_join.count()}")
df_join.show(3, truncate=False)
```

**Celula 5 — Agregacoes e Spark SQL:**
```python
# Estatisticas por fonte
df_uniao.groupBy("fonte").agg(
    F.count("*").alias("qtd_linhas"),
    F.round(F.avg("fator_impacto"), 2).alias("impacto_medio"),
    F.round(F.max("fator_impacto"), 2).alias("impacto_maximo"),
).show()

# SQL — periodicos com maior discrepancia de citacoes entre as fontes
df_join.createOrReplaceTempView("periodicos")
spark.sql("""
    SELECT titulo, citacoes_jcr, citacoes_scimago,
           ABS(citacoes_jcr - citacoes_scimago) AS diferenca
    FROM periodicos ORDER BY diferenca DESC LIMIT 5
""").show(truncate=False)
```

**Celula 6 — Gravar resultado e encerrar:**
```python
df_join.write.mode("overwrite").parquet("output/periodicos_parquet_nb")
df_join.write.mode("overwrite").option("header", True).csv("output/periodicos_csv_nb")
print(f"Parquet: {spark.read.parquet('output/periodicos_parquet_nb').count()} linhas gravadas")
spark.stop()
```

> Os arquivos de saida sao gravados em `output/periodicos_parquet_nb/` e
> `output/periodicos_csv_nb/` para nao colidir com os gerados pelo script.

---

## 7. Desafios extras

1. **Left join**: troque o `inner` por `left` e conte quantos periodicos do JCR NAO
   existem no Scimago (dica: `filter(F.col("impacto_scimago").isNull())`).
2. **Dedup por titulo**: o join tem o mesmo periodico 2x (ISSN impresso + eletronico).
   Gere uma versao com 1 linha por titulo (`dropDuplicates(["titulo"])`). Quantas linhas
   sobram?
3. **Discrepancia de rankings**: crie a coluna `razao_impacto = impacto_jcr /
   impacto_scimago` e liste os 10 periodicos onde os rankings mais discordam.
4. **Particionamento**: grave o union particionado por fonte:
   `df_uniao.write.partitionBy("fonte").parquet(...)` - e olhe a estrutura de pastas
   criada.
5. **Schema explicito**: releia o `jcr.csv` SEM `inferSchema`, declarando um
   `StructType` na mao - lembre que na leitura as colunas ainda tem os nomes ORIGINAIS
   do arquivo (ex.: `Total Cites`), entao declare `StructField("Total Cites",
   IntegerType())` e renomeie depois com a funcao `padronizar`.

---

## 8. Troubleshooting

### `Path does not exist: data/jcr.csv`

Abra o shell/execute o script a partir da pasta `spark-tutorial/` (os caminhos da secao 3
sao relativos). O script da secao 4 usa caminhos absolutos e roda de qualquer lugar.

### `AnalysisException: Column 'titulo' is ambiguous`

Apos um join, colunas homonimas dos dois lados precisam do alias do DataFrame
(`F.col("j.titulo")`). Refaca o passo 3.6 incluindo os `.alias("j")` / `.alias("s")`.

### Erro `TaskResultLost` ou `Failed to connect to /10.x.x.x` (trava no join)

VPN/rede corporativa. O join usa broadcast entre processos - exporte
`SPARK_LOCAL_IP=127.0.0.1` antes de abrir o shell (detalhes no troubleshooting do
`TUTORIAL_AMBIENTE_SPARK.md`).

### Numeros diferentes dos meus

Confira se voce: (1) usou `trim` no ISSN, (2) descartou os 310 nulos ANTES do union/join,
(3) usou `inner` join. Cada decisao dessas muda as contagens.

### `total_citacoes` aparece como `297.0` em vez de `297`

Faltou o `.cast("int")` da padronizacao (passo 3.3) - o inferSchema leu como double.

### Quero ver o plano otimizado do Catalyst

```python
df_join.explain()
```
Mostra o plano fisico (procure por `BroadcastHashJoin` - o Spark detectou que um lado e
pequeno o suficiente para ser replicado, evitando shuffle do lado grande).
