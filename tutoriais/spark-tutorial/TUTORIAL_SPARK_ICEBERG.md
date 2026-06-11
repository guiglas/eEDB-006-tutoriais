# Tutorial: Lakehouse local com Spark + Apache Iceberg

> Exercicio de **Lakehouse**: gravar o cruzamento dos rankings JCR x Scimago como uma
> **tabela Iceberg** no disco local, fazer um **UPDATE** via SQL (impossivel em Parquet
> puro!), inspecionar os **snapshots** e fazer **time travel** para consultar a tabela
> como ela era antes da alteracao.

**Pre-requisitos**:
- Ambiente montado E preparado para Iceberg (secoes 4/5 e **6** do
  `TUTORIAL_AMBIENTE_SPARK.md`)
- Ter feito o `TUTORIAL_SPARK_DATAFRAME.md` (reusamos o mesmo ETL)

---

## Sumario

1. [Conceitos: por que Iceberg?](#1-conceitos-por-que-iceberg)
2. [Abrindo a sessao com Iceberg](#2-abrindo-a-sessao-com-iceberg)
3. [Passo a passo no shell interativo](#3-passo-a-passo-no-shell-interativo)
   - [3.1 ETL: preparar o DataFrame](#31-etl-preparar-o-dataframe)
   - [3.2 Criar a tabela Iceberg](#32-criar-a-tabela-iceberg)
   - [3.3 O que foi criado no disco?](#33-o-que-foi-criado-no-disco)
   - [3.4 UPDATE - alterando dados com SQL](#34-update---alterando-dados-com-sql)
   - [3.5 Snapshots - o historico da tabela](#35-snapshots---o-historico-da-tabela)
   - [3.6 TIME TRAVEL - consultando o passado](#36-time-travel---consultando-o-passado)
4. [O script completo (spark-submit)](#4-o-script-completo-spark-submit)
5. [Executando no Docker](#5-executando-no-docker)
6. [Via Jupyter Notebook](#6-via-jupyter-notebook)
7. [Desafios extras](#7-desafios-extras)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Conceitos: por que Iceberg?

No tutorial anterior gravamos o resultado em **Parquet**. Otimo formato... mas ele e so
um formato de ARQUIVO. Uma "tabela" feita de arquivos Parquet soltos tem limitacoes
serias:

| Problema com Parquet puro | Solucao do Iceberg |
|---|---|
| UPDATE/DELETE de uma linha = reescrever arquivos na mao | `UPDATE`, `DELETE`, `MERGE` via SQL |
| Sem transacoes: leitor pode ver dados pela metade | Commits **ACID** - ou ve tudo, ou nada |
| Sem historico: sobrescreveu, perdeu | **Snapshots** + **time travel** |
| Mudar schema (adicionar coluna) = perigo de quebrar tudo | Evolucao de schema segura |
| Listar milhares de arquivos para planejar a consulta | Metadados proprios indexam os arquivos |

O **Apache Iceberg** e um **formato de TABELA** (table format): uma camada de metadados
sobre os arquivos Parquet que adiciona tudo isso. E a fundacao do conceito de
**Lakehouse** - o data lake (arquivos baratos) com comportamentos de warehouse
(transacoes, SQL completo, historico). Concorrentes: Delta Lake e Apache Hudi.

> **Conceito - snapshot**: cada operacao de escrita (criar, inserir, atualizar...) gera
> um novo **snapshot** = uma "foto" completa e imutavel da tabela naquele momento. Os
> snapshots compartilham os arquivos que nao mudaram (barato!) e permitem consultar a
> tabela em qualquer ponto do passado: o **time travel**.

> **Conceito - catalogo**: o Iceberg precisa de um *catalogo* para saber qual e o
> metadado atual de cada tabela. Em producao usa-se Hive Metastore, AWS Glue, Nessie ou
> um REST catalog. Para estudo local usamos o catalogo **hadoop**: os metadados ficam em
> arquivos na propria pasta `warehouse/` - zero infraestrutura.

---

## 2. Abrindo a sessao com Iceberg

Na pasta `spark-tutorial/`, com o venv ativado, abra o shell ja configurado
(mesmo comando da secao 6.1 do tutorial de ambiente):

```bash
pyspark \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=warehouse
```

(No Windows/PowerShell, troque as barras `\` de fim de linha por acentos graves `` ` ``.)

Relembrando o que cada configuracao faz:

| Configuracao | Significado |
|---|---|
| `--packages ...iceberg-spark-runtime-3.5_2.12:1.10.2` | Conector Iceberg para Spark 3.5 (baixado do Maven na 1a vez, depois vem do cache) |
| `spark.sql.extensions` | Liga os comandos SQL do Iceberg (UPDATE, MERGE, CALL...) |
| `spark.sql.catalog.local` | Registra o catalogo `local` - as tabelas serao `local.<schema>.<tabela>` |
| `spark.sql.catalog.local.type=hadoop` | Catalogo baseado em arquivos (sem servidor) |
| `spark.sql.catalog.local.warehouse=warehouse` | Tudo sera gravado na pasta `warehouse/` |

---

## 3. Passo a passo no shell interativo

### 3.1 ETL: preparar o DataFrame

Mesmo tratamento do tutorial de DataFrames, condensado (leitura, padronizacao, limpeza
de nulos e join por ISSN):

```python
from pyspark.sql import functions as F
spark.sparkContext.setLogLevel("ERROR")

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
df_periodicos.count()
```

**Resultado esperado**:
```
29808
```

### 3.2 Criar a tabela Iceberg

```python
df_periodicos.writeTo("local.db.periodicos").createOrReplace()
spark.sql("SELECT COUNT(*) AS qtd FROM local.db.periodicos").show()
```

**Resultado esperado**:
```
+-----+
|  qtd|
+-----+
|29808|
+-----+
```

> **Conceitos**:
> - `writeTo(...)` e a API "v2" de escrita, usada com catalogos como o Iceberg.
>   `createOrReplace()` cria a tabela (ou recria do zero se ja existir).
> - O nome tem 3 partes: catalogo `local` + schema `db` + tabela `periodicos`.
> - A partir de agora a tabela e consultavel por SQL como em um banco de dados.

### 3.3 O que foi criado no disco?

Sem fechar o shell, abra OUTRO terminal na pasta `spark-tutorial/` e explore:

```bash
find warehouse -type d
ls warehouse/db/periodicos/metadata/
```

**Resultado esperado**:
```
warehouse
warehouse/db
warehouse/db/periodicos
warehouse/db/periodicos/data
warehouse/db/periodicos/metadata

... e dentro de metadata/:
v1.metadata.json
snap-XXXXXXXXXXXXXXXXXXX-1-....avro
....avro (manifests)
version-hint.text
```

| Pasta/arquivo | O que e |
|---|---|
| `data/` | Os dados de verdade - arquivos **Parquet** comuns |
| `metadata/vN.metadata.json` | O "cerebro": schema, particionamento e lista de snapshots da tabela |
| `metadata/snap-*.avro` | A lista de manifests de cada snapshot |
| `metadata/*-m0.avro` (manifests) | Indice dos arquivos de dados: quais arquivos, faixas min/max de cada coluna |
| `metadata/version-hint.text` | Ponteiro para o metadata atual (especifico do catalogo hadoop) |

> Iceberg = seus mesmos Parquets + uma arvore de metadados que os transforma em tabela.

### 3.4 UPDATE - alterando dados com SQL

Suponha que o fator de impacto do periodico "CA-A CANCER JOURNAL FOR CLINICIANS" foi
revisado. Primeiro, o valor atual (de volta ao shell pyspark):

```python
spark.sql("""
    SELECT titulo, impacto_jcr FROM local.db.periodicos
    WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'
""").show(truncate=False)
```

**Resultado esperado** (2 linhas: ISSN impresso + eletronico):
```
+----------------------------------+-----------+
|titulo                            |impacto_jcr|
+----------------------------------+-----------+
|CA-A CANCER JOURNAL FOR CLINICIANS|503.1      |
|CA-A CANCER JOURNAL FOR CLINICIANS|503.1      |
+----------------------------------+-----------+
```

Agora o UPDATE - um comando que NAO EXISTE para Parquet puro:

```python
spark.sql("""
    UPDATE local.db.periodicos
    SET impacto_jcr = 999.9
    WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'
""")
```

Confira:

```python
spark.sql("""
    SELECT titulo, impacto_jcr FROM local.db.periodicos
    WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'
""").show(truncate=False)
```

**Resultado esperado**:
```
+----------------------------------+-----------+
|titulo                            |impacto_jcr|
+----------------------------------+-----------+
|CA-A CANCER JOURNAL FOR CLINICIANS|999.9      |
|CA-A CANCER JOURNAL FOR CLINICIANS|999.9      |
+----------------------------------+-----------+
```

> **Conceito - copy-on-write**: o Iceberg nao "edita" o Parquet (Parquet e imutavel).
> Ele reescreve apenas os arquivos que continham as linhas afetadas e registra um NOVO
> snapshot apontando para a versao nova. O snapshot antigo continua existindo - e por
> isso o time travel funciona.

### 3.5 Snapshots - o historico da tabela

Toda tabela Iceberg expoe tabelas de metadados. A mais util: `<tabela>.snapshots`:

```python
spark.sql("""
    SELECT snapshot_id, committed_at, operation
    FROM local.db.periodicos.snapshots
    ORDER BY committed_at
""").show(truncate=False)
```

**Resultado esperado** (ids e horarios serao outros para voce):
```
+-------------------+-----------------------+---------+
|snapshot_id        |committed_at           |operation|
+-------------------+-----------------------+---------+
|3663225055368546597|2026-06-11 15:01:22.765|overwrite|
|7565065978423178730|2026-06-11 15:01:25.097|overwrite|
+-------------------+-----------------------+---------+
```

Dois snapshots: o 1o e a criacao da tabela (`createOrReplace`), o 2o e o UPDATE.

> Outras tabelas de metadados para explorar: `.history`, `.files`, `.manifests`,
> `.partitions`.

### 3.6 TIME TRAVEL - consultando o passado

Guarde o id do PRIMEIRO snapshot (criacao):

```python
snap_criacao = spark.sql("""
    SELECT snapshot_id FROM local.db.periodicos.snapshots
    ORDER BY committed_at LIMIT 1
""").first()["snapshot_id"]
print(snap_criacao)
```

E viaje no tempo com `VERSION AS OF`:

```python
spark.sql(f"""
    SELECT titulo, impacto_jcr
    FROM local.db.periodicos VERSION AS OF {snap_criacao}
    WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'
""").show(truncate=False)
```

**Resultado esperado** - o valor de ANTES do UPDATE:
```
+----------------------------------+-----------+
|titulo                            |impacto_jcr|
+----------------------------------+-----------+
|CA-A CANCER JOURNAL FOR CLINICIANS|503.1      |
|CA-A CANCER JOURNAL FOR CLINICIANS|503.1      |
+----------------------------------+-----------+
```

A consulta normal (sem time travel) segue mostrando o valor novo:

```python
spark.sql("""
    SELECT titulo, impacto_jcr FROM local.db.periodicos
    WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'
""").show(truncate=False)
```

**Resultado esperado**: `999.9` nas duas linhas.

Tambem e possivel viajar por TIMESTAMP (qualquer instante entre os snapshots) - copie o
`committed_at` do primeiro snapshot:

```python
spark.sql("""
    SELECT titulo, impacto_jcr
    FROM local.db.periodicos TIMESTAMP AS OF '2026-06-11 15:01:23'
    WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'
""").show(truncate=False)
```

(Ajuste o timestamp para um instante ENTRE o seu 1o e o seu 2o snapshot - use os valores
da SUA tabela `.snapshots`.)

> **Para que serve na pratica?** Auditoria ("qual era o saldo ontem as 18h?"),
> reproducibilidade ("treine o modelo com os dados exatos da segunda-feira"), e
> recuperacao de desastre ("alguem rodou um UPDATE errado" - veja o desafio 1).

Saia do shell:

```python
exit()
```

---

## 4. O script completo (spark-submit)

Todo o fluxo esta consolidado em `scripts/exercicio_iceberg.py` — ele configura a sessao
via codigo (equivalente as flags do shell), refaz o ETL, cria a tabela, faz o UPDATE,
lista snapshots e executa o time travel automaticamente.

> **Onde salvar**: salve o arquivo em `spark-tutorial/scripts/exercicio_iceberg.py`
> (o arquivo ja existe se voce clonou o repositorio).

```python
spark = (
    SparkSession.builder
    .appName("ExercicioIceberg")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
    .getOrCreate()
)
```

(Veja o arquivo para o codigo completo comentado.)

Execute (na pasta `spark-tutorial/`, com o venv ativado):

```bash
# Forma recomendada — spark-submit e a CLI oficial do Spark:
spark-submit scripts/exercicio_iceberg.py

# Equivalente com o Python do venv (identico em modo local):
python scripts/exercicio_iceberg.py
```

**Resultado esperado** (resumo; ids/horarios variam):
```
Spark 3.5.8 + Iceberg prontos. Warehouse: .../spark-tutorial/warehouse
Linhas a gravar na tabela Iceberg: 29808
Tabela local.db.periodicos criada com 29808 linhas

Antes do UPDATE:    impacto_jcr = 503.1  (2 linhas)
Depois do UPDATE:   impacto_jcr = 999.9  (2 linhas)

Historico de snapshots da tabela:  2 snapshots (operation=overwrite)
(se voce fez a secao 3 antes, no MESMO warehouse, verao 4: os 2 da secao 3 + 2 do script)

TIME TRAVEL para o snapshot de criacao: impacto_jcr = 503.1
Consulta atual (sem time travel) continua com o valor novo: 999.9

Exercicio Iceberg concluido!
```

> Rode o script 2x e olhe a tabela `.snapshots`: como ele usa `createOrReplace`, cada
> execucao recria a tabela - mas os snapshots anteriores continuam la (replace tambem e
> so um commit!).

---

## 5. Executando no Docker

Com o container rodando (`docker compose up -d` na pasta `docker/`):

```bash
docker exec spark /opt/spark/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2 \
  scripts/exercicio_iceberg.py
```

**Resultado esperado**: mesma saida da secao 4. A tabela aparece na sua maquina em
`docker/warehouse/db/periodicos/` (volume montado).

> No Docker passamos o `--packages` na linha de comando: o conector e resolvido ANTES do
> script iniciar e fica guardado no volume `ivy-cache` (a 1a execucao baixa, as proximas
> nao).

---

## 6. Via Jupyter Notebook

Rodando o exercicio Lakehouse no navegador. No Jupyter a configuracao do Iceberg vai
toda no `SparkSession.builder` — sem flags de linha de comando.

> **Pre-requisito**: `pip install jupyter` (veja a secao 8 do
> `TUTORIAL_AMBIENTE_SPARK.md`).

Na pasta `spark-tutorial/`, com o venv ativado:

```bash
jupyter notebook
```

Crie um novo notebook (**New > Python 3 (ipykernel)**) e execute com **Shift+Enter**:

**Celula 1 — Setup com Iceberg:**
```python
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("IcebergNotebook")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print(f"Spark {spark.version} + Iceberg prontos!")
```

> Na primeira execucao desta celula, o download do Iceberg (~100 MB) aparece no
> TERMINAL do `jupyter notebook`. O kernel pode parecer travado — aguarde ate
> a celula exibir "prontos!". As execucoes seguintes carregam do cache `~/.ivy2`.

**Celula 2 — ETL (leitura, padronizacao e join):**
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
        .dropna(subset=["fator_impacto"])
    )

df_jcr = padronizar(spark.read.csv("data/jcr.csv", header=True, inferSchema=True))
df_scimago = padronizar(spark.read.csv("data/scimago.csv", header=True, inferSchema=True))

df_periodicos = (
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
print(f"Linhas para gravar na tabela Iceberg: {df_periodicos.count()}")
```

**Celula 3 — Criar tabela Iceberg:**
```python
# Usamos "periodicos_nb" para nao colidir com a tabela do tutorial interativo
df_periodicos.writeTo("local.db.periodicos_nb").createOrReplace()
spark.sql("SELECT COUNT(*) AS qtd FROM local.db.periodicos_nb").show()
```

**Celula 4 — UPDATE e verificacao:**
```python
# Valor antes do UPDATE
spark.sql("""
    SELECT titulo, impacto_jcr FROM local.db.periodicos_nb
    WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'
""").show(truncate=False)

# UPDATE via SQL (impossivel em Parquet puro!)
spark.sql("""
    UPDATE local.db.periodicos_nb
    SET impacto_jcr = 999.9
    WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'
""")

# Valor depois
spark.sql("""
    SELECT titulo, impacto_jcr FROM local.db.periodicos_nb
    WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'
""").show(truncate=False)
```

**Celula 5 — Snapshots e time travel:**
```python
# Historico da tabela
spark.sql("""
    SELECT snapshot_id, committed_at, operation
    FROM local.db.periodicos_nb.snapshots ORDER BY committed_at
""").show(truncate=False)

# Guardar o id do primeiro snapshot (criacao)
snap_criacao = spark.sql("""
    SELECT snapshot_id FROM local.db.periodicos_nb.snapshots
    ORDER BY committed_at LIMIT 1
""").first()["snapshot_id"]

# Time travel: valor ANTES do UPDATE
spark.sql(f"""
    SELECT titulo, impacto_jcr
    FROM local.db.periodicos_nb VERSION AS OF {snap_criacao}
    WHERE titulo = 'CA-A CANCER JOURNAL FOR CLINICIANS'
""").show(truncate=False)
print(f"^ Valor antes do UPDATE (time travel para snapshot {snap_criacao})")
```

**Celula 6 — Encerrar:**
```python
spark.stop()
```

---

## 7. Desafios extras

1. **Rollback**: desfaca o UPDATE de verdade (nao so na leitura) com uma *procedure*:
   ```python
   spark.sql(f"CALL local.system.rollback_to_snapshot('db.periodicos', {snap_criacao})")
   ```
   Confira que a consulta normal voltou a mostrar 503.1 - e repare: a tabela
   `.snapshots` NAO ganha linha nova (rollback so move o ponteiro de volta); quem
   registra a mudanca e a tabela `.history`.
2. **DELETE**: remova os periodicos com `impacto_jcr < 1.0` e conte as linhas antes e
   depois. Quantos snapshots a tabela tem agora?
3. **Evolucao de schema**: `ALTER TABLE local.db.periodicos ADD COLUMN origem STRING` -
   e mostre que os snapshots antigos continuam legiveis por time travel.
4. **MERGE INTO**: simule uma carga incremental - crie um DataFrame com 2 ISSNs
   existentes (valores novos) + 1 ISSN inventado e aplique
   `MERGE INTO ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`.
5. **Inspecao**: explore `SELECT * FROM local.db.periodicos.files` - quantos arquivos
   Parquet a tabela tem? E depois do UPDATE?

---

## 8. Troubleshooting

### `[TABLE_OR_VIEW_NOT_FOUND] The table or view local.db.periodicos cannot be found`

A sessao foi aberta SEM as configuracoes do catalogo `local`. Feche o shell e abra com
TODAS as flags da secao 2 (`--packages` e os 4 `--conf`).

### `UPDATE ... is not supported temporarily` / erro de sintaxe no UPDATE

Faltou a configuracao `spark.sql.extensions=...IcebergSparkSessionExtensions` - e ela
que habilita UPDATE/MERGE/CALL no SQL do Spark.

### Download do conector falha (`unresolved dependency`)

Primeira execucao precisa de internet (Maven Central). Com proxy corporativo, configure
`HTTP_PROXY`/`HTTPS_PROXY`. Depois do 1o download, o JAR fica em `~/.ivy2/` (local) ou
no volume `ivy-cache` (Docker).

### `Cannot write incompatible data to table` ao recriar a tabela

Voce mudou o schema do DataFrame e usou `append()`. Para recriar do zero use
`createOrReplace()` como no tutorial.

### Time travel com `TIMESTAMP AS OF` da erro `Cannot find a snapshot older than ...`

O timestamp informado e ANTERIOR ao primeiro snapshot da tabela. Use um instante igual
ou posterior ao `committed_at` do 1o snapshot (consulte `.snapshots`).

### A pasta warehouse/ esta crescendo a cada execucao

Cada commit gera novos arquivos e snapshots - e o preco do historico. Em producao
roda-se manutencao (`expire_snapshots`). Para estudo, pode simplesmente apagar:
`DROP TABLE local.db.periodicos PURGE` (ou `rm -rf warehouse/`).

### Erro `TaskResultLost` / `Failed to connect to /10.x.x.x`

VPN/rede corporativa - exporte `SPARK_LOCAL_IP=127.0.0.1` antes do shell (o script ja
tem a config embutida). Detalhes no `TUTORIAL_AMBIENTE_SPARK.md`.
