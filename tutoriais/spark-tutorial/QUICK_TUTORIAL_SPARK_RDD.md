# Quick Tutorial: WordCount com Spark RDD

> So os comandos. Explicacoes: `TUTORIAL_SPARK_RDD.md`.
> Pre-requisito: ambiente pronto (`QUICK_TUTORIAL_AMBIENTE_SPARK.md`).

---

## 1. Versao interativa (pyspark)

Na pasta `spark-tutorial/`, com o venv ativado:

```bash
pyspark
```

```python
import re
sc = spark.sparkContext
sc.setLogLevel("ERROR")

linhas = sc.textFile("data/lorem.txt")
linhas.count()                                   # 40

palavras = linhas.flatMap(lambda l: re.findall(r"[a-zA-Z]+", l.lower()))
palavras.count()                                 # 1695

pares = palavras.map(lambda p: (p, 1))
contagem = pares.reduceByKey(lambda a, b: a + b)
contagem.count()                                 # 939

contagem.takeOrdered(10, key=lambda par: -par[1])
# [('the', 63), ('a', 48), ('of', 39), ('and', 30), ('to', 25),
#  ('in', 25), ('i', 24), ('you', 16), ('on', 15), ('that', 15)]
# (empates podem trocar de ordem)

contagem.sortBy(lambda par: -par[1]).saveAsTextFile("output/wordcount_shell")
exit()
```

---

## 2. Versao script

Script em: `spark-tutorial/scripts/wordcount_rdd.py`

```bash
# spark-submit (CLI oficial):
spark-submit scripts/wordcount_rdd.py

# ou via Python do venv:
python scripts/wordcount_rdd.py
```

**Resultado esperado**:
```
Total de linhas    : 40
Total de palavras  : 1695
Palavras distintas : 939
Top 10: the 63, a 48, of 39, and 30, to/in 25, i 24, you 16, on/that 15
```

Conferir saida:

```bash
ls output/wordcount/            # _SUCCESS  part-00000  part-00001
head -5 output/wordcount/part-00000
```

---

## 3. Via Docker

```bash
docker exec spark /opt/spark/bin/spark-submit scripts/wordcount_rdd.py
head -5 docker/output/wordcount/part-00000
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
import re
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("WordCountNB").master("local[*]")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.driver.host", "127.0.0.1").getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("ERROR")
```

```python
# Celula 2 — MapReduce
linhas = sc.textFile("data/lorem.txt")
palavras = linhas.flatMap(lambda l: re.findall(r"[a-zA-Z]+", l.lower()))
contagem = palavras.map(lambda p: (p, 1)).reduceByKey(lambda a, b: a + b)
print(f"Linhas: {linhas.count()} | Palavras: {palavras.count()} | Distintas: {contagem.count()}")
```

```python
# Celula 3 — Top 10
for palavra, total in contagem.takeOrdered(10, key=lambda par: -par[1]):
    print(f"{palavra:<15} {total}")
```

```python
# Celula 4 — gravar e encerrar
import shutil; shutil.rmtree("output/wordcount_nb", ignore_errors=True)
contagem.sortBy(lambda par: -par[1]).saveAsTextFile("output/wordcount_nb")
spark.stop()
```
