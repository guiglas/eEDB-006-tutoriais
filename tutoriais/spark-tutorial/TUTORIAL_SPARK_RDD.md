# Tutorial: WordCount com Spark RDD (MapReduce)

> Exercicio classico de Big Data: contar a frequencia de cada palavra de um arquivo de
> texto usando a **API RDD** do Spark - o mesmo modelo **MapReduce** do Hadoop, porem em
> memoria e com muito menos codigo. Ao final, voce tera o Top 10 de palavras do arquivo
> `data/lorem.txt` e o resultado completo gravado em disco.

**Pre-requisito**: ambiente montado conforme o `TUTORIAL_AMBIENTE_SPARK.md`
(local ou Docker).

---

## Sumario

1. [Conceitos: RDD e MapReduce](#1-conceitos-rdd-e-mapreduce)
2. [Conhecendo os dados](#2-conhecendo-os-dados)
3. [Passo a passo no shell interativo](#3-passo-a-passo-no-shell-interativo)
4. [O script completo (spark-submit)](#4-o-script-completo-spark-submit)
5. [Validando o resultado gravado](#5-validando-o-resultado-gravado)
6. [Executando no Docker](#6-executando-no-docker)
7. [Via Jupyter Notebook](#7-via-jupyter-notebook)
8. [Visualizando o job na Spark UI](#8-visualizando-o-job-na-spark-ui)
9. [Desafios extras](#9-desafios-extras)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Conceitos: RDD e MapReduce

### O que e um RDD?

**RDD (Resilient Distributed Dataset)** e a estrutura de dados original do Spark: uma
colecao de elementos **particionada** entre os nucleos/maquinas do cluster.

- **Resilient**: se uma particao se perde, o Spark a reconstroi a partir da "receita"
  (linhagem) de transformacoes.
- **Distributed**: cada particao pode ser processada em paralelo, em maquinas diferentes.
- **Dataset**: uma colecao de objetos Python quaisquer (strings, tuplas, etc.).

### Transformacoes vs Acoes (conceito mais importante do Spark!)

| Tipo | O que faz | Exemplos | Quando executa |
|---|---|---|---|
| **Transformacao** | Descreve uma nova etapa de processamento | `map`, `flatMap`, `filter`, `reduceByKey` | **Nao executa nada** - so monta o plano (lazy) |
| **Acao** | Pede um resultado concreto | `count`, `take`, `collect`, `saveAsTextFile` | **Dispara** a execucao de todo o plano |

Essa "preguica" (lazy evaluation) permite ao Spark otimizar o plano inteiro antes de
gastar CPU - voce encadeia 10 transformacoes e nada roda ate a primeira acao.

### O modelo MapReduce

O WordCount e o "Hello World" do MapReduce porque exercita as 3 fases do modelo:

```
   linhas do arquivo
        |
        v
  ┌──── MAP ─────┐   cada linha vira N pares (palavra, 1)
  │ "ipsum dolor"│ -> ("ipsum",1), ("dolor",1)
  └──────────────┘
        |
        v
  ┌── SHUFFLE ───┐   o Spark agrupa os pares pela CHAVE (palavra)
  │ ("ipsum",1)  │ -> ("ipsum", [1,1,1...])   <- pares da mesma palavra
  │ ("ipsum",1)  │                               se encontram, mesmo vindo
  └──────────────┘                               de particoes diferentes
        |
        v
  ┌── REDUCE ────┐   soma os 1s de cada palavra
  │ ("ipsum",    │ -> ("ipsum", 14)
  │  [1,1,...])  │
  └──────────────┘
```

No Hadoop classico isso exigia uma classe Java para o Mapper, outra para o Reducer e um
Driver (veja o `hadoop-docker-tutorial` desta colecao). No Spark, sao **3 linhas**.

---

## 2. Conhecendo os dados

O arquivo `data/lorem.txt` tem 40 linhas de texto em "Lorem Ipsum" e em ingles.
De olhada nele:

```bash
head -2 data/lorem.txt
wc -l data/lorem.txt
```

**Resultado esperado**:
```
Classic Lorem Ipsum Filler Text:
Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor ...
      40 data/lorem.txt
```

---

## 3. Passo a passo no shell interativo

Vamos construir o WordCount peca por peca no shell `pyspark`, vendo o efeito de cada
transformacao. Na pasta `spark-tutorial/`, com o venv ativado:

```bash
pyspark
```

> No Docker: `docker exec -it spark /opt/spark/bin/pyspark` (ja cai na pasta certa).

### Passo 3.1 - O SparkContext

O shell ja entrega a variavel `spark` (SparkSession). A API RDD e acessada pelo
**SparkContext**:

```python
sc = spark.sparkContext
sc.setLogLevel("ERROR")   # silencia os WARNs para enxergar melhor
```

### Passo 3.2 - Ler o arquivo como RDD de linhas

```python
linhas = sc.textFile("data/lorem.txt")
linhas.count()
```

**Resultado esperado**:
```
40
```

> **Conceito**: `textFile` e uma transformacao - o arquivo so e lido de fato quando voce
> chama a acao `count()`. Cada elemento do RDD e UMA linha do arquivo (string).

Espie a primeira linha com a acao `take`:

```python
linhas.take(1)
```

**Resultado esperado**:
```
['Classic Lorem Ipsum Filler Text:']
```

### Passo 3.3 - MAP: quebrar linhas em palavras (flatMap)

Se usassemos `map`, cada linha viraria UMA lista de palavras (RDD de listas).
Queremos um RDD de PALAVRAS - por isso `flatMap`, que "achata" o resultado:

```python
import re
palavras = linhas.flatMap(lambda linha: re.findall(r"[a-zA-Z]+", linha.lower()))
palavras.take(5)
```

**Resultado esperado**:
```
['classic', 'lorem', 'ipsum', 'filler', 'text']
```

> **Conceitos**:
> - `lower()` normaliza: "Lorem" e "lorem" devem contar como a mesma palavra.
> - `re.findall(r"[a-zA-Z]+", ...)` extrai so sequencias de letras - elimina pontuacao
>   e numeros de uma vez (ex.: "amet," vira "amet").

```python
palavras.count()
```

**Resultado esperado**:
```
1695
```

### Passo 3.4 - MAP: cada palavra vira o par (palavra, 1)

O passo "Map" classico do MapReduce - emitir pares chave-valor:

```python
pares = palavras.map(lambda palavra: (palavra, 1))
pares.take(3)
```

**Resultado esperado**:
```
[('classic', 1), ('lorem', 1), ('ipsum', 1)]
```

### Passo 3.5 - REDUCE: somar os 1s por palavra (reduceByKey)

```python
contagem = pares.reduceByKey(lambda a, b: a + b)
contagem.count()
```

**Resultado esperado**:
```
939
```

(939 palavras distintas no arquivo.)

> **Conceito**: `reduceByKey` faz o SHUFFLE (agrupa pela chave) e aplica a funcao de
> reducao aos valores de cada chave, dois a dois: `1+1=2`, `2+1=3`... A funcao precisa
> ser associativa e comutativa, pois a ordem de chegada nao e garantida.

### Passo 3.6 - Top 10 palavras

```python
top10 = contagem.takeOrdered(10, key=lambda par: -par[1])
for palavra, total in top10:
    print(f"{palavra:<10} {total}")
```

**Resultado esperado** (empates podem trocar de posicao entre si):
```
the        63
a          48
of         39
and        30
to         25
in         25
i          24
you        16
on         15
that       15
```

> **Conceito**: `takeOrdered(n, key=...)` ordena pelo criterio dado e traz so os N
> primeiros para o driver. O sinal de menos (`-par[1]`) inverte para ordem decrescente.

### Passo 3.7 - Gravar o resultado em disco

```python
contagem.sortBy(lambda par: -par[1]).saveAsTextFile("output/wordcount_shell")
exit()
```

> Se for rodar esta secao de novo, apague antes a saida anterior
> (`rm -rf output/wordcount_shell`) - o `saveAsTextFile` nao sobrescreve.

> **Conceito**: `saveAsTextFile` grava UMA PASTA, com um arquivo `part-0000N` por
> particao do RDD, mais um marcador `_SUCCESS`. E o padrao do ecossistema
> Hadoop/Spark - em um cluster, cada executor grava sua particao em paralelo;
> ninguem espera por um "arquivo unico".

---

## 4. O script completo (spark-submit)

A versao "producao" do exercicio esta em `scripts/wordcount_rdd.py` - e o mesmo codigo
do passo a passo, organizado e comentado:

```python
"""
wordcount_rdd.py - WordCount com a API RDD do Spark (modelo MapReduce)
"""
import re
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# ---------------------------------------------------------------
# 0. Caminhos de entrada e saida
# ---------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent  # pasta spark-tutorial/
ARQUIVO_ENTRADA = sys.argv[1] if len(sys.argv) > 1 else str(BASE_DIR / "data" / "lorem.txt")
PASTA_SAIDA = str(BASE_DIR / "output" / "wordcount")

# Remove a saida anterior, se existir (RDD saveAsTextFile nao sobrescreve)
shutil.rmtree(PASTA_SAIDA, ignore_errors=True)

# ---------------------------------------------------------------
# 1. SparkSession + SparkContext (porta de entrada da API RDD)
# ---------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("WordCountRDD")
    .master("local[*]")
    # Fixa o driver no localhost - evita erros de rede em VPN/Wi-Fi corporativo
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# ---------------------------------------------------------------
# 2. EXTRACT - le o arquivo texto como um RDD de linhas
# ---------------------------------------------------------------
linhas = sc.textFile(ARQUIVO_ENTRADA)
print(f"Arquivo de entrada : {ARQUIVO_ENTRADA}")
print(f"Total de linhas    : {linhas.count()}")

# ---------------------------------------------------------------
# 3. MAP - quebra cada linha em palavras normalizadas
# ---------------------------------------------------------------
palavras = linhas.flatMap(lambda linha: re.findall(r"[a-zA-Z]+", linha.lower()))
print(f"Total de palavras  : {palavras.count()}")

# ---------------------------------------------------------------
# 4. MAP - transforma cada palavra no par (palavra, 1)
# ---------------------------------------------------------------
pares = palavras.map(lambda palavra: (palavra, 1))

# ---------------------------------------------------------------
# 5. REDUCE - soma os 1s de cada palavra
# ---------------------------------------------------------------
contagem = pares.reduceByKey(lambda a, b: a + b)
print(f"Palavras distintas : {contagem.count()}")

# ---------------------------------------------------------------
# 6. ACAO - Top 10 palavras mais frequentes
# ---------------------------------------------------------------
top10 = contagem.takeOrdered(10, key=lambda par: -par[1])
print("\nTop 10 palavras mais frequentes:")
for posicao, (palavra, total) in enumerate(top10, start=1):
    print(f"{posicao:2d}. {palavra:<15} {total}")

# ---------------------------------------------------------------
# 7. LOAD - grava o resultado completo em disco
# ---------------------------------------------------------------
contagem.sortBy(lambda par: -par[1]).saveAsTextFile(PASTA_SAIDA)
print(f"\nResultado completo gravado em: {PASTA_SAIDA}")

spark.stop()
```

> **Onde salvar**: salve o arquivo em `spark-tutorial/scripts/wordcount_rdd.py`
> (o arquivo ja existe se voce clonou o repositorio).

Execute (na pasta `spark-tutorial/`, com o venv ativado):

```bash
# Forma recomendada — spark-submit e a CLI oficial do Spark:
spark-submit scripts/wordcount_rdd.py

# Equivalente com o Python do venv (identico em modo local):
python scripts/wordcount_rdd.py
```

**Resultado esperado** (ignorando logs):
```
Arquivo de entrada : .../spark-tutorial/data/lorem.txt
Total de linhas    : 40
Total de palavras  : 1695
Palavras distintas : 939

Top 10 palavras mais frequentes:
 1. the             63
 2. a               48
 3. of              39
 4. and             30
 5. to              25
 6. in              25
 7. i               24
 8. you             16
 9. on              15
10. that            15

Resultado completo gravado em: .../spark-tutorial/output/wordcount
```

> As posicoes 5/6 e 9/10 sao empates (25/25 e 15/15) - podem aparecer trocadas entre
> execucoes. E normal: a ordem entre empates nao e deterministica em processamento
> paralelo.

> **Dica**: o script aceita outro arquivo como argumento:
> `python scripts/wordcount_rdd.py data/outro_texto.txt`

---

## 5. Validando o resultado gravado

```bash
ls output/wordcount/
```

**Resultado esperado**:
```
_SUCCESS    part-00000    part-00001
```

| Arquivo | O que e |
|---|---|
| `_SUCCESS` | Marcador vazio: o job terminou sem erro |
| `part-00000`, `part-00001` | O resultado, um arquivo por particao do RDD (para arquivos pequenos o `textFile` cria no minimo 2 particoes - `defaultMinPartitions`) |

Veja as primeiras contagens:

```bash
head -5 output/wordcount/part-00000
```

**Resultado esperado**:
```
('the', 63)
('a', 48)
('of', 39)
('and', 30)
('to', 25)
```

---

## 6. Executando no Docker

Com o container do `TUTORIAL_AMBIENTE_SPARK.md` rodando (`docker compose up -d` na pasta
`docker/`):

```bash
docker exec spark /opt/spark/bin/spark-submit scripts/wordcount_rdd.py
```

**Resultado esperado**: a mesma saida da secao 4 (com caminhos `/opt/spark/work-dir/...`).

O resultado gravado aparece na SUA maquina, em `docker/output/wordcount/` (a pasta esta
montada como volume):

```bash
head -5 docker/output/wordcount/part-00000
```

---

## 7. Via Jupyter Notebook

Rodando o WordCount no navegador, celula a celula — sem precisar do shell `pyspark`
ou de um arquivo `.py`.

> **Pre-requisito**: Jupyter instalado no mesmo venv (`pip install jupyter`). Veja
> a secao 8 do `TUTORIAL_AMBIENTE_SPARK.md` para detalhes de instalacao.

Na pasta `spark-tutorial/`, com o venv ativado, inicie o servidor:

```bash
jupyter notebook
```

Crie um novo notebook (**New > Python 3 (ipykernel)**) e execute cada bloco com
**Shift+Enter**:

**Celula 1 — Setup:**
```python
import re
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("WordCountNotebook")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("ERROR")
print(f"Spark {spark.version} pronto!")
```

**Celula 2 — Ler o arquivo:**
```python
linhas = sc.textFile("data/lorem.txt")
print(f"Total de linhas : {linhas.count()}")
print(f"Primeira linha  : {linhas.first()}")
```

**Celula 3 — MAP: extrair palavras:**
```python
palavras = linhas.flatMap(lambda l: re.findall(r"[a-zA-Z]+", l.lower()))
print(f"Total de palavras: {palavras.count()}")
print(f"Amostra          : {palavras.take(5)}")
```

**Celula 4 — REDUCE: contar por palavra:**
```python
pares = palavras.map(lambda p: (p, 1))
contagem = pares.reduceByKey(lambda a, b: a + b)
print(f"Palavras distintas: {contagem.count()}")
```

**Celula 5 — Top 10:**
```python
top10 = contagem.takeOrdered(10, key=lambda par: -par[1])
for palavra, total in top10:
    print(f"{palavra:<15} {total}")
```

**Celula 6 — Gravar resultado:**
```python
import shutil
shutil.rmtree("output/wordcount_notebook", ignore_errors=True)
contagem.sortBy(lambda par: -par[1]).saveAsTextFile("output/wordcount_notebook")
print("Salvo em output/wordcount_notebook/")
```

**Celula 7 — Encerrar:**
```python
spark.stop()
```

> O resultado e identico ao do script: 40 linhas, 1.695 palavras, 939 distintas,
> Top 1: "the" com 63 ocorrencias.

---

## 8. Visualizando o job na Spark UI

Para ver o MapReduce acontecendo, rode o passo a passo da secao 3 no shell e, com o shell
aberto, acesse **http://localhost:4040**:

> Se o container Docker estiver rodando, a porta 4040 do host esta ocupada por ele -
> use `http://127.0.0.1:4040` (ou veja no log do shell qual porta foi anunciada: 4041,
> 4042...).

- Aba **Jobs**: cada acao (`count`, `takeOrdered`, `saveAsTextFile`) virou um job.
- Clique em um job > **DAG Visualization**: veja os stages separados pelo SHUFFLE que o
  `reduceByKey` provocou.
- Aba **Stages**: quantas tasks (uma por particao) rodaram em paralelo.

---

## 9. Desafios extras

Para fixar, modifique o exercicio (use uma copia do script):

1. **Stopwords**: remova palavras sem significado ("the", "a", "of", "and"...) com um
   `filter` antes do `map`. Qual e o novo Top 10?
2. **Palavras grandes**: conte apenas palavras com 8+ letras
   (`filter(lambda p: len(p) >= 8)`).
3. **Iniciais**: conte quantas palavras comecam com cada letra
   (`map(lambda p: (p[0], 1))`).
4. **Outro arquivo**: rode sobre um texto seu:
   `python scripts/wordcount_rdd.py caminho/do/arquivo.txt`.
5. **Uma particao so**: troque `saveAsTextFile(...)` por
   `coalesce(1).saveAsTextFile(...)` e observe que sai um unico `part-00000`
   (e entenda por que isso e ruim para arquivos grandes).

---

## 10. Troubleshooting

### `FileAlreadyExistsException: Output directory ... already exists`

`saveAsTextFile` NUNCA sobrescreve (heranca do Hadoop, para proteger resultados).
Apague a pasta antes: `rm -rf output/wordcount_shell` - o script da secao 4 ja faz isso
via `shutil.rmtree`.

### `Py4JJavaError ... Input path does not exist`

O caminho relativo `data/lorem.txt` so funciona se voce abriu o shell DENTRO da pasta
`spark-tutorial/`. Confira com `pwd` e abra o shell no lugar certo.

### O Top 10 veio em ordem diferente do tutorial

Empates (mesma contagem) podem trocar de posicao entre execucoes - compare os PARES
(palavra, total), nao as posicoes.

### Erro `TaskResultLost` ou `Failed to connect to /10.x.x.x`

VPN/rede corporativa - veja o troubleshooting do `TUTORIAL_AMBIENTE_SPARK.md`
(exporte `SPARK_LOCAL_IP=127.0.0.1`).

### No shell, nada acontece quando rodo `flatMap`/`map`/`reduceByKey`

Correto! Sao transformacoes lazy - so executam quando voce chama uma acao
(`count`, `take`, `saveAsTextFile`). Se nao deu erro, esta funcionando.
