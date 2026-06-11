# Tutorial: Ambiente de Desenvolvimento Apache Spark

> Guia passo a passo para montar um ambiente Spark de desenvolvimento na sua maquina.
> Ao final, voce tera o Spark 3.5 rodando localmente (via Python) **e** via Docker,
> com um Hello World executado com sucesso e o ambiente preparado para usar
> Apache Iceberg nos exercicios seguintes.

---

## Sumario

1. [O que voce vai instalar](#1-o-que-voce-vai-instalar)
2. [Como o Spark funciona (conceitos essenciais)](#2-como-o-spark-funciona-conceitos-essenciais)
3. [Estrutura do projeto](#3-estrutura-do-projeto)
4. [Caminho A - Instalacao local via Python](#4-caminho-a---instalacao-local-via-python)
   - [4.1 macOS](#41-macos)
   - [4.2 Linux (Ubuntu)](#42-linux-ubuntu)
   - [4.3 Windows](#43-windows)
   - [4.4 Criar o ambiente virtual e instalar o PySpark](#44-criar-o-ambiente-virtual-e-instalar-o-pyspark)
   - [4.5 Testar o shell interativo (pyspark)](#45-testar-o-shell-interativo-pyspark)
   - [4.6 Executar o Hello World](#46-executar-o-hello-world)
5. [Caminho B - Spark via Docker](#5-caminho-b---spark-via-docker)
   - [5.1 Instalar o Docker](#51-instalar-o-docker)
   - [5.2 Entendendo o docker-compose.yml](#52-entendendo-o-docker-composeyml)
   - [5.3 Subir o ambiente](#53-subir-o-ambiente)
   - [5.4 Hello World dentro do container](#54-hello-world-dentro-do-container)
   - [5.5 Shell interativo dentro do container](#55-shell-interativo-dentro-do-container)
   - [5.6 Parar e remover o ambiente](#56-parar-e-remover-o-ambiente)
6. [Preparando o ambiente para o Apache Iceberg](#6-preparando-o-ambiente-para-o-apache-iceberg)
7. [Spark UI - acompanhando seus jobs](#7-spark-ui---acompanhando-seus-jobs)
8. [Executando via Jupyter Notebook](#8-executando-via-jupyter-notebook)
9. [Troubleshooting](#9-troubleshooting)
10. [Proximos passos](#10-proximos-passos)

---

## 1. O que voce vai instalar

| Componente | Versao | O que faz |
|---|---|---|
| JDK (Java) | 17 | O Spark roda na JVM - o Java e obrigatorio mesmo usando Python |
| Python | 3.10 a 3.12 | Linguagem que usaremos para programar no Spark (PySpark) |
| PySpark | 3.5.8 | O Apache Spark completo, distribuido como pacote Python |
| Docker Desktop / Engine | ultima | (Caminho B) roda o Spark em um container, sem instalar Java/Python |
| Apache Iceberg (conector) | 1.10.2 | Formato de tabela para Lakehouse - usado no tutorial de Iceberg |

> **Importante sobre versoes**: o Spark 3.5 funciona com Java **8, 11 ou 17** - nao
> funciona com Java 21+ (por exemplo, Java 24 ou 26). E funciona com Python **3.8 a 3.12** -
> nao funciona com Python 3.13+. Este tutorial padroniza **JDK 17 + Python 3.10-3.12**.

---

## 2. Como o Spark funciona (conceitos essenciais)

Antes de instalar, entenda o que voce esta instalando:

- **Apache Spark** e um motor de processamento distribuido: ele divide seus dados em
  particoes e processa as particoes em paralelo, em um cluster de maquinas.
- Toda aplicacao Spark tem um **driver** (o programa principal, que coordena) e
  **executors** (os processos que fazem o trabalho pesado).
- No **modo local** - que usaremos aqui - driver e executors rodam todos na sua maquina,
  usando os nucleos da CPU como "mini cluster". E o modo ideal para aprender e desenvolver:
  o mesmo codigo que roda local roda depois em um cluster de verdade (EMR, Databricks, k8s).
- `master("local[*]")` significa "rode local usando todos os nucleos disponiveis".
- **PySpark** e a API Python do Spark. Ao instalar o pacote `pyspark` pelo `pip`, voce
  instala o Spark inteiro (os JARs da JVM vem juntos no pacote) - por isso o Java e
  pre-requisito mesmo programando so em Python.

```
┌─────────────────────────── sua maquina ───────────────────────────┐
│  Python (seu script)  ──>  Driver JVM  ──>  Executors (threads)   │
│        PySpark                Spark              local[*]          │
└────────────────────────────────────────────────────────────────────┘
```

---

## 3. Estrutura do projeto

Os tutoriais usam a pasta `tutoriais/spark-tutorial/` com esta estrutura:

```
spark-tutorial/
├── TUTORIAL_AMBIENTE_SPARK.md      <- voce esta aqui
├── TUTORIAL_SPARK_RDD.md           <- exercicio WordCount (MapReduce)
├── TUTORIAL_SPARK_DATAFRAME.md     <- exercicio ETL com jcr.csv e scimago.csv
├── TUTORIAL_SPARK_ICEBERG.md       <- exercicio Lakehouse com Iceberg
├── QUICK_TUTORIAL_*.md             <- versoes resumidas (so comandos)
├── data/
│   ├── lorem.txt                   <- texto de entrada do WordCount
│   ├── jcr.csv                     <- ranking de periodicos (JCR)
│   └── scimago.csv                 <- ranking de periodicos (Scimago)
├── scripts/
│   ├── hello_spark.py              <- Hello World (teste do ambiente)
│   ├── wordcount_rdd.py            <- solucao do exercicio RDD
│   ├── exercicio_dataframe.py      <- solucao do exercicio DataFrame
│   └── exercicio_iceberg.py        <- solucao do exercicio Iceberg
├── docker/
│   └── docker-compose.yml          <- ambiente Spark via Docker
├── output/                         <- resultados gravados pelo Spark (criado na execucao)
└── warehouse/                      <- tabelas Iceberg (criado na execucao)
```

> **Atencao**: `output/` e `warehouse/` valem para as execucoes LOCAIS. Quando voce
> executa pelo Docker, os resultados aparecem em `docker/output/` e `docker/warehouse/`
> (sao os volumes montados no container).

Abra um terminal e navegue ate a pasta (ajuste o caminho para onde voce salvou o projeto):

```bash
cd ~/Documents/Big\ Data/tutoriais/spark-tutorial
```

No Windows (PowerShell):

```powershell
cd "$env:USERPROFILE\Documents\Big Data\tutoriais\spark-tutorial"
```

---

## 4. Caminho A - Instalacao local via Python

Siga a subsecao do seu sistema operacional (4.1, 4.2 ou 4.3) para instalar **Java 17 e
Python**, depois continue em [4.4](#44-criar-o-ambiente-virtual-e-instalar-o-pyspark),
que e igual para todos.

> Este tutorial foi testado em macOS (Apple Silicon) e em container Ubuntu.
> Os passos de Windows seguem a documentacao oficial, mas nao foram testados
> automaticamente - se algo divergir, veja o [Troubleshooting](#8-troubleshooting).

### 4.1 macOS

#### Passo 1 - Homebrew (se ainda nao tiver)

```bash
brew --version
```

Se aparecer a versao (`Homebrew 4` ou superior), pule. Senao, instale com:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

#### Passo 2 - Instalar o JDK 17

```bash
brew install openjdk@17
```

#### Passo 3 - Apontar o JAVA_HOME para o JDK 17

O macOS pode ter varios Javas instalados. A variavel `JAVA_HOME` diz ao Spark qual usar:

```bash
echo 'export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"' >> ~/.zshrc
echo 'export SPARK_LOCAL_IP=127.0.0.1' >> ~/.zshrc
source ~/.zshrc
```

> **Por que `SPARK_LOCAL_IP=127.0.0.1`?** Em redes corporativas/VPN, o Spark tenta usar o
> IP da rede e os processos internos nao conseguem falar entre si (erro `TaskResultLost` /
> `Failed to connect`). Fixar no localhost evita esse problema no modo local.

Confira:

```bash
"$JAVA_HOME/bin/java" -version
```

**Resultado esperado** (o patch pode variar):
```
openjdk version "17.0.19" 2026-04-21
OpenJDK Runtime Environment Homebrew (build 17.0.19+0)
```

#### Passo 4 - Conferir o Python

```bash
python3 --version
```

**Resultado esperado**: `Python 3.10.x`, `3.11.x` ou `3.12.x`. Se voce tiver 3.13+,
instale uma versao suportada: `brew install python@3.12` (e use `python3.12` nos
comandos seguintes).

Continue em [4.4](#44-criar-o-ambiente-virtual-e-instalar-o-pyspark).

### 4.2 Linux (Ubuntu)

#### Passo 1 - Instalar JDK 17, Python e venv

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk python3 python3-venv python3-pip
```

#### Passo 2 - Apontar o JAVA_HOME

```bash
echo 'export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))' >> ~/.bashrc
echo 'export SPARK_LOCAL_IP=127.0.0.1' >> ~/.bashrc
source ~/.bashrc
```

> Se voce tiver mais de um Java instalado, escolha o 17 com
> `sudo update-alternatives --config java` antes de rodar os comandos acima.

Confira:

```bash
java -version
```

**Resultado esperado**:
```
openjdk version "17.0.x" ...
```

#### Passo 3 - Conferir o Python

```bash
python3 --version
```

**Resultado esperado**: `Python 3.10.x` a `3.12.x` (Ubuntu 22.04 traz 3.10; Ubuntu 24.04 traz 3.12).

Continue em [4.4](#44-criar-o-ambiente-virtual-e-instalar-o-pyspark).

### 4.3 Windows

> Use o **PowerShell** (menu Iniciar > digite "PowerShell").

#### Passo 1 - Instalar o JDK 17 (Eclipse Temurin)

```powershell
winget install EclipseAdoptium.Temurin.17.JDK
```

Durante a instalacao, se aparecer a opcao **"Set JAVA_HOME variable"**, marque-a.

Sem winget? Baixe o instalador `.msi` em https://adoptium.net/temurin/releases/?version=17
e marque a opcao "Set JAVA_HOME" no assistente.

#### Passo 2 - Instalar o Python 3.12

```powershell
winget install Python.Python.3.12
```

#### Passo 3 - Conferir JAVA_HOME e Python (abra um PowerShell NOVO)

```powershell
java -version
echo $env:JAVA_HOME
python --version
```

**Resultado esperado**:
```
openjdk version "17.0.x" ...
C:\Program Files\Eclipse Adoptium\jdk-17.0.x.x-hotspot\
Python 3.12.x
```

Se `JAVA_HOME` vier vazio, defina manualmente (ajuste o numero da versao para a pasta
que existir em `C:\Program Files\Eclipse Adoptium\`):

```powershell
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17.0.19.9-hotspot"
```

e abra um PowerShell novo.

#### Passo 4 - Instalar o winutils (necessario para o Spark GRAVAR arquivos no Windows)

O Spark usa bibliotecas do Hadoop que, no Windows, precisam dos utilitarios
`winutils.exe` e `hadoop.dll`:

1. Acesse https://github.com/cdarlint/winutils e baixe o repositorio
   (**Code > Download ZIP**)
2. Extraia e copie a pasta `hadoop-3.3.6\bin` para `C:\hadoop\bin`
   (o resultado final deve ser `C:\hadoop\bin\winutils.exe`)
3. Defina as variaveis:

```powershell
setx HADOOP_HOME "C:\hadoop"
[Environment]::SetEnvironmentVariable("Path", [Environment]::GetEnvironmentVariable("Path","User") + ";C:\hadoop\bin", "User")
```

> (Evite `setx PATH ...`: ele trunca o PATH em 1024 caracteres e mistura o PATH de
> sistema com o de usuario. O comando acima altera apenas o PATH do usuario, sem
> truncar. Alternativa grafica: Iniciar > "Editar variaveis de ambiente".)

4. Feche e abra o PowerShell novamente.

Continue em [4.4](#44-criar-o-ambiente-virtual-e-instalar-o-pyspark).

### 4.4 Criar o ambiente virtual e instalar o PySpark

> **Conceito - ambiente virtual (venv)**: uma "caixinha" isolada de pacotes Python para o
> projeto. Evita conflito entre versoes de bibliotecas de projetos diferentes.

Na pasta `spark-tutorial/`:

**macOS / Linux:**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark==3.5.8
```

**Windows (PowerShell):**

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install pyspark==3.5.8
```

> Se a ativacao falhar com erro de "execution policy", rode
> `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser` e tente de novo.

> Opcional, recomendado: `pip install psutil` - elimina o aviso
> `Please install psutil to have better support with spilling` nas execucoes.

A instalacao baixa ~400 MB (o Spark inteiro vem dentro do pacote). Confira:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

**Resultado esperado**:
```
3.5.8
```

> Com o venv **ativado**, `python` ja aponta para o Python do projeto - em todos os
> sistemas. Os comandos a seguir assumem o venv ativado (o prompt mostra `(.venv)`).

### 4.5 Testar o shell interativo (pyspark)

O `pyspark` e um terminal Python com o Spark ja carregado - otimo para experimentar:

```bash
pyspark
```

**Resultado esperado** (apos alguns segundos):
```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.8
      /_/

Using Python version 3.12.x ...
SparkSession available as 'spark'.
>>>
```

Teste com um comando (digite e pressione Enter):

```python
spark.range(5).show()
```

**Resultado esperado**:
```
+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
+---+
```

Saia do shell:

```python
exit()
```

### 4.6 Executar o Hello World

O script `scripts/hello_spark.py` cria uma SparkSession, monta um DataFrame de 3 linhas
e imprime na tela - se ele rodar, seu ambiente esta funcionando. Conteudo do script:

```python
"""
hello_spark.py - Primeiro programa com Apache Spark
Cria uma SparkSession, monta um DataFrame minusculo e mostra na tela.
Se este script rodar, seu ambiente Spark esta funcionando.
"""
from pyspark.sql import SparkSession

# 1. Cria (ou reaproveita) a sessao Spark - porta de entrada de tudo no Spark
spark = (
    SparkSession.builder
    .appName("HelloSpark")
    .master("local[*]")  # roda local, usando todos os nucleos da maquina
    # Fixa o driver no localhost - evita erros de rede em VPN/Wi-Fi corporativo
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)

# 2. Reduz o volume de logs para enxergar melhor a saida
spark.sparkContext.setLogLevel("ERROR")

print("=" * 50)
print("Hello, Spark!")
print(f"Versao do Spark : {spark.version}")
print(f"Master          : {spark.sparkContext.master}")
print("=" * 50)

# 3. Cria um DataFrame de teste com 3 linhas
dados = [("Ana", 28), ("Bruno", 34), ("Carla", 25)]
df = spark.createDataFrame(dados, ["nome", "idade"])

# 4. Acoes: show() imprime a tabela, count() conta as linhas
df.show()
print(f"Total de linhas: {df.count()}")

# 5. Encerra a sessao
spark.stop()
print("Sessao encerrada com sucesso. Ambiente OK!")
```

> **Onde salvar**: salve o arquivo em `spark-tutorial/scripts/hello_spark.py`
> (o arquivo ja existe se voce clonou o repositorio).

Execute (a partir da pasta `spark-tutorial/`, com o venv ativado):

```bash
# Forma recomendada — spark-submit e a CLI oficial do Spark:
spark-submit scripts/hello_spark.py

# Equivalente com o Python do venv (identico em modo local):
python scripts/hello_spark.py
```

**Resultado esperado** (ignore as linhas de log WARN do Spark):
```
==================================================
Hello, Spark!
Versao do Spark : 3.5.8
Master          : local[*]
==================================================
+-----+-----+
| nome|idade|
+-----+-----+
|  Ana|   28|
|Bruno|   34|
|Carla|   25|
+-----+-----+

Total de linhas: 3
Sessao encerrada com sucesso. Ambiente OK!
```

**Ambiente local pronto!** Voce pode ir direto para a
[secao 6 (Iceberg)](#6-preparando-o-ambiente-para-o-apache-iceberg) ou conhecer o
caminho Docker a seguir.

---

## 5. Caminho B - Spark via Docker

> **Conceito - por que Docker?** O container traz Spark, Java e Python ja instalados e
> identicos para todo mundo - zero problema de "na minha maquina funciona". E a forma
> mais proxima de como o Spark roda em producao (Kubernetes, EMR etc.).

### 5.1 Instalar o Docker

Verifique se ja tem:

```bash
docker --version
```

Se aparecer `Docker version 2x.x.x`, pule para [5.2](#52-entendendo-o-docker-composeyml).

#### macOS

1. Acesse https://www.docker.com/products/docker-desktop/
2. Baixe para **Apple Silicon** (M1/M2/M3/M4) ou **Intel**, conforme seu chip
3. Abra o `.dmg`, arraste o Docker para Applications e abra o aplicativo

#### Windows

1. Acesse https://www.docker.com/products/docker-desktop/
2. Execute o instalador e marque **"Use WSL 2 instead of Hyper-V"** se solicitado
3. Reinicie se pedido e abra o **Docker Desktop**

#### Linux (Ubuntu)

```bash
sudo apt update
sudo apt install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker
```

Em todos os sistemas, confirme que o daemon esta rodando:

```bash
docker info | head -5
```

(Se der erro "Cannot connect to the Docker daemon", abra o Docker Desktop e aguarde.)

### 5.2 Entendendo o docker-compose.yml

O arquivo `docker/docker-compose.yml` descreve o ambiente:

```yaml
services:
  spark:
    image: apache/spark:3.5.8-python3       # imagem oficial: Spark 3.5.8 + Python 3
    container_name: spark
    user: root                               # simplifica permissoes nos volumes
    command: ["tail", "-f", "/dev/null"]     # mantem o container vivo, aguardando voce
    working_dir: /opt/spark/work-dir
    ports:
      - "4040:4040"                          # Spark UI da aplicacao em execucao
    volumes:
      - ../data:/opt/spark/work-dir/data:ro        # dados de entrada (somente leitura)
      - ../scripts:/opt/spark/work-dir/scripts     # scripts dos exercicios
      - ./output:/opt/spark/work-dir/output        # resultados gravados pelo Spark
      - ./warehouse:/opt/spark/work-dir/warehouse  # tabelas Iceberg
      - ivy-cache:/root/.ivy2                      # cache do conector Iceberg

volumes:
  ivy-cache:
```

Linha a linha:

| Trecho | O que faz |
|---|---|
| `image: apache/spark:3.5.8-python3` | Imagem oficial do projeto Apache Spark, com Python embutido |
| `user: root` | Evita problemas de permissao de escrita nos volumes (ok para estudo) |
| `command: tail -f /dev/null` | "Nao faca nada, so fique vivo" - voce entra no container quando quiser |
| `ports: 4040:4040` | Expoe a Spark UI para abrir no navegador da sua maquina |
| `volumes: ../data:...` | Compartilha pastas da sua maquina com o container - os resultados aparecem no seu disco |
| `ivy-cache` | Volume nomeado que guarda o conector Iceberg baixado (nao baixa de novo a cada uso) |

### 5.3 Subir o ambiente

```bash
cd docker
docker compose up -d
```

**Resultado esperado** (na primeira vez baixa a imagem, ~1 GB):
```
[+] Running 2/2
 ✔ Volume "docker_ivy-cache"  Created
 ✔ Container spark            Started
```

Confira que o container esta rodando:

```bash
docker ps --filter name=spark
```

**Resultado esperado**: uma linha com `apache/spark:3.5.8-python3` e STATUS `Up`.

### 5.4 Hello World dentro do container

Dentro da imagem, o Spark fica em `/opt/spark`. O comando `spark-submit` e a forma
padrao de executar aplicacoes Spark (local e em cluster):

```bash
docker exec spark /opt/spark/bin/spark-submit scripts/hello_spark.py
```

**Resultado esperado** (entre as linhas de log):
```
==================================================
Hello, Spark!
Versao do Spark : 3.5.8
Master          : local[*]
==================================================
+-----+-----+
| nome|idade|
+-----+-----+
|  Ana|   28|
|Bruno|   34|
|Carla|   25|
+-----+-----+

Total de linhas: 3
Sessao encerrada com sucesso. Ambiente OK!
```

> Repare: e o MESMO script da instalacao local - as pastas `scripts/` e `data/` estao
> montadas dentro do container via volumes.

### 5.5 Shell interativo dentro do container

```bash
docker exec -it spark /opt/spark/bin/pyspark
```

Teste:

```python
spark.range(5).show()
exit()
```

Para abrir um terminal bash dentro do container (explorar arquivos, etc.):

```bash
docker exec -it spark bash
ls data/ scripts/
exit
```

### 5.6 Parar e remover o ambiente

```bash
docker compose down        # para e remove o container (mantem o cache do Iceberg)
docker compose down -v     # idem, e apaga tambem o volume de cache
```

---

## 6. Preparando o ambiente para o Apache Iceberg

> **Conceito**: o **Apache Iceberg** e um *formato de tabela* para data lakes - ele
> adiciona transacoes ACID, UPDATE/DELETE via SQL, controle de schema e **time travel**
> sobre arquivos Parquet. E a base do conceito de **Lakehouse**. Voce vai usa-lo no
> `TUTORIAL_SPARK_ICEBERG.md`.

O Iceberg **nao precisa de instalacao separada**: ele e um conector (um JAR) que o
proprio Spark baixa do Maven Central na primeira execucao, atraves da opcao
`--packages`. So precisamos validar que o download e a configuracao funcionam.

### 6.1 Teste rapido - ambiente local

Com o venv ativado, abra o shell pyspark ja configurado para Iceberg:

```bash
pyspark \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=warehouse
```

No Windows (PowerShell), o caractere de continuacao de linha e o acento grave:

```powershell
pyspark `
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2 `
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog `
  --conf spark.sql.catalog.local.type=hadoop `
  --conf spark.sql.catalog.local.warehouse=warehouse
```

O que cada configuracao significa:

| Configuracao | Significado |
|---|---|
| `--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2` | Baixa o conector Iceberg 1.10.2 compilado para Spark 3.5 / Scala 2.12 |
| `spark.sql.extensions=...IcebergSparkSessionExtensions` | Habilita os comandos SQL extras do Iceberg (UPDATE, MERGE, CALL...) |
| `spark.sql.catalog.local=...SparkCatalog` | Cria um catalogo de tabelas chamado `local` gerenciado pelo Iceberg |
| `spark.sql.catalog.local.type=hadoop` | Catalogo "hadoop": os metadados ficam em arquivos no proprio diretorio |
| `spark.sql.catalog.local.warehouse=warehouse` | Pasta onde as tabelas (dados + metadados) serao gravadas |

Na primeira execucao voce vera o download do pacote:

```
org.apache.iceberg#iceberg-spark-runtime-3.5_2.12 added as a dependency
...
	found org.apache.iceberg#iceberg-spark-runtime-3.5_2.12;1.10.2 in central
```

Dentro do shell, crie uma tabela Iceberg de teste:

```python
spark.sql("CREATE TABLE local.db.teste (id INT, nome STRING) USING iceberg")
spark.sql("INSERT INTO local.db.teste VALUES (1, 'spark'), (2, 'iceberg')")
spark.sql("SELECT * FROM local.db.teste ORDER BY id").show()
```

**Resultado esperado**:
```
+---+-------+
| id|   nome|
+---+-------+
|  1|  spark|
|  2|iceberg|
+---+-------+
```

Limpe a tabela de teste e saia:

```python
spark.sql("DROP TABLE local.db.teste PURGE")
exit()
```

**Ambiente local preparado para Iceberg!**

### 6.2 Teste rapido - Docker

Com o container do passo 5 rodando, o mesmo teste via `spark-sql` (shell SQL do Spark):

```bash
docker exec -it spark /opt/spark/bin/spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=warehouse \
  -e "CREATE TABLE IF NOT EXISTS local.db.teste (id INT, nome STRING) USING iceberg; INSERT INTO local.db.teste VALUES (1, 'spark'), (2, 'iceberg'); SELECT * FROM local.db.teste ORDER BY id; DROP TABLE local.db.teste PURGE;"
```

**Resultado esperado** (no final da saida):
```
1	spark
2	iceberg
```

> O download fica guardado no volume `ivy-cache` - nas proximas execucoes o Iceberg
> carrega instantaneamente.

---

## 7. Spark UI - acompanhando seus jobs

Enquanto uma aplicacao Spark esta rodando, ela publica uma interface web na porta
**4040** com os jobs, stages, tasks e uso de memoria.

Para ver a UI com calma, abra um shell (`pyspark` local, ou no Docker conforme a secao
5.5), rode um comando qualquer (`spark.range(1000000).count()`) e acesse no navegador:

```
http://localhost:4040
```

Voce vera as abas **Jobs**, **Stages**, **Storage**, **Environment**, **Executors** e
**SQL / DataFrame**. A UI morre junto com a aplicacao - ela so existe enquanto a sessao
esta aberta.

> **Atencao**: se o container Docker estiver no ar, a porta 4040 do host ja esta
> reservada para ele. Uma sessao LOCAL aberta ao mesmo tempo vai para a porta seguinte
> (4041, 4042...) - veja no log do shell qual porta foi anunciada (e o item "Porta 4040
> ja em uso" do Troubleshooting).

---

## 8. Executando via Jupyter Notebook

O Jupyter Notebook permite escrever e executar codigo PySpark celula a celula no
navegador — ideal para aulas, exploracao interativa e visualizacao de resultados
etapa por etapa.

### 8.1 Instalar o Jupyter

Com o venv ativado (mesmo ambiente onde instalou o PySpark):

```bash
pip install jupyter
```

Confira:

```bash
jupyter --version
```

**Resultado esperado**: linha com `jupyter core : 5.x.x` e `notebook : 7.x.x`.

### 8.2 Iniciar o servidor

Na pasta `spark-tutorial/`, com o venv ativado:

```bash
jupyter notebook
```

O navegador abrira automaticamente em `http://localhost:8888`. Se nao abrir, copie
o link exibido no terminal (comeca com `http://localhost:8888/?token=...`).

> Para encerrar o servidor: pressione `Ctrl+C` duas vezes no terminal.

### 8.3 Hello World no Notebook

Na interface do Jupyter, clique em **New > Python 3 (ipykernel)** para criar um
notebook em branco. Execute cada celula com **Shift+Enter**:

**Celula 1 — Iniciar a sessao Spark:**
```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HelloSparkNotebook")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print(f"Spark {spark.version} pronto!")
```

**Celula 2 — Criar e mostrar um DataFrame:**
```python
dados = [("Ana", 28), ("Bruno", 34), ("Carla", 25)]
df = spark.createDataFrame(dados, ["nome", "idade"])
df.show()
print(f"Total de linhas: {df.count()}")
```

**Celula 3 — Encerrar a sessao:**
```python
spark.stop()
print("Sessao encerrada.")
```

> **Atencao**: apos `spark.stop()`, voce precisa re-executar a Celula 1 para criar
> uma nova sessao. Cada notebook deve ter apenas UMA SparkSession ativa por vez.

### 8.4 Configuracao para Iceberg no Notebook

No Jupyter nao e possivel passar flags `--packages` na linha de comando. A
configuracao vai inteiramente no `SparkSession.builder`:

```python
from pyspark.sql import SparkSession

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

> Na primeira execucao desta celula, o download do conector Iceberg (~100 MB)
> aparece no TERMINAL onde voce iniciou `jupyter notebook`. O kernel do navegador
> pode parecer travado durante o download — aguarde ate a celula exibir "prontos!".

---

## 9. Troubleshooting

### "Unable to locate a Java Runtime" / "JAVA_HOME is not set"

O Spark nao achou o Java. Confirme `echo $JAVA_HOME` (macOS/Linux) ou
`echo $env:JAVA_HOME` (Windows) e refaca o passo de JAVA_HOME da sua plataforma.
Lembre de abrir um terminal NOVO depois de definir variaveis.

### "java.lang.UnsupportedOperationException: getSubject is supported only if..." ou "Unsupported class file major version"

Voce esta com um Java mais novo que o 17 (ex.: Java 21, 24 ou 26). O Spark 3.5 exige
Java 8, 11 ou 17. Aponte o `JAVA_HOME` para o JDK 17 (passo da sua plataforma) e abra um
terminal novo.

### Erro `TaskResultLost` ou `Failed to connect to /10.x.x.x:porta`

Classico de **VPN / rede corporativa**: o Spark fez bind no IP da rede e nao consegue
falar consigo mesmo. Solucoes (qualquer uma):
- exportar `SPARK_LOCAL_IP=127.0.0.1` (ja incluido na secao 4 deste tutorial), ou
- nas SparkSession dos scripts: `.config("spark.driver.bindAddress", "127.0.0.1")` e
  `.config("spark.driver.host", "127.0.0.1")` (os scripts deste tutorial ja tem).

### "Python in worker has different version ... than that in driver" 

Voce tem mais de um Python e o Spark esta misturando-os. Rode sempre com o venv ativado.
Se persistir, exporte `PYSPARK_PYTHON` apontando para o python do venv.

### PySpark nao instala ou falha ao iniciar com Python 3.13+

O PySpark 3.5 suporta Python ate o 3.12. Instale o 3.12 e recrie o venv:
`python3.12 -m venv .venv`.

### Windows: erro ao GRAVAR arquivos (`HADOOP_HOME and hadoop.home.dir are unset`, `NativeIO$Windows`)

Falta o winutils - refaca o [passo 4 da secao 4.3](#43-windows) e abra um PowerShell novo.
Leituras funcionam sem winutils; gravacoes geralmente nao.

### Windows: `.venv\Scripts\Activate.ps1 cannot be loaded because running scripts is disabled`

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Docker: "Cannot connect to the Docker daemon"

O Docker Desktop nao esta aberto (macOS/Windows) ou o servico nao subiu (Linux:
`sudo systemctl start docker`).

### Docker: `exec: "spark-submit": executable file not found`

Dentro do container use sempre o caminho completo: `/opt/spark/bin/spark-submit`,
`/opt/spark/bin/pyspark`, `/opt/spark/bin/spark-sql`.

### Iceberg: download do pacote falha (timeout, proxy)

O `--packages` baixa do Maven Central na primeira vez - precisa de internet. Em redes
com proxy, configure as variaveis `HTTP_PROXY`/`HTTPS_PROXY` no terminal. O download fica
em cache (`~/.ivy2` local; volume `ivy-cache` no Docker), entao so precisa funcionar uma vez.

### Porta 4040 ja em uso

Outra sessao Spark esta aberta. O Spark automaticamente tenta 4041, 4042... - veja no log
qual porta ele anunciou (`Bound SparkUI to ...`).

### A primeira execucao e lenta

Normal: ha o custo de subir a JVM e, no Docker/Iceberg, o download de imagem/conector.
As execucoes seguintes sao bem mais rapidas.

---

## 10. Proximos passos

Ambiente pronto! Agora siga os exercicios, nesta ordem:

1. **TUTORIAL_SPARK_RDD.md** - WordCount com a API RDD (o "Hello World" do MapReduce)
2. **TUTORIAL_SPARK_DATAFRAME.md** - ETL com DataFrames usando os rankings JCR e Scimago
3. **TUTORIAL_SPARK_ICEBERG.md** - Lakehouse local: tabela Iceberg, UPDATE e time travel
