# Quick Tutorial: Ambiente Spark

> So os comandos. Explicacoes: `TUTORIAL_AMBIENTE_SPARK.md`.

---

## 1. Instalacao local (Python)

### macOS

```bash
brew install openjdk@17
echo 'export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"' >> ~/.zshrc
echo 'export SPARK_LOCAL_IP=127.0.0.1' >> ~/.zshrc
source ~/.zshrc
```

### Ubuntu

```bash
sudo apt update && sudo apt install -y openjdk-17-jdk python3 python3-venv python3-pip
echo 'export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))' >> ~/.bashrc
echo 'export SPARK_LOCAL_IP=127.0.0.1' >> ~/.bashrc
source ~/.bashrc
```

### Windows (PowerShell)

```powershell
winget install EclipseAdoptium.Temurin.17.JDK   # marcar "Set JAVA_HOME"
winget install Python.Python.3.12
# winutils: baixar https://github.com/cdarlint/winutils (pasta hadoop-3.3.6\bin -> C:\hadoop\bin)
setx HADOOP_HOME "C:\hadoop"
setx PATH "$env:PATH;C:\hadoop\bin"
# abrir um PowerShell NOVO depois disso
```

### Todos os sistemas - venv + PySpark

```bash
cd tutoriais/spark-tutorial
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\Activate.ps1
pip install pyspark==3.5.8
```

### Testar

Script em: `spark-tutorial/scripts/hello_spark.py`

```bash
python -c "import pyspark; print(pyspark.__version__)"   # 3.5.8

# spark-submit (CLI oficial):
spark-submit scripts/hello_spark.py

# ou via Python do venv:
python scripts/hello_spark.py
```

**Resultado esperado**: tabela com Ana/Bruno/Carla e `Sessao encerrada com sucesso. Ambiente OK!`

---

## 2. Via Docker

```bash
cd docker
docker compose up -d
docker exec spark /opt/spark/bin/spark-submit scripts/hello_spark.py
```

**Resultado esperado**: mesma saida do hello local (versao 3.5.8).

Shell interativo e desligamento:

```bash
docker exec -it spark /opt/spark/bin/pyspark   # exit() para sair
docker compose down                            # parar (use -v para apagar cache Iceberg)
```

---

## 3. Via Jupyter Notebook

```bash
pip install jupyter        # so na primeira vez, com o venv ativado
jupyter notebook           # abre http://localhost:8888
```

Novo notebook > **New > Python 3** > Shift+Enter em cada celula:

```python
# Celula 1
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("HelloNB").master("local[*]")
         .config("spark.driver.bindAddress","127.0.0.1")
         .config("spark.driver.host","127.0.0.1").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print(f"Spark {spark.version} pronto!")
```

```python
# Celula 2
df = spark.createDataFrame([("Ana",28),("Bruno",34),("Carla",25)],["nome","idade"])
df.show()
```

```python
# Celula 3
spark.stop()
```

---

## 4. Smoke test Iceberg

### Local

```bash
pyspark \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=warehouse
```

```python
spark.sql("CREATE TABLE local.db.teste (id INT, nome STRING) USING iceberg")
spark.sql("INSERT INTO local.db.teste VALUES (1, 'spark'), (2, 'iceberg')")
spark.sql("SELECT * FROM local.db.teste ORDER BY id").show()
spark.sql("DROP TABLE local.db.teste PURGE")
exit()
```

**Resultado esperado**: tabela com `1 spark / 2 iceberg`.

### Docker

```bash
docker exec -it spark /opt/spark/bin/spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=warehouse \
  -e "CREATE TABLE IF NOT EXISTS local.db.teste (id INT, nome STRING) USING iceberg; INSERT INTO local.db.teste VALUES (1, 'spark'), (2, 'iceberg'); SELECT * FROM local.db.teste ORDER BY id; DROP TABLE local.db.teste PURGE;"
```

**Resultado esperado** (final da saida): `1  spark` / `2  iceberg`.
