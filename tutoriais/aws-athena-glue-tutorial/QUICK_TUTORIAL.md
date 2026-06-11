# Quick Tutorial: AWS Athena com Glue Data Catalog

> Versao rapida para usuarios ja familiarizados com os conceitos. Para explicacoes detalhadas, consulte TUTORIAL.md completo.

---

## Setup Rapido

### 1. Criar infraestrutura S3

```bash
cd ~/Documents/Big\ Data/tutoriais/aws-athena-glue-tutorial/terraform
terraform init
terraform apply -auto-approve
```

### 2. Criar Database e Tabelas no Glue

```bash
BUCKET=$(terraform output -raw s3_bucket)

# Database
aws glue create-database \
    --database-input '{"Name": "athena_lab"}' \
    --region us-east-1

# Clientes
aws glue create-table \
    --database-name athena_lab \
    --table-input '{
        "Name": "clientes",
        "StorageDescriptor": {
            "Location": "s3://'"${BUCKET}"'/data/clientes/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "Parameters": {"skip.header.line.count": "1", "field.delim": ","}},
            "Columns": [{"Name": "id_cliente", "Type": "int"}, {"Name": "nome", "Type": "string"}, {"Name": "email", "Type": "string"}, {"Name": "cidade", "Type": "string"}, {"Name": "estado", "Type": "string"}]
        }
    }' \
    --region us-east-1

# Produtos
aws glue create-table \
    --database-name athena_lab \
    --table-input '{
        "Name": "produtos",
        "StorageDescriptor": {
            "Location": "s3://'"${BUCKET}"'/data/produtos/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "Parameters": {"skip.header.line.count": "1", "field.delim": ","}},
            "Columns": [{"Name": "id_produto", "Type": "int"}, {"Name": "nome_produto", "Type": "string"}, {"Name": "categoria", "Type": "string"}, {"Name": "preco", "Type": "double"}]
        }
    }' \
    --region us-east-1

# Vendas
aws glue create-table \
    --database-name athena_lab \
    --table-input '{
        "Name": "vendas",
        "StorageDescriptor": {
            "Location": "s3://'"${BUCKET}"'/data/vendas/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "Parameters": {"skip.header.line.count": "1", "field.delim": ","}},
            "Columns": [{"Name": "id_venda", "Type": "int"}, {"Name": "id_cliente", "Type": "int"}, {"Name": "id_produto", "Type": "int"}, {"Name": "quantidade", "Type": "int"}, {"Name": "data_venda", "Type": "string"}]
        }
    }' \
    --region us-east-1

# Produtos
aws glue create-table \
    --database-name athena_lab \
    --table-input '{
        "Name": "produtos",
        "StorageDescriptor": {
            "Location": "s3://'"${BUCKET}"'/data/produtos/",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "Parameters": {"skip.header.line.count": "1", "field.delim": ","}},
            "Columns": [{"Name": "id_produto", "Type": "int"}, {"Name": "nome_produto", "Type": "string"}, {"Name": "categoria", "Type": "string"}, {"Name": "preco", "Type": "double"}]
        }
    }' \
    --region us-east-1

# Vendas
aws glue create-table \
    --database-name athena_lab \
    --table-input '{
        "Name": "vendas",
        "StorageDescriptor": {
            "Location": "s3://'"${BUCKET}"'/data/vendas/",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "Parameters": {"skip.header.line.count": "1", "field.delim": ","}},
            "Columns": [{"Name": "id_venda", "Type": "int"}, {"Name": "id_cliente", "Type": "int"}, {"Name": "id_produto", "Type": "int"}, {"Name": "quantidade", "Type": "int"}, {"Name": "data_venda", "Type": "string"}]
        }
    }' \
    --region us-east-1
```

### 3. Executar Query no Athena

```bash
BUCKET=$(terraform output -raw s3_bucket)

QUERY_ID=$(aws athena start-query-execution \
    --query-string "SELECT c.estado, p.categoria, round(SUM(v.quantidade * p.preco), 2) AS total_vendido, COUNT(*) AS num_vendas FROM athena_lab.vendas v JOIN athena_lab.clientes c ON v.id_cliente = c.id_cliente JOIN athena_lab.produtos p ON v.id_produto = p.id_produto GROUP BY c.estado, p.categoria ORDER BY total_vendido DESC" \
    --result-configuration "OutputLocation=s3://${BUCKET}/results/resultado_vendas/" \
    --work-group primary \
    --query 'QueryExecutionId' \
    --output text \
    --region us-east-1)

echo "Query ID: $QUERY_ID"

# Aguardar conclusao
for i in {1..30}; do
    STATE=$(aws athena get-query-execution --query-execution-id $QUERY_ID --query 'QueryExecution.Status.State' --output text --region us-east-1)
    echo "Estado: $STATE"
    [ "$STATE" == "SUCCEEDED" ] && break
    [ "$STATE" == "FAILED" ] && exit 1
    sleep 2
done
```

### 4. Ver Resultados

```bash
BUCKET=$(terraform output -raw s3_bucket)
aws s3 ls s3://${BUCKET}/results/resultado_vendas/
aws s3 cp s3://${BUCKET}/results/resultado_vendas/${QUERY_ID}.csv - | column -t -s','
```

### 5. Descomissionamento

```bash
cd ~/Documents/Big\ Data/tutoriais/aws-athena-glue-tutorial/terraform
terraform destroy -auto-approve
```

---

## Query Alternativas

```sql
-- Cliente que mais comprou
SELECT c.nome, SUM(v.quantidade * p.preco) AS total_gasto
FROM vendas v
JOIN clientes c ON v.id_cliente = c.id_cliente
JOIN produtos p ON v.id_produto = p.id_produto
GROUP BY c.nome ORDER BY total_gasto DESC LIMIT 5;

-- Produto mais vendido
SELECT p.nome_produto, SUM(v.quantidade) AS total_unidades
FROM vendas v JOIN produtos p ON v.id_produto = p.id_produto
GROUP BY p.nome_produto ORDER BY total_unidades DESC;

-- Vendas por estado
SELECT c.estado, COUNT(*) AS total_vendas
FROM vendas v JOIN clientes c ON v.id_cliente = c.id_cliente
GROUP BY c.estado ORDER BY total_vendas DESC;
```