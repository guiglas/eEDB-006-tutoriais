# Acesso SSH ao Cluster EMR

> Guia completo para acessar o master node do cluster via SSH.

---

## Onde esta a chave

A chave privada `labsuser.pem` (key pair `vockey`) esta em:

```
tutoriais/aws_credenciais/labsuser.pem
```

O arquivo ja foi baixado durante a configuracao inicial das credenciais AWS.

---

## 1. Dentro do Learner Lab (terminal embutido)

O Learner Lab fornece um terminal no proprio navegador (painel esquerdo da interface do lab ou AWS CloudShell). Esse terminal ja esta na mesma rede do cluster EMR e possui a chave pre-instalada.

### Passo a passo

1. No painel do Learner Lab (a esquerda), clique em **"AWS Details"**
2. Clique em **"Download PEM"** (macOS/Linux) ou **"Download PPK"** (Windows)
3. Salve o arquivo em um local seguro (`~/.ssh/` ou `tutoriais/aws_credenciais/`)
4. No terminal do lab, obtenha o DNS do master node:

```bash
cd ~/Documents/Big\ Data/tutoriais/aws-emr-hive-tutorial/terraform
CLUSTER_ID=$(terraform output -raw cluster_id)
MASTER_DNS=$(aws emr describe-cluster --cluster-id $CLUSTER_ID \
    --query 'Cluster.MasterPublicDnsName' --output text)
echo "Master DNS: $MASTER_DNS"
```

5. Conecte via SSH:

```bash
ssh -i ~/.ssh/labsuser.pem -o StrictHostKeyChecking=no hadoop@$MASTER_DNS
```

> **Alternativa**: Se o terminal do lab ja tiver a chave em `~/.ssh/labsuser.pem`, o comando acima funciona diretamente.

### Exploracao dentro do master

Apos conectar, explore o ambiente:

```bash
# Listar tabelas Hive
hive -e 'SHOW TABLES;'

# Ver schema de uma tabela
hive -e 'DESCRIBE clientes_s3;'

# Ver resultados do workflow S3
hive -e 'SELECT * FROM resultado_vendas_s3;'

# Ver resultados do workflow HDFS
hive -e 'SELECT * FROM resultado_vendas_hdfs;'

# Arquivos no HDFS
hdfs dfs -ls /data/hdfs/
hdfs dfs -ls /data/hdfs/output/
hdfs dfs -cat /data/hdfs/output/000000_0

# Queries extras
hive -e "
  SELECT c.nome, SUM(v.quantidade * p.preco) AS total_gasto
  FROM vendas_s3 v
  JOIN clientes_s3 c ON v.id_cliente = c.id_cliente
  JOIN produtos_s3 p ON v.id_produto = p.id_produto
  GROUP BY c.nome
  ORDER BY total_gasto DESC
  LIMIT 5;
"

# Sair
exit
```

---

## 2. Fora do Learner Lab (conta AWS propria)

Em uma conta AWS propria, o SSH funciona normalmente. O cluster ja tem a key pair `vockey` associada no `main.tf`.

### Liberar o security group

O EMR cria um security group publico que permite SSH. Verifique e libere para seu IP:

```bash
# Descobrir o security group do master
SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=ElasticMapReduce-Master-Public*" \
    --query 'SecurityGroups[0].GroupId' --output text)
echo "Security Group: $SG_ID"

# Adicionar regra SSH do seu IP atual
aws ec2 authorize-security-group-ingress \
    --group-id $SG_ID \
    --protocol tcp --port 22 \
    --cidr $(curl -s ifconfig.me)/32

# Verificar regra
aws ec2 describe-security-groups --group-ids $SG_ID \
    --query 'SecurityGroups[0].IpPermissions[?FromPort==`22`]'
```

### Conectar

Dentro do repositório eEDB-006-tutoriais

```bash
MASTER_DNS=$(terraform output -raw master_dns)
ssh -i $(pwd)/tutoriais/aws_credenciais/labsuser.pem \
    -o StrictHostKeyChecking=no hadoop@$MASTER_DNS
```

---

## 3. Troubleshooting SSH

| Problema | Causa | Solucao |
|----------|-------|---------|
| `Operation timed out` | Security group nao permite seu IP | Libere o SG com o comando da secao 2 |
| `Permission denied (publickey)` | Chave errada ou nao associada | Verifique `key_name` no `main.tf` |
| `Connection refused` | Master node nao esta pronto | Cluster pode estar STARTING ainda |
| `Connection closed` | Master em estado TERMINATED | Cluster foi destruido — recrie |
| `ssh: Could not resolve hostname` | Master DNS expirou | Re-consulte com `aws emr describe-cluster` |
| `Warning: Permanently added` | Primeira conexao | Normal, pode ignorar |
| `labsuser.pem: bad permissions` | PEM com permissao 644 | `chmod 400 labsuser.pem` |

---

## 4. Key pair vockey vs labsuser.pem

| Nome AWS | Arquivo local | Uso |
|----------|---------------|-----|
| `vockey` | `labsuser.pem` | Key pair criado no Learner Lab para us-east-1 |
| `EMR_EC2_DefaultRole` | — | Role IAM que o EMR usa para acessar S3 e outros servicos |

O nome do key pair na AWS e `vockey`. O arquivo baixado e `labsuser.pem`. O Terraform usa `key_name = "vockey"` no `main.tf` para associar a chave ao cluster. O SSH usa `-i labsuser.pem` com o usuario `hadoop` (usuario padrao do EMR).

> **Nota**: Se estiver em uma regiao diferente de us-east-1, crie um novo key pair e atualize `key_name` no `main.tf`.
