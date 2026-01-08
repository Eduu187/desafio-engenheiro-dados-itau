import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StructType, StructField, LongType, StringType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("TesteDados") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Schemas
schema_clientes = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True)
])

schema_pedidos = StructType([
    StructField("id", LongType(), True),
    StructField("client_id", LongType(), True),
    StructField("value", DecimalType(10, 2), True)
])

df_clientes = spark.read.json("data/clientes.json", schema=schema_clientes)
df_pedidos = spark.read.json("data/pedidos.json", schema=schema_pedidos)

### 1. Data Quality - Relatório de Falhas
# Identifique e reporte pedidos com problemas de qualidade:
#   - id e motivo
df_dq = df_pedidos.join(df_clientes, df_pedidos.client_id == df_clientes.id, "left")

df_pedidos_invalidos = df_dq.filter(
    (df_pedidos.id.isNull()) | 
    (df_pedidos.client_id.isNull()) | 
    (df_pedidos.value.isNull()) | 
    (df_pedidos.value < 0) |
    (df_clientes.id.isNull())
).withColumn(
    "motivo",
    F.when(df_pedidos.id.isNull(), "ID do pedido nulo")
     .when(df_pedidos.client_id.isNull(), "Client_id nulo")
     .when(df_pedidos.value.isNull(), "Valor do pedido nulo")
     .when(df_pedidos.value < 0, "Valor negativo")
     .when(df_clientes.id.isNull(), "Cliente não cadastrado (Órfão)")
     .otherwise("Erro desconhecido")
)

print("Relatório 1: Pedidos Inválidos")
df_pedidos_invalidos.select(df_pedidos.id, "motivo").show()

### 2. Agregação de Dados
# Crie uma análise que mostre para cada cliente:
# - Nome do cliente
# - Quantidade de pedidos realizados
# - Valor total de pedidos (formatado como decimal 11,2)
# - Ordene por valor total decrescente
df_pedidos_validos = df_pedidos.filter(
    F.col("id").isNotNull() & 
    F.col("client_id").isNotNull() & 
    (F.col("value") >= 0)
)

df_analise_clientes = df_pedidos_validos.join(
    F.broadcast(df_clientes), 
    df_pedidos_validos.client_id == df_clientes.id
).groupBy("name").agg(
    F.count(df_pedidos_validos.id).alias("qtd_pedidos"),
    F.sum("value").cast(DecimalType(11, 2)).alias("valor_total")
).orderBy(F.desc("valor_total"))

print("Relatório 2: Análise por Cliente")
df_analise_clientes.show()

### 3. Análise Estatística
# Calcule as seguintes métricas sobre o valor total por cliente:
# - Média aritmética
# - Mediana
# - Percentil 10 (10% inferiores)
# - Percentil 90 (10% superiores)
df_estatisticas = df_analise_clientes.agg(
    F.avg("valor_total").alias("media"),
    F.percentile_approx("valor_total", 0.5).alias("mediana"),
    F.percentile_approx("valor_total", 0.1).alias("p10"),
    F.percentile_approx("valor_total", 0.9).alias("p90")
)

print("Relatório 3: Métricas Estatísticas")
df_estatisticas.show()

stats = df_estatisticas.collect()[0]
media_val = stats["media"]
p10_val = stats["p10"]
p90_val = stats["p90"]

### 4: Filtragem - Acima da Média
# Liste todos os clientes cujo valor total de pedidos está acima da média aritmética, ordenado por valor.
print("Relatório 4: Clientes Acima da Média")
df_analise_clientes.filter(F.col("valor_total") > media_val) \
    .orderBy("valor_total") \
    .show()

### 5: Filtragem - Média Truncada
# Liste todos os clientes cujo valor total está entre o percentil 10 e 90 (removendo outliers das extremidades), ordenado por valor.

print("Relatório 5: Clientes entre P10 e P90 (Sem outliers)")
df_analise_clientes.filter(
    (F.col("valor_total") >= p10_val) & 
    (F.col("valor_total") <= p90_val)
).orderBy("valor_total").show()

spark.stop()