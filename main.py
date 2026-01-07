# ### 1. Data Quality - Relatório de Falhas
# Identifique e reporte pedidos com problemas de qualidade:
#   - id e motivo

# ### 2. Agregação de Dados
# Crie uma análise que mostre para cada cliente:
# - Nome do cliente
# - Quantidade de pedidos realizados
# - Valor total de pedidos (formatado como decimal 11,2)
# - Ordene por valor total decrescente

# ### 3. Análise Estatística
# Calcule as seguintes métricas sobre o valor total por cliente:
# - Média aritmética
# - Mediana
# - Percentil 10 (10% inferiores)
# - Percentil 90 (10% superiores)

# ### 4: Filtragem - Acima da Média
# Liste todos os clientes cujo valor total de pedidos está acima da média aritmética, ordenado por valor.

# ### 5: Filtragem - Média Truncada
# Liste todos os clientes cujo valor total está entre o percentil 10 e 90 (removendo outliers das extremidades), ordenado por valor.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

spark = SparkSession.builder.appName("TesteEcommerce").getOrCreate()

df_clients = spark.read.json("data/clients.json")
df_orders = spark.read.json("data/pedidos.json")