# Teste Técnico - Engenheiro de Dados PySpark

## Contexto
Você foi contratado para analisar dados de pedidos e clientes de um e-commerce. Os dados estão armazenados em formato JSON e você precisa realizar análises para entender o comportamento de compra dos clientes.

## Dados Disponíveis

### Clientes (`clients`)
- **Volume**: ~10 mil de registros
- **Localização**: `clients.json`
- **Schema**:
  - `id` (long): Identificador único do cliente
  - `name` (string): Nome do cliente

### Pedidos (`pedidos`)
- **Volume**: ~1 milhão de registros
- **Localização**: `pedidos.json`
- **Schema**:
  - `id` (long): Identificador único do pedido
  - `client_id` (long): Identificador do cliente (FK para clients.id)
  - `value` (decimal(5,2)): Valor do pedido

## Requisitos Técnicos

### 1. Data Quality - Relatório de Falhas
Identifique e reporte pedidos com problemas de qualidade:
  - id e motivo

### 2. Agregação de Dados
Crie uma análise que mostre para cada cliente:
- Nome do cliente
- Quantidade de pedidos realizados
- Valor total de pedidos (formatado como decimal 11,2)
- Ordene por valor total decrescente

### 3. Análise Estatística
Calcule as seguintes métricas sobre o valor total por cliente:
- Média aritmética
- Mediana
- Percentil 10 (10% inferiores)
- Percentil 90 (10% superiores)

### 4: Filtragem - Acima da Média
Liste todos os clientes cujo valor total de pedidos está acima da média aritmética, ordenado por valor.

### 5: Filtragem - Média Truncada
Liste todos os clientes cujo valor total está entre o percentil 10 e 90 (removendo outliers das extremidades), ordenado por valor.

## Critérios de Avaliação

1. **Performance** (25%): Uso correto do spark para melhor balacemanto de dados
2. **Correção** (25%): Resultados precisos e lógica correta
3. **Coerência entre entrega e conhecimento** (50%): Bate papo sobre a solução

## Entrega
- Código PySpark funcional
- Cada um dos 7 pontos de requisitos vão ser basicamente um dataframe
- Não precisa persistir dataframes, use apenas o show()

**Boa sorte!**