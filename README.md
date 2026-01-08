# Teste Técnico - Engenheiro de Dados PySpark

## Visão Geral

Solução em PySpark para análise de dados de pedidos e clientes de um e-commerce, incluindo validação de qualidade, agregações, análises estatísticas e filtragens.

## Estrutura

```
├── main.py              # Script principal
├── data/
│   ├── clientes.json        # ~10k registros
│   └── pedidos.json         # ~1M registros
└── requirements.txt         # pyspark==3.4.1
```

## Execução

**Instalar dependências:**
```bash
pip install -r requirements.txt
```

**Rodar o script:**
```bash
python main.py
```

## Requisitos Implementados

1. **Data Quality** - Identifica pedidos inválidos (client_id nulo, value nulo, valor negativo, órfão)
2. **Agregação** - Quantidade e valor total por cliente, ordenado por valor
3. **Análise Estatística** - Média, mediana, percentil 10 e 90
4. **Filtro Acima da Média** - Clientes com valor acima da média
5. **Filtro P10-P90** - Clientes sem outliers (entre percentis 10 e 90)

## Notas

- Usa `F.broadcast()` para otimização de join
- Schemas tipados com StructType
- Tratamento explícito de valores nulos
- Precision decimal 11.2 mantida

## Decisões Técnicas

- **Join otimizado**: uso de `broadcast` no dataframe de clientes (~10k registros) para evitar shuffle em join com pedidos (~1M).
- **Schemas explícitos**: definição manual com `StructType` para evitar inferência incorreta e garantir tipos numéricos.
- **Qualidade de dados**: separação explícita entre pedidos válidos e inválidos, permitindo análises consistentes.
- **Estatísticas**: uso de `percentile_approx` por eficiência em grandes volumes de dados.
- **Precisão numérica**: valores financeiros agregados e convertidos para `Decimal(11,2)` para evitar perda de precisão.
- **Persistência**: `cache()` aplicado apenas no dataframe reutilizado, evitando consumo desnecessário de memória.
