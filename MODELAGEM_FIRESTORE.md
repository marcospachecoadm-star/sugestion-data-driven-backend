# Modelagem Firestore - Estoqueia

Analytics calculado em janela movel de 45 dias, seguindo a logica NielsenIQ OSA:
disponibilidade, giro, cobertura, ruptura, estoque parado e acao recomendada por SKU.

## SaaS e controle de custo

Toda execucao analitica deve ser escopada por empresa. O backend recusa chamadas sem `empresaId`, exceto se `ALLOW_GLOBAL_JOBS=true` for configurado manualmente.

Chamadas recomendadas:

```txt
/run-analytics?empresaId=superparanaloja1
/import-storage-csv?empresaId=superparanaloja1
/import-and-run?empresaId=superparanaloja1
```

Headers:

```txt
x-api-key: chave-do-backend
x-empresa-id: superparanaloja1
```

Guardrails ativos:

- `REQUIRE_API_KEY=true`
- `ALLOW_GLOBAL_JOBS=false`
- `MAX_UPLOAD_FILES_PER_RUN=10`
- `MAX_CSV_ROWS_PER_FILE=5000`
- `MAX_PRODUCTS_PER_ANALYTICS=5000`
- `MAX_STOCK_ROWS_PER_ANALYTICS=5000`
- `MAX_SALES_ROWS_PER_ANALYTICS=20000`
- `MAX_ITEMS_PER_INDICATOR=250`
- `OUTLIER_STD_DEV_FACTOR=3`
- `OUTLIER_MEDIAN_FACTOR=3`
- `TREND_MIN_FACTOR=0.75`
- `TREND_MAX_FACTOR=1.4`
- `TREND_CONFIRMATION_THRESHOLD=1.15`

## Tratamento Nielsen de outlier e sazonalidade

O backend nao usa a venda bruta de 45 dias diretamente para sugerir compra. Ele calcula uma demanda ajustada:

```txt
media_diaria_bruta_45d = vendas_45d / 45
media_diaria_ajustada_45d = media de 45 dias com picos suavizados
fator_tendencia = ajuste confirmado por 15d e 7d
giro_diario_calculado = media_diaria_ajustada_45d * fator_tendencia
```

Regras:

- pico diario acima de media + 3 desvios ou 3x mediana positiva vira outlier;
- outlier isolado e nao confirmado por 15 dias marca `sazonalidade_detectada = true`;
- 7 dias sozinho nao infla compra agressivamente;
- tendencia alta precisa ser confirmada tambem nos 15 dias;
- sugestao de compra usa `giro_diario_calculado`, nao a media bruta.

Campos gerados nos itens:

- `vendas_45d`
- `vendas_15d`
- `vendas_7d`
- `media_diaria_bruta_45d`
- `media_diaria_ajustada_45d`
- `media_diaria_15d`
- `media_diaria_7d`
- `fator_tendencia`
- `giro_diario_calculado`
- `outlier_detectado`
- `dias_outlier`
- `sazonalidade_detectada`

Para o FlutterFlow/Firebase funcionar como SaaS, cada usuario autenticado deve receber custom claim:

```json
{
  "empresa_id": "superparanaloja1"
}
```

Usuarios administradores internos podem receber:

```json
{
  "admin": true
}
```

O backend Render usa Firebase Admin SDK e ignora regras de cliente, mas o app FlutterFlow respeita `firestore.rules` e `storage.rules`.

## Colecoes de entrada

### produtos

Fonte cadastral dos produtos importados por CSV.

Campos aceitos pelo backend:

- `empresa_id`
- `id`, `produto_id`, `sku`, `codigo` ou `cod_produto`
- `nome`, `produto_nome` ou `descricao`
- `categoria` ou `departamento`
- `fornecedor` ou `distribuidor`
- `marca`
- `custo_unitario`, `preco_compra`, `preco_custo`, `custo` ou `preco`
- `estoque_minimo`

### estoque

Estoque atual importado por CSV.

Campos aceitos pelo backend:

- `empresa_id`
- `produto_id`, `id`, `sku`, `codigo` ou `cod_produto`
- `estoque_atual`, `quantidade_estoque`, `quantidade`, `qtd`, `qtde`, `saldo` ou `estoque`
- `estoque_minimo`, `minimo` ou `min`
- `estoque_desejado`
- `dias_cobertura_alvo`
- `dias_seguranca`

### vendas

Historico de vendas importado por CSV. O analytics considera apenas os ultimos 45 dias quando a venda tem data.

Campos aceitos pelo backend:

- `empresa_id`
- `venda_id` ou `id`
- `produto_id`, `id`, `sku`, `codigo` ou `cod_produto`
- `data`, `data_venda`, `criado_em` ou `created_at`
- `quantidade_vendida`, `quantidade`, `qtd`, `qtde` ou `qty`
- `total_vendido`, `valor_total`, `total`, `receita`, `valor`, `valor_venda` ou equivalentes
- `preco_unitario`, `valor_unitario`, `preco` ou equivalentes

## Colecoes geradas pelo analytics

### indicadoresResumo

Um documento por empresa:

- `{empresaId}_dashboard`
- `dashboard` quando roda sem `empresaId`

Use esta colecao na tela inicial para os cards e o resumo geral.

Campos principais:

- `periodo_dias`
- `metodologia_indicadores`
- `total_vendas`
- `total_vendas_formatado`
- `giro_medio`
- `giro_medio_formatado`
- `giro_medio_diario`
- `giro_medio_diario_formatado`
- `giro_medio_45d`
- `giro_medio_45d_formatado`
- `giro_medio_status`
- `giro_medio_status_label`
- `giro_medio_dias`
- `unidades_vendidas_45d`
- `unidades_vendidas_45d_formatado`
- `skus_ativos_45d`
- `skus_com_venda_45d`
- `percentual_skus_com_venda_45d`
- `percentual_skus_com_venda_45d_formatado`
- `itens_criticos`
- `itens_abaixo_minimo`
- `itens_ruptura`
- `alertas_pendentes`
- `itens_sem_vendas`
- `investimento_sugerido`
- `investimento_sugerido_formatado`
- `acoes_recomendadas`
- `sugestoes_compra`
- `reposicao_urgente`
- `atualizado_em`

### indicadoresItens

Colecao generica para abrir qualquer card em lista de produtos.

Query padrao no FlutterFlow:

```txt
indicadoresItens
where empresa_id == empresaAtual
where indicador_tipo == tipoDoCard
```

Tipos disponiveis:

- `giro_medio`
- `itens_criticos`
- `itens_sem_vendas`
- `sugestao_compra`
- `alertas`
- `acao_recomendada`

Campos principais:

- `indicador_tipo`
- `produto_id`
- `produto_nome`
- `sku`
- `categoria`
- `fornecedor`
- `prioridade`
- `status`
- `titulo`
- `descricao`
- `valor`
- `valor_formatado`
- `vendas_45d`
- `vendas_45d_formatado`
- `total_vendido_45d`
- `giro_diario`
- `giro_diario_formatado`
- `giro_45d`
- `giro_45d_formatado`
- `estoque_atual`
- `estoque_minimo`
- `cobertura_dias`
- `cobertura_dias_formatado`
- `quantidade_sugerida`
- `investimento_sugerido`
- `valor_parado`
- `status_giro`
- `status_giro_label`
- `status_estoque`
- `abc_classe`
- `ranking`

## Telas do FlutterFlow

### Dashboard

Fonte:

```txt
indicadoresResumo/{empresaId}_dashboard
```

Cards:

- Giro Medio: `giro_medio_formatado`
- Itens Criticos: `itens_criticos`
- Alertas: `alertas_pendentes`
- Sugestao Compra: `sugestoes_compra`

Resumo Geral:

- Total de Vendas: `total_vendas_formatado`
- Giro Medio: `giro_medio_formatado`
- Saude do Giro: `percentual_skus_com_venda_45d_formatado` e `giro_medio_status_label`
- Itens Criticos: `itens_criticos`
- Alertas Pendentes: `alertas_pendentes`
- Itens sem Vendas: `itens_sem_vendas`
- Investimento Sugerido: `investimento_sugerido_formatado`

### Giro Medio

Query:

```txt
indicadoresItens
where empresa_id == empresaAtual
where indicador_tipo == "giro_medio"
orderBy ranking asc
```

### Itens Criticos

Query:

```txt
indicadoresItens
where empresa_id == empresaAtual
where indicador_tipo == "itens_criticos"
orderBy prioridade desc
```

### Itens sem Vendas

Query:

```txt
indicadoresItens
where empresa_id == empresaAtual
where indicador_tipo == "itens_sem_vendas"
orderBy valor desc
```

### Acoes Recomendadas

Query:

```txt
acoesRecomendadas
where empresa_id == empresaAtual
where status == "pendente"
orderBy prioridade desc
```

### Sugestao de Compra

Query:

```txt
sugestoesCompra
where empresa_id == empresaAtual
where status == "pendente"
orderBy prioridade desc
```

## Observacao

Esta modelagem substitui a estrutura antiga. O backend novo gera apenas as colecoes operacionais de entrada e as colecoes analiticas acima, sempre isoladas por `empresa_id`.
