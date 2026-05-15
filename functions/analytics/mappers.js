const {
  round,
  formatCurrency,
  formatPercent,
} = require("./utils");

function toRankingDoc(item) {
  return {
    empresa_id: item.empresaId || null,
    produto_id: item.produtoId,
    produto_nome: item.produtoNome,
    categoria: item.categoria,
    quantidade_vendida: round(item.quantidadeVendida),
    quantidade_vendida_7_dias: round(item.quantidadeVendida7Dias),
    total_vendido: round(item.totalVendido),
    total_vendido_7_dias: round(item.totalVendido7Dias),
    total_vendido_7_dias_formatado: formatCurrency(item.totalVendido7Dias),
    total_vendido_formatado: formatCurrency(item.totalVendido),
    percentual_vendas: round(item.percentualVendas),
    percentual_vendas_formatado: formatPercent(item.percentualVendas),
    percentual_acumulado: round(item.percentualAcumulado),
    percentual_acumulado_formatado: formatPercent(item.percentualAcumulado),
    disponibilidade_prateleira: round(item.disponibilidadePrateleira),
    disponibilidade_prateleira_formatada: formatPercent(item.disponibilidadePrateleira),
    venda_perdida_estimada: round(item.vendaPerdidaEstimada),
    venda_perdida_estimada_formatada: formatCurrency(item.vendaPerdidaEstimada),
    status_niq: item.statusNiq,
    acao_recomendada: item.acaoRecomendada,
    ranking: item.ranking,
    classe: item.abcClass,
  };
}

function toCurvaAbcDoc(item) {
  return {
    empresa_id: item.empresaId || null,
    produto_id: item.produtoId,
    produto_nome: item.produtoNome,
    categoria: item.categoria,
    total_vendido: round(item.totalVendido),
    total_vendido_formatado: formatCurrency(item.totalVendido),
    percentual_vendas: round(item.percentualVendas),
    percentual_vendas_formatado: formatPercent(item.percentualVendas),
    percentual_acumulado: round(item.percentualAcumulado),
    percentual_acumulado_formatado: formatPercent(item.percentualAcumulado),
    disponibilidade_prateleira: round(item.disponibilidadePrateleira),
    disponibilidade_prateleira_formatada: formatPercent(item.disponibilidadePrateleira),
    venda_perdida_estimada: round(item.vendaPerdidaEstimada),
    venda_perdida_estimada_formatada: formatCurrency(item.vendaPerdidaEstimada),
    status_niq: item.statusNiq,
    acao_recomendada: item.acaoRecomendada,
    classe: item.abcClass,
  };
}

function toRupturaDoc(item) {
  return {
    empresa_id: item.empresaId || null,
    produto_id: item.produtoId,
    produto_nome: item.produtoNome,
    categoria: item.categoria,
    estoque_atual: round(item.estoqueAtual),
    media_venda_dia: round(item.mediaVendaDia),
    dias_cobertura: item.diasCobertura === null ? null : round(item.diasCobertura),
    disponibilidade_prateleira: round(item.disponibilidadePrateleira),
    disponibilidade_prateleira_formatada: formatPercent(item.disponibilidadePrateleira),
    taxa_ruptura_sku: round(item.taxaRupturaSku),
    taxa_ruptura_sku_formatada: formatPercent(item.taxaRupturaSku),
    venda_perdida_estimada: round(item.vendaPerdidaEstimada),
    venda_perdida_estimada_formatada: formatCurrency(item.vendaPerdidaEstimada),
    status_niq: item.statusNiq,
    acao_recomendada: item.acaoRecomendada,
    risco: item.risco,
    prioridade: item.prioridade,
  };
}

function toSugestaoCompraDoc(item) {
  return {
    empresa_id: item.empresaId || null,
    produto_id: item.produtoId,
    produto_nome: item.produtoNome,
    categoria: item.categoria,
    estoque_atual: round(item.estoqueAtual),
    estoque_desejado: round(item.estoqueDesejado),
    media_venda_dia: round(item.mediaVendaDia),
    dias_cobertura_alvo: round(item.diasCoberturaAlvo),
    dias_seguranca: round(item.diasSeguranca),
    dias_cobertura: item.diasCobertura === null ? null : round(item.diasCobertura),
    quantidade_vendida: round(item.quantidadeVendida),
    quantidade_sugerida: round(item.quantidadeSugerida),
    investimento_sugerido: round(item.investimentoSugerido),
    investimento_sugerido_formatado: formatCurrency(item.investimentoSugerido),
    venda_perdida_estimada: round(item.vendaPerdidaEstimada),
    venda_perdida_estimada_formatada: formatCurrency(item.vendaPerdidaEstimada),
    disponibilidade_prateleira: round(item.disponibilidadePrateleira),
    disponibilidade_prateleira_formatada: formatPercent(item.disponibilidadePrateleira),
    status_niq: item.statusNiq,
    acao_recomendada: item.acaoRecomendada,
    prioridade: item.prioridade,
  };
}

function toProdutoMortoDoc(item) {
  return {
    empresa_id: item.empresaId || null,
    produto_id: item.produtoId,
    produto_nome: item.produtoNome,
    categoria: item.categoria,
    estoque_atual: round(item.estoqueAtual),
    quantidade_vendida: round(item.quantidadeVendida),
    total_vendido: round(item.totalVendido),
    total_vendido_formatado: formatCurrency(item.totalVendido),
    disponibilidade_prateleira: round(item.disponibilidadePrateleira),
    disponibilidade_prateleira_formatada: formatPercent(item.disponibilidadePrateleira),
    status_niq: item.statusNiq,
    acao_recomendada: item.acaoRecomendada,
    prioridade: item.estoqueAtual > 20 ? "alta" : "media",
  };
}

module.exports = {
  toRankingDoc,
  toCurvaAbcDoc,
  toRupturaDoc,
  toSugestaoCompraDoc,
  toProdutoMortoDoc,
};
