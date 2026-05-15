const {
  WINDOW_DAYS,
  RECENT_WINDOW_DAYS,
  DEAD_STOCK_MIN_UNITS,
  NIQ_AVAILABILITY_TARGET,
  STOCK_QUANTITY_KEYS,
  SALES_QUANTITY_KEYS,
  SALES_TOTAL_KEYS,
  UNIT_PRICE_KEYS,
} = require("./constants");
const {
  firstNumber,
  isWithinWindow,
  isWithinWindowStrict,
  safeDocId,
  sum,
  average,
  round,
  formatCurrency,
  formatPercent,
} = require("./utils");
const {
  createEmptyMetrics,
  getOrCreateMetrics,
  mergeProductIdentity,
  applyCalculations,
  buildAlerts,
  normalizeId,
} = require("./metrics");
const {
  toRankingDoc,
  toCurvaAbcDoc,
  toRupturaDoc,
  toSugestaoCompraDoc,
  toProdutoMortoDoc,
} = require("./mappers");
const {
  admin,
  getDb,
  getTenantCollection,
  replaceCollection,
} = require("../repositories/firebaseRepository");

async function rebuildAnalytics(empresaId = null) {
  const db = getDb();
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - WINDOW_DAYS);
  const recentCutoff = new Date();
  recentCutoff.setDate(recentCutoff.getDate() - RECENT_WINDOW_DAYS);

  const [productsSnap, stockSnap, salesSnap] = await Promise.all([
    getTenantCollection("produtos", empresaId),
    getTenantCollection("estoque", empresaId),
    getTenantCollection("vendas", empresaId),
  ]);

  const metricsByProduct = new Map();

  for (const doc of productsSnap.docs) {
    const data = doc.data();
    const produtoId = normalizeId(data, doc.id);
    metricsByProduct.set(produtoId, createEmptyMetrics(produtoId, data));
  }

  for (const doc of stockSnap.docs) {
    const data = doc.data();
    const produtoId = normalizeId(data, doc.id);
    const metrics = getOrCreateMetrics(metricsByProduct, produtoId, data);
    metrics.estoqueAtual = firstNumber(data, STOCK_QUANTITY_KEYS, metrics.estoqueAtual);
    metrics.estoqueDesejado = firstNumber(data, ["estoque_desejado"], metrics.estoqueDesejado);
    metrics.diasCoberturaAlvo = firstNumber(data, ["dias_cobertura_alvo"], metrics.diasCoberturaAlvo);
    metrics.diasSeguranca = firstNumber(data, ["dias_seguranca"], metrics.diasSeguranca);
    mergeProductIdentity(metrics, data);
  }

  let vendasProcessadas = 0;

  for (const doc of salesSnap.docs) {
    const data = doc.data();
    if (!isWithinWindow(data, cutoff)) {
      continue;
    }

    vendasProcessadas += 1;
    const produtoId = normalizeId(data, doc.id);
    const metrics = getOrCreateMetrics(metricsByProduct, produtoId, data);
    const quantidadeVendida = firstNumber(data, SALES_QUANTITY_KEYS, 0);
    const totalVendido = firstNumber(data, SALES_TOTAL_KEYS, null);
    const precoUnitario = firstNumber(data, UNIT_PRICE_KEYS, 0);
    const valorVenda = totalVendido !== null ? totalVendido : quantidadeVendida * precoUnitario;

    metrics.quantidadeVendida += quantidadeVendida;
    metrics.totalVendido += valorVenda;

    if (isWithinWindowStrict(data, recentCutoff)) {
      metrics.quantidadeVendida7Dias += quantidadeVendida;
      metrics.totalVendido7Dias += valorVenda;
    }

    mergeProductIdentity(metrics, data);
  }

  const metricsList = Array.from(metricsByProduct.values());
  applyCalculations(metricsList);

  const sugestoes = metricsList.filter((item) => item.quantidadeSugerida > 0);
  const alertas = buildAlerts(metricsList);
  const produtosMortos = metricsList.filter(
    (item) => item.estoqueAtual >= DEAD_STOCK_MIN_UNITS && item.quantidadeVendida <= 0,
  );

  await Promise.all([
    replaceCollection("ranking_vendas", metricsList, toRankingDoc, empresaId),
    replaceCollection("curva_abc", metricsList, toCurvaAbcDoc, empresaId),
    replaceCollection("previsao_ruptura", metricsList, toRupturaDoc, empresaId),
    replaceCollection("sugestoes_compra", sugestoes, toSugestaoCompraDoc, empresaId),
    replaceCollection("produtos_mortos", produtosMortos, toProdutoMortoDoc, empresaId),
    replaceCollection("alertas", alertas, (alerta) => alerta, empresaId),
  ]);

  const totalVendas = sum(metricsList, (item) => item.totalVendido);
  const totalVendas7Dias = sum(metricsList, (item) => item.totalVendido7Dias);
  const investimentoSugerido = sum(sugestoes, (item) => item.investimentoSugerido);
  const vendaPerdidaEstimada = sum(metricsList, (item) => item.vendaPerdidaEstimada);
  const giroMedio = average(metricsList, (item) => item.mediaVendaDia);
  const coberturaMediaDias = average(
    metricsList.filter((item) => item.diasCobertura !== null),
    (item) => item.diasCobertura,
  );
  const coberturaMediaItensComVenda = average(
    metricsList.filter((item) => item.quantidadeVendida > 0 && item.diasCobertura !== null),
    (item) => item.diasCobertura,
  );
  const produtosAtivos = metricsList.filter((item) => item.quantidadeVendida > 0).length;
  const produtosDisponiveis = metricsList.filter(
    (item) => item.quantidadeVendida > 0 && item.estoqueAtual > 0,
  ).length;
  const produtosRuptura = metricsList.filter(
    (item) => item.quantidadeVendida > 0 && item.estoqueAtual <= 0,
  ).length;
  const riscoAlto = metricsList.filter((item) => item.risco === "alto").length;
  const riscoMedio = metricsList.filter((item) => item.risco === "medio").length;
  const riscoBaixo = metricsList.filter((item) => item.risco === "baixo").length;
  const itensEmRisco = riscoAlto + riscoMedio;
  const excessoEstoque = metricsList.filter((item) => item.statusNiq === "excesso_estoque").length;
  const disponibilidadePrateleira = produtosAtivos > 0 ? (produtosDisponiveis / produtosAtivos) * 100 : 100;
  const taxaRuptura = produtosAtivos > 0 ? (produtosRuptura / produtosAtivos) * 100 : 0;
  const oportunidadeReceitaPercentual = (totalVendas + vendaPerdidaEstimada) > 0 ?
    (vendaPerdidaEstimada / (totalVendas + vendaPerdidaEstimada)) * 100 :
    0;
  const itensCriticos = riscoAlto;

  const dashboardDocId = empresaId ? `${safeDocId(empresaId)}_dashboard` : "dashboard";
  const rupturaResumoDocId = empresaId ? `${safeDocId(empresaId)}_resumo` : "_resumo";
  const resumoRuptura = {
    tipo: "resumo",
    empresa_id: empresaId || null,
    total_itens: metricsList.length,
    cobertura_media_dias: round(coberturaMediaDias),
    media_cobertura: round(coberturaMediaDias),
    cobertura_media_itens_com_venda: round(coberturaMediaItensComVenda),
    media_cobertura_itens_com_venda: round(coberturaMediaItensComVenda),
    itens_em_risco: itensEmRisco,
    risco_alto: riscoAlto,
    risco_medio: riscoMedio,
    risco_baixo: riscoBaixo,
    produtos_em_ruptura: produtosRuptura,
    excesso_estoque: excessoEstoque,
    disponibilidade_prateleira: round(disponibilidadePrateleira),
    disponibilidade_prateleira_formatada: formatPercent(disponibilidadePrateleira),
    taxa_ruptura: round(taxaRuptura),
    taxa_ruptura_formatada: formatPercent(taxaRuptura),
    venda_perdida_estimada: round(vendaPerdidaEstimada),
    venda_perdida_estimada_formatada: formatCurrency(vendaPerdidaEstimada),
    atualizado_em: admin.firestore.FieldValue.serverTimestamp(),
  };

  await db
    .collection("previsao_ruptura")
    .doc(rupturaResumoDocId)
    .set(resumoRuptura, {merge: true});

  await db.doc(`insights/${dashboardDocId}`).set(
    {
      empresa_id: empresaId || null,
      metodologia_indicadores: "NIQ OSA: disponibilidade, risco de ruptura, impacto em vendas e acao por SKU",
      meta_disponibilidade_prateleira: NIQ_AVAILABILITY_TARGET,
      meta_disponibilidade_prateleira_formatada: formatPercent(NIQ_AVAILABILITY_TARGET),
      disponibilidade_prateleira: round(disponibilidadePrateleira),
      disponibilidade_prateleira_formatada: formatPercent(disponibilidadePrateleira),
      taxa_ruptura: round(taxaRuptura),
      taxa_ruptura_formatada: formatPercent(taxaRuptura),
      cobertura_media_dias: round(coberturaMediaDias),
      cobertura_media_itens_com_venda: round(coberturaMediaItensComVenda),
      giro_medio: round(giroMedio),
      itens_criticos: itensCriticos,
      itens_em_risco: itensEmRisco,
      risco_alto: riscoAlto,
      risco_medio: riscoMedio,
      risco_baixo: riscoBaixo,
      produtos_em_ruptura: produtosRuptura,
      excesso_estoque: excessoEstoque,
      alertas_pendentes: alertas.length,
      venda_perdida_estimada: round(vendaPerdidaEstimada),
      venda_perdida_estimada_formatada: formatCurrency(vendaPerdidaEstimada),
      oportunidade_receita_percentual: round(oportunidadeReceitaPercentual),
      oportunidade_receita_percentual_formatada: formatPercent(oportunidadeReceitaPercentual),
      investimento_sugerido: round(investimentoSugerido),
      investimento_sugerido_formatado: formatCurrency(investimentoSugerido),
      total_vendas: round(totalVendas),
      total_vendas_formatado: formatCurrency(totalVendas),
      total_vendas_7_dias: round(totalVendas7Dias),
      total_vendas_7_dias_formatado: formatCurrency(totalVendas7Dias),
      produtos_processados: metricsList.length,
      vendas_processadas: vendasProcessadas,
      atualizado_em: admin.firestore.FieldValue.serverTimestamp(),
    },
    {merge: true},
  );

  return {
    empresaId: empresaId || null,
    produtosProcessados: metricsList.length,
    vendasProcessadas,
    sugestoesCompra: sugestoes.length,
    alertas: alertas.length,
    produtosMortos: produtosMortos.length,
    rankingVendas: metricsList.length,
    totalVendas: round(totalVendas),
    totalVendas7Dias: round(totalVendas7Dias),
    vendaPerdidaEstimada: round(vendaPerdidaEstimada),
    disponibilidadePrateleira: round(disponibilidadePrateleira),
    taxaRuptura: round(taxaRuptura),
    coberturaMediaDias: round(coberturaMediaDias),
    coberturaMediaItensComVenda: round(coberturaMediaItensComVenda),
    itensEmRisco,
    riscoAlto,
    riscoMedio,
    riscoBaixo,
    produtosRuptura,
    excessoEstoque,
    investimentoSugerido: round(investimentoSugerido),
  };
}

module.exports = {
  rebuildAnalytics,
};
