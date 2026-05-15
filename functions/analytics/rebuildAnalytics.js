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
    (item) => {
      const semVenda = item.quantidadeVendida <= 0 && item.totalVendido <= 0;
      const coberturaExcessiva = item.mediaVendaDia > 0 &&
        item.diasCobertura !== null &&
        item.diasCobertura >= 90;

      return item.estoqueAtual >= DEAD_STOCK_MIN_UNITS &&
        (semVenda || coberturaExcessiva);
    },
  );

  await Promise.all([
    replaceCollection("ranking_vendas", metricsList, toRankingDoc, empresaId),
    replaceCollection("curva_abc", metricsList, toCurvaAbcDoc, empresaId),
    replaceCollection("previsao_ruptura", metricsList, toRupturaDoc, empresaId),
    replaceCollection("sugestoes_compra", sugestoes, toSugestaoCompraDoc, empresaId),
    replaceCollection("produtos_mortos", produtosMortos, toProdutoMortoDoc, empresaId),
    replaceCollection("mortos", produtosMortos, toProdutoMortoDoc, empresaId),
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
  const curvaClasseA = metricsList.filter((item) => item.abcClass === "A");
  const curvaClasseB = metricsList.filter((item) => item.abcClass === "B");
  const curvaClasseC = metricsList.filter((item) => item.abcClass === "C");
  const vendaClasseA = sum(curvaClasseA, (item) => item.totalVendido);
  const vendaClasseB = sum(curvaClasseB, (item) => item.totalVendido);
  const vendaClasseC = sum(curvaClasseC, (item) => item.totalVendido);
  const percentualItensClasseA = metricsList.length > 0 ? (curvaClasseA.length / metricsList.length) * 100 : 0;
  const percentualItensClasseB = metricsList.length > 0 ? (curvaClasseB.length / metricsList.length) * 100 : 0;
  const percentualItensClasseC = metricsList.length > 0 ? (curvaClasseC.length / metricsList.length) * 100 : 0;
  const percentualFaturamentoClasseA = totalVendas > 0 ? (vendaClasseA / totalVendas) * 100 : 0;
  const percentualFaturamentoClasseB = totalVendas > 0 ? (vendaClasseB / totalVendas) * 100 : 0;
  const percentualFaturamentoClasseC = totalVendas > 0 ? (vendaClasseC / totalVendas) * 100 : 0;

  const dashboardDocId = empresaId ? `${safeDocId(empresaId)}_dashboard` : "dashboard";
  const curvaResumoDocId = empresaId ? `${safeDocId(empresaId)}_resumo` : "_resumo";
  const rupturaResumoDocId = empresaId ? `${safeDocId(empresaId)}_resumo` : "_resumo";
  const resumoCurvaAbc = {
    tipo: "resumo",
    empresa_id: empresaId || null,
    metodologia_curva_abc: "Curva ABC por faturamento: Classe A ate 80% acumulado, Classe B ate 95%, Classe C acima de 95%.",
    total_itens: metricsList.length,
    total_vendas: round(totalVendas),
    total_vendas_formatado: formatCurrency(totalVendas),
    itens_classe_a: curvaClasseA.length,
    itens_classe_b: curvaClasseB.length,
    itens_classe_c: curvaClasseC.length,
    percentual_itens_classe_a: round(percentualItensClasseA),
    percentual_itens_classe_a_formatado: formatPercent(percentualItensClasseA),
    percentual_itens_classe_b: round(percentualItensClasseB),
    percentual_itens_classe_b_formatado: formatPercent(percentualItensClasseB),
    percentual_itens_classe_c: round(percentualItensClasseC),
    percentual_itens_classe_c_formatado: formatPercent(percentualItensClasseC),
    classe_a_itens_percentual: round(percentualItensClasseA),
    classe_a_itens_percentual_formatado: formatPercent(percentualItensClasseA),
    classe_a_faturamento: round(vendaClasseA),
    classe_a_faturamento_formatado: formatCurrency(vendaClasseA),
    classe_a_faturamento_percentual: round(percentualFaturamentoClasseA),
    classe_a_faturamento_percentual_formatado: formatPercent(percentualFaturamentoClasseA),
    classe_b_faturamento: round(vendaClasseB),
    classe_b_faturamento_formatado: formatCurrency(vendaClasseB),
    classe_b_faturamento_percentual: round(percentualFaturamentoClasseB),
    classe_b_faturamento_percentual_formatado: formatPercent(percentualFaturamentoClasseB),
    classe_c_faturamento: round(vendaClasseC),
    classe_c_faturamento_formatado: formatCurrency(vendaClasseC),
    classe_c_faturamento_percentual: round(percentualFaturamentoClasseC),
    classe_c_faturamento_percentual_formatado: formatPercent(percentualFaturamentoClasseC),
    atualizado_em: admin.firestore.FieldValue.serverTimestamp(),
  };
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
    .collection("curva_abc")
    .doc(curvaResumoDocId)
    .set(resumoCurvaAbc, {merge: true});

  await db
    .collection("previsao_ruptura")
    .doc(rupturaResumoDocId)
    .set(resumoRuptura, {merge: true});

  const mortosResumoDocId = empresaId ? `${safeDocId(empresaId)}_resumo` : "_resumo";
  const valorTotalParado = sum(produtosMortos, (item) => item.estoqueAtual * item.custoUnitario);
  const somaDiasParados = sum(produtosMortos, (item) => {
    if (item.diasCobertura !== null && item.diasCobertura >= 90) {
      return item.diasCobertura;
    }

    return 90;
  });
  const resumoMortos = {
    tipo: "resumo",
    empresa_id: empresaId || null,
    total_itens: produtosMortos.length,
    valor_total_parado: round(valorTotalParado),
    valor_total_parado_formatado: formatCurrency(valorTotalParado),
    media_dias_parados: produtosMortos.length > 0 ? round(somaDiasParados / produtosMortos.length) : 0,
    atualizado_em: admin.firestore.FieldValue.serverTimestamp(),
  };

  await Promise.all([
    db.collection("mortos").doc(mortosResumoDocId).set(resumoMortos, {merge: true}),
    db.collection("produtos_mortos").doc(mortosResumoDocId).set(resumoMortos, {merge: true}),
  ]);

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
      produtos_mortos_total_itens: resumoMortos.total_itens,
      produtos_mortos_valor_total_parado: resumoMortos.valor_total_parado,
      produtos_mortos_valor_total_parado_formatado: resumoMortos.valor_total_parado_formatado,
      produtos_mortos_media_dias_parados: resumoMortos.media_dias_parados,
      curva_abc_itens_classe_a: resumoCurvaAbc.itens_classe_a,
      curva_abc_itens_classe_b: resumoCurvaAbc.itens_classe_b,
      curva_abc_itens_classe_c: resumoCurvaAbc.itens_classe_c,
      curva_abc_percentual_itens_classe_a: resumoCurvaAbc.percentual_itens_classe_a,
      curva_abc_percentual_itens_classe_a_formatado: resumoCurvaAbc.percentual_itens_classe_a_formatado,
      curva_abc_faturamento_classe_a_percentual: resumoCurvaAbc.classe_a_faturamento_percentual,
      curva_abc_faturamento_classe_a_percentual_formatado: resumoCurvaAbc.classe_a_faturamento_percentual_formatado,
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
    curvaAbc: {
      itensClasseA: resumoCurvaAbc.itens_classe_a,
      percentualItensClasseA: resumoCurvaAbc.percentual_itens_classe_a,
      faturamentoClasseAPercentual: resumoCurvaAbc.classe_a_faturamento_percentual,
    },
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
