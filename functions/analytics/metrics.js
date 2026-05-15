const {
  WINDOW_DAYS,
  DEFAULT_COVERAGE_TARGET_DAYS,
  DEFAULT_SAFETY_DAYS,
  PRODUCT_ID_KEYS,
  STOCK_QUANTITY_KEYS,
  SALES_QUANTITY_KEYS,
  SALES_TOTAL_KEYS,
  UNIT_COST_KEYS,
} = require("./constants");
const {
  firstString,
  firstNumber,
  normalizeProductId,
  safeDocId,
  sum,
  round,
  formatCurrency,
} = require("./utils");
const {admin} = require("../repositories/firebaseRepository");

function createEmptyMetrics(produtoId, data) {
  return {
    produtoId,
    empresaId: firstString(data, ["empresa_id", "empresaId", "tenant_id"], null),
    produtoNome: firstString(data, ["produto_nome", "nome", "descricao"], produtoId),
    categoria: firstString(data, ["categoria", "departamento"], "Sem categoria"),
    estoqueAtual: firstNumber(data, STOCK_QUANTITY_KEYS, 0),
    estoqueDesejado: firstNumber(data, ["estoque_desejado"], 0),
    diasCoberturaAlvo: firstNumber(data, ["dias_cobertura_alvo"], DEFAULT_COVERAGE_TARGET_DAYS),
    diasSeguranca: firstNumber(data, ["dias_seguranca"], DEFAULT_SAFETY_DAYS),
    quantidadeVendida: firstNumber(data, SALES_QUANTITY_KEYS, 0),
    quantidadeVendida7Dias: 0,
    totalVendido: firstNumber(data, SALES_TOTAL_KEYS, 0),
    totalVendido7Dias: 0,
    custoUnitario: firstNumber(data, UNIT_COST_KEYS, 0),
    valorUnitarioMedio: 0,
    vendaPerdidaEstimada: 0,
    disponibilidadePrateleira: 100,
    taxaRupturaSku: 0,
    statusNiq: "sem_giro",
    acaoRecomendada: "monitorar",
    mediaVendaDia: 0,
    diasCobertura: null,
    risco: "baixo",
    prioridade: "baixa",
    quantidadeSugerida: 0,
    investimentoSugerido: 0,
    abcClass: "C",
    ranking: 0,
    percentualVendas: 0,
    percentualAcumulado: 0,
  };
}

function getOrCreateMetrics(metricsByProduct, produtoId, data) {
  const existing = metricsByProduct.get(produtoId);
  if (existing) {
    return existing;
  }

  const metrics = createEmptyMetrics(produtoId, data);
  metricsByProduct.set(produtoId, metrics);
  return metrics;
}

function mergeProductIdentity(metrics, data) {
  metrics.produtoNome = firstString(data, ["produto_nome", "nome", "descricao"], metrics.produtoNome);
  metrics.categoria = firstString(data, ["categoria", "departamento"], metrics.categoria);
  metrics.custoUnitario = firstNumber(data, UNIT_COST_KEYS, metrics.custoUnitario);
}

function applyCalculations(metricsList) {
  for (const item of metricsList) {
    if (item.totalVendido <= 0 && item.quantidadeVendida > 0 && item.custoUnitario > 0) {
      item.totalVendido = item.quantidadeVendida * item.custoUnitario;
    }
  }

  const totalVendasGeral = sum(metricsList, (item) => item.totalVendido);
  const sortedBySales = [...metricsList].sort((a, b) => b.totalVendido - a.totalVendido);
  let acumulado = 0;

  sortedBySales.forEach((item, index) => {
    item.ranking = index + 1;
    item.percentualVendas = totalVendasGeral > 0 ? (item.totalVendido / totalVendasGeral) * 100 : 0;
    acumulado += item.percentualVendas;
    item.percentualAcumulado = acumulado;
    item.abcClass = acumulado <= 80 ? "A" : acumulado <= 95 ? "B" : "C";
  });

  for (const item of metricsList) {
    item.mediaVendaDia = item.quantidadeVendida / WINDOW_DAYS;
    item.diasCobertura = item.mediaVendaDia > 0 ? item.estoqueAtual / item.mediaVendaDia : null;

    const alvoEstoque = item.estoqueDesejado > 0 ?
      item.estoqueDesejado :
      item.mediaVendaDia * (item.diasCoberturaAlvo + item.diasSeguranca);

    item.quantidadeSugerida = Math.max(0, Math.ceil(alvoEstoque - item.estoqueAtual));
    item.investimentoSugerido = item.quantidadeSugerida * item.custoUnitario;
    item.risco = calculateRisk(item);
    item.prioridade = calculatePriority(item);
    applyNiqIndicators(item);
  }
}

function applyNiqIndicators(item) {
  item.valorUnitarioMedio = item.quantidadeVendida > 0 ?
    item.totalVendido / item.quantidadeVendida :
    item.custoUnitario;
  item.disponibilidadePrateleira = item.estoqueAtual > 0 ? 100 : 0;
  item.taxaRupturaSku = item.quantidadeVendida > 0 && item.estoqueAtual <= 0 ? 100 : 0;

  if (item.mediaVendaDia <= 0) {
    item.statusNiq = item.estoqueAtual > 0 ? "sem_giro_com_estoque" : "sem_giro";
    item.acaoRecomendada = item.estoqueAtual > 0 ? "avaliar_sortimento" : "monitorar";
    item.vendaPerdidaEstimada = 0;
    return;
  }

  const diasCobertura = item.diasCobertura === null ? 0 : item.diasCobertura;
  const diasEmRisco = item.estoqueAtual <= 0 ?
    item.diasSeguranca :
    Math.max(0, item.diasSeguranca - diasCobertura);
  item.vendaPerdidaEstimada = diasEmRisco * item.mediaVendaDia * item.valorUnitarioMedio;

  if (item.estoqueAtual <= 0) {
    item.statusNiq = "ruptura";
    item.acaoRecomendada = "repor_urgente";
    return;
  }

  if (item.risco === "alto") {
    item.statusNiq = "risco_ruptura";
    item.acaoRecomendada = "antecipar_reposicao";
    return;
  }

  if (item.diasCobertura !== null && item.diasCobertura > item.diasCoberturaAlvo * 2) {
    item.statusNiq = "excesso_estoque";
    item.acaoRecomendada = "reduzir_compra_ou_promocionar";
    return;
  }

  item.statusNiq = "saudavel";
  item.acaoRecomendada = "manter";
}

function calculateRisk(item) {
  if (item.mediaVendaDia <= 0) {
    return "baixo";
  }

  if (item.diasCobertura !== null && item.diasCobertura <= item.diasSeguranca) {
    return "alto";
  }

  if (item.diasCobertura !== null && item.diasCobertura <= item.diasSeguranca * 2) {
    return "medio";
  }

  return "baixo";
}

function calculatePriority(item) {
  if (item.risco === "alto" || (item.abcClass === "A" && item.quantidadeSugerida > 0)) {
    return "alta";
  }

  if (item.risco === "medio" || item.quantidadeSugerida > 0) {
    return "media";
  }

  return "baixa";
}

function buildAlerts(metricsList) {
  const alerts = [];

  for (const item of metricsList) {
    if (item.risco === "alto") {
      alerts.push({
        id: `${safeDocId(item.produtoId)}_ruptura`,
        empresa_id: item.empresaId || null,
        tipo: "ruptura",
        produto_id: item.produtoId,
        produto_nome: item.produtoNome,
        categoria: item.categoria,
        prioridade: "alta",
        estoque_atual: round(item.estoqueAtual),
        dias_cobertura: item.diasCobertura === null ? null : round(item.diasCobertura),
        venda_perdida_estimada: round(item.vendaPerdidaEstimada),
        venda_perdida_estimada_formatada: formatCurrency(item.vendaPerdidaEstimada),
        status_niq: item.statusNiq,
        acao_recomendada: item.acaoRecomendada,
        status: "pendente",
        criado_em: admin.firestore.FieldValue.serverTimestamp(),
      });
    }

    if (item.mediaVendaDia > 0 && item.diasCobertura !== null && item.diasCobertura > item.diasCoberturaAlvo * 2) {
      alerts.push({
        id: `${safeDocId(item.produtoId)}_excesso`,
        empresa_id: item.empresaId || null,
        tipo: "excesso_estoque",
        produto_id: item.produtoId,
        produto_nome: item.produtoNome,
        categoria: item.categoria,
        prioridade: "media",
        estoque_atual: round(item.estoqueAtual),
        dias_cobertura: round(item.diasCobertura),
        venda_perdida_estimada: round(item.vendaPerdidaEstimada),
        venda_perdida_estimada_formatada: formatCurrency(item.vendaPerdidaEstimada),
        status_niq: item.statusNiq,
        acao_recomendada: item.acaoRecomendada,
        status: "pendente",
        criado_em: admin.firestore.FieldValue.serverTimestamp(),
      });
    }
  }

  return alerts;
}

function normalizeId(data, fallback) {
  return normalizeProductId(firstString(data, PRODUCT_ID_KEYS, fallback));
}

module.exports = {
  createEmptyMetrics,
  getOrCreateMetrics,
  mergeProductIdentity,
  applyCalculations,
  buildAlerts,
  normalizeId,
};
