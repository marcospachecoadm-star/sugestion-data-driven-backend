const {getDb} = require("../repositories/firebaseRepository");
const {normalizeEmpresaId} = require("./tenantService");

async function buscarResumoIndicadores(params = {}) {
  const empresaId = normalizeEmpresaId(params.empresaId);

  if (!empresaId) {
    throw new Error("empresaId e obrigatorio para consultar indicadores.");
  }

  const db = getDb();
  const [
    rankingSnap,
    curvaSnap,
    rupturaSnap,
    sugestoesSnap,
    mortosSnap,
    alertasSnap,
  ] = await Promise.all([
    getTenantSnap(db, "ranking_vendas", empresaId),
    getTenantSnap(db, "curva_abc", empresaId),
    getTenantSnap(db, "previsao_ruptura", empresaId),
    getTenantSnap(db, "sugestoes_compra", empresaId),
    getTenantSnap(db, "produtos_mortos", empresaId),
    getTenantSnap(db, "alertas", empresaId),
  ]);

  const ranking = summarizeRanking(rankingSnap.docs);
  const curvaAbc = summarizeCurvaAbc(curvaSnap.docs);
  const ruptura = summarizeRuptura(rupturaSnap.docs);
  const sugestoes = summarizeSugestoes(sugestoesSnap.docs);
  const produtosMortos = summarizeProdutosMortos(mortosSnap.docs);
  const alertas = summarizeAlertas(alertasSnap.docs);

  return {
    empresaId,
    metodologia: "NIQ OSA: disponibilidade de prateleira, ruptura, venda perdida, cobertura, priorizacao por valor e acao por SKU.",
    ranking,
    curvaAbc,
    ruptura,
    sugestoes,
    produtosMortos,
    alertas,
  };
}

function getTenantSnap(db, collectionName, empresaId) {
  return db
    .collection(collectionName)
    .where("empresa_id", "==", empresaId)
    .get();
}

function summarizeRanking(docs) {
  const items = docs.map((doc) => doc.data());
  const totalVendas = sum(items, "total_vendido");
  const totalVendas7Dias = sum(items, "total_vendido_7_dias");
  const quantidadeVendida = sum(items, "quantidade_vendida");
  const quantidadeVendida7Dias = sum(items, "quantidade_vendida_7_dias");
  const vendaPerdidaEstimada = sum(items, "venda_perdida_estimada");

  return {
    totalItens: items.length,
    totalVendas: round(totalVendas),
    totalVendasFormatado: formatCurrency(totalVendas),
    totalVendas7Dias: round(totalVendas7Dias),
    totalVendas7DiasFormatado: formatCurrency(totalVendas7Dias),
    quantidadeVendida: round(quantidadeVendida),
    quantidadeVendida7Dias: round(quantidadeVendida7Dias),
    vendaPerdidaEstimada: round(vendaPerdidaEstimada),
    vendaPerdidaEstimadaFormatada: formatCurrency(vendaPerdidaEstimada),
    ticketMedioPorUnidade: quantidadeVendida > 0 ? round(totalVendas / quantidadeVendida) : 0,
    ticketMedioPorUnidadeFormatado: formatCurrency(quantidadeVendida > 0 ? totalVendas / quantidadeVendida : 0),
  };
}

function summarizeCurvaAbc(docs) {
  const counts = {A: 0, B: 0, C: 0};
  let vendaClasseA = 0;
  let vendaClasseB = 0;
  let vendaClasseC = 0;

  for (const doc of docs) {
    const item = doc.data();
    const classe = item.classe || "C";
    const totalVendido = Number(item.total_vendido || 0);

    if (classe === "A") {
      counts.A += 1;
      vendaClasseA += totalVendido;
    } else if (classe === "B") {
      counts.B += 1;
      vendaClasseB += totalVendido;
    } else {
      counts.C += 1;
      vendaClasseC += totalVendido;
    }
  }

  const totalVendas = vendaClasseA + vendaClasseB + vendaClasseC;

  return {
    classeA: counts.A,
    classeB: counts.B,
    classeC: counts.C,
    vendaClasseA: round(vendaClasseA),
    vendaClasseAFormatada: formatCurrency(vendaClasseA),
    vendaClasseB: round(vendaClasseB),
    vendaClasseBFormatada: formatCurrency(vendaClasseB),
    vendaClasseC: round(vendaClasseC),
    vendaClasseCFormatada: formatCurrency(vendaClasseC),
    participacaoClasseA: percent(vendaClasseA, totalVendas),
    participacaoClasseAFormatada: formatPercent(percent(vendaClasseA, totalVendas)),
  };
}

function summarizeRuptura(docs) {
  let riscoAlto = 0;
  let riscoMedio = 0;
  let riscoBaixo = 0;
  let ruptura = 0;
  let excessoEstoque = 0;
  let itensComVenda = 0;
  let itensDisponiveis = 0;
  let somaCobertura = 0;
  let itensComCobertura = 0;
  let vendaPerdidaEstimada = 0;
  let vendaPerdidaEstimadaAltoRisco = 0;

  for (const doc of docs) {
    const item = doc.data();
    const risco = item.risco || "baixo";
    const estoqueAtual = Number(item.estoque_atual || 0);
    const mediaVendaDia = Number(item.media_venda_dia || 0);
    const perdaItem = Number(item.venda_perdida_estimada || 0);
    const diasCobertura = Number(item.dias_cobertura);

    if (risco === "alto") {
      riscoAlto += 1;
      vendaPerdidaEstimadaAltoRisco += Number.isFinite(perdaItem) ? perdaItem : 0;
    } else if (risco === "medio") {
      riscoMedio += 1;
    } else {
      riscoBaixo += 1;
    }

    if (mediaVendaDia > 0) itensComVenda += 1;
    if (estoqueAtual > 0) itensDisponiveis += 1;
    if (mediaVendaDia > 0 && estoqueAtual <= 0) ruptura += 1;

    if (Number.isFinite(diasCobertura)) {
      somaCobertura += diasCobertura;
      itensComCobertura += 1;
      if (mediaVendaDia > 0 && diasCobertura > 60) excessoEstoque += 1;
    }

    if (Number.isFinite(perdaItem)) vendaPerdidaEstimada += perdaItem;
  }

  return {
    totalItens: docs.length,
    riscoAlto,
    riscoMedio,
    riscoBaixo,
    ruptura,
    excessoEstoque,
    mediaCobertura: itensComCobertura > 0 ? round(somaCobertura / itensComCobertura) : 0,
    disponibilidadePrateleira: itensComVenda > 0 ? round((itensDisponiveis / itensComVenda) * 100) : 100,
    disponibilidadePrateleiraFormatada: formatPercent(itensComVenda > 0 ? (itensDisponiveis / itensComVenda) * 100 : 100),
    taxaRuptura: itensComVenda > 0 ? round((ruptura / itensComVenda) * 100) : 0,
    taxaRupturaFormatada: formatPercent(itensComVenda > 0 ? (ruptura / itensComVenda) * 100 : 0),
    percentualRiscoAlto: docs.length > 0 ? round((riscoAlto / docs.length) * 100) : 0,
    percentualRiscoAltoFormatado: formatPercent(docs.length > 0 ? (riscoAlto / docs.length) * 100 : 0),
    vendaPerdidaEstimada: round(vendaPerdidaEstimada),
    vendaPerdidaEstimadaFormatada: formatCurrency(vendaPerdidaEstimada),
    vendaPerdidaEstimadaAltoRisco: round(vendaPerdidaEstimadaAltoRisco),
    vendaPerdidaEstimadaAltoRiscoFormatada: formatCurrency(vendaPerdidaEstimadaAltoRisco),
  };
}

function summarizeSugestoes(docs) {
  const items = docs.map((doc) => doc.data());
  const investimento = sum(items, "investimento_sugerido");
  const quantidadeSugerida = sum(items, "quantidade_sugerida");
  const prioridade = countBy(items, "prioridade");

  return {
    totalItens: items.length,
    quantidadeSugerida: round(quantidadeSugerida),
    investimentoSugerido: round(investimento),
    investimentoSugeridoFormatado: formatCurrency(investimento),
    prioridadeAlta: prioridade.alta || 0,
    prioridadeMedia: prioridade.media || 0,
    prioridadeBaixa: prioridade.baixa || 0,
  };
}

function summarizeProdutosMortos(docs) {
  const items = docs.map((doc) => doc.data());
  const estoqueParado = sum(items, "estoque_atual");
  const vendaPerdidaEstimada = sum(items, "venda_perdida_estimada");

  return {
    totalItens: items.length,
    estoqueParado: round(estoqueParado),
    vendaPerdidaEstimada: round(vendaPerdidaEstimada),
    vendaPerdidaEstimadaFormatada: formatCurrency(vendaPerdidaEstimada),
  };
}

function summarizeAlertas(docs) {
  const items = docs.map((doc) => doc.data());
  const tipo = countBy(items, "tipo");
  const prioridade = countBy(items, "prioridade");
  const status = countBy(items, "status");

  return {
    totalItens: items.length,
    ruptura: tipo.ruptura || 0,
    excessoEstoque: tipo.excesso_estoque || 0,
    prioridadeAlta: prioridade.alta || 0,
    prioridadeMedia: prioridade.media || 0,
    prioridadeBaixa: prioridade.baixa || 0,
    pendentes: status.pendente || 0,
  };
}

function sum(items, field) {
  return items.reduce((total, item) => {
    const value = Number(item[field] || 0);
    return total + (Number.isFinite(value) ? value : 0);
  }, 0);
}

function countBy(items, field) {
  return items.reduce((counts, item) => {
    const key = item[field] || "sem_valor";
    counts[key] = (counts[key] || 0) + 1;
    return counts;
  }, {});
}

function percent(value, total) {
  return total > 0 ? round((value / total) * 100) : 0;
}

function round(value) {
  return Math.round((value + Number.EPSILON) * 100) / 100;
}

function formatCurrency(value) {
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
  }).format(value);
}

function formatPercent(value) {
  return `${round(value).toLocaleString("pt-BR", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  })}%`;
}

module.exports = {
  buscarResumoIndicadores,
};
