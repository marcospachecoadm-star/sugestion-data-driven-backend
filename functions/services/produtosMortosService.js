const {getDb} = require("../repositories/firebaseRepository");
const {normalizeEmpresaId} = require("./tenantService");

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 200;

async function buscarProdutosMortos(params = {}) {
  const empresaId = normalizeEmpresaId(params.empresaId);

  if (!empresaId) {
    throw new Error("empresaId e obrigatorio para consultar produtos mortos.");
  }

  const limit = normalizeLimit(params.limit);
  const snap = await getDb()
    .collection("produtos_mortos")
    .where("empresa_id", "==", empresaId)
    .limit(limit)
    .get();

  const items = snap.docs.map((doc) => normalizeProdutoMorto(doc.id, doc.data()));
  const valorTotalParado = items.reduce((total, item) => total + item.valorParado, 0);
  const somaDiasParados = items.reduce((total, item) => total + item.diasParados, 0);
  const mediaDiasParados = items.length > 0 ? round(somaDiasParados / items.length) : 0;

  return {
    empresaId,
    totalItens: items.length,
    valorTotalParado: round(valorTotalParado),
    valorTotalParadoFormatado: formatCurrency(valorTotalParado),
    mediaDiasParados,
    items,
  };
}

function normalizeProdutoMorto(id, data) {
  const estoqueAtual = Number(data.estoque_atual || data.estoque || 0);
  const custoUnitario = Number(data.custo_unitario || data.preco_custo || data.custo || 0);
  const totalVendido = Number(data.total_vendido || 0);
  const valorParado = Number(data.valor_parado || data.estoque_parado_valor || estoqueAtual * custoUnitario || 0);
  const diasParados = Number(data.dias_parados || data.dias_sem_venda || data.dias_sem_giro || 90);

  return {
    id,
    produtoId: data.produto_id || id,
    produtoNome: data.produto_nome || "Produto",
    categoria: data.categoria || "Sem categoria",
    status: data.status_niq || "sem_giro",
    statusLabel: formatStatus(data.status_niq || "sem_giro"),
    estoqueAtual: round(estoqueAtual),
    estoqueAtualFormatado: `${round(estoqueAtual)} un`,
    diasParados: round(diasParados),
    diasParadosFormatado: `${round(diasParados)} dias`,
    valorParado: round(valorParado),
    valorParadoFormatado: formatCurrency(valorParado),
    totalVendido: round(totalVendido),
    totalVendidoFormatado: formatCurrency(totalVendido),
    acaoRecomendada: data.acao_recomendada || "liquidar",
    prioridade: data.prioridade || "media",
  };
}

function normalizeLimit(value) {
  const parsed = Number(value || DEFAULT_LIMIT);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_LIMIT;
  }

  return Math.min(Math.floor(parsed), MAX_LIMIT);
}

function formatStatus(status) {
  const labels = {
    sem_giro: "Sem Giro",
    sem_giro_com_estoque: "Sem Giro",
    excesso_estoque: "Excesso",
  };

  return labels[status] || status;
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

module.exports = {
  buscarProdutosMortos,
};
