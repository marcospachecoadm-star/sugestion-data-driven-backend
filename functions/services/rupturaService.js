const {getDb} = require("../repositories/firebaseRepository");
const {normalizeEmpresaId} = require("./tenantService");

async function buscarResumoPrevisaoRuptura(params = {}) {
  const empresaId = normalizeEmpresaId(params.empresaId);

  if (!empresaId) {
    throw new Error("empresaId e obrigatorio para consultar previsao de ruptura.");
  }

  const snap = await getDb()
    .collection("previsao_ruptura")
    .where("empresa_id", "==", empresaId)
    .get();

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

  for (const doc of snap.docs) {
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

    if (mediaVendaDia > 0) {
      itensComVenda += 1;
    }

    if (estoqueAtual > 0) {
      itensDisponiveis += 1;
    }

    if (mediaVendaDia > 0 && estoqueAtual <= 0) {
      ruptura += 1;
    }

    if (Number.isFinite(diasCobertura)) {
      somaCobertura += diasCobertura;
      itensComCobertura += 1;

      if (mediaVendaDia > 0 && diasCobertura > 60) {
        excessoEstoque += 1;
      }
    }

    if (Number.isFinite(perdaItem)) {
      vendaPerdidaEstimada += perdaItem;
    }
  }

  const totalItens = snap.size;
  const mediaCobertura = itensComCobertura > 0 ?
    round(somaCobertura / itensComCobertura) :
    0;
  const disponibilidadePrateleira = itensComVenda > 0 ?
    round((itensDisponiveis / itensComVenda) * 100) :
    100;
  const taxaRuptura = itensComVenda > 0 ?
    round((ruptura / itensComVenda) * 100) :
    0;
  const percentualRiscoAlto = totalItens > 0 ?
    round((riscoAlto / totalItens) * 100) :
    0;

  return {
    empresaId,
    metodologia: "NIQ OSA: disponibilidade em prateleira, ruptura, risco de ruptura, cobertura e venda perdida estimada",
    totalItens,
    riscoAlto,
    riscoMedio,
    riscoBaixo,
    ruptura,
    excessoEstoque,
    itensComVenda,
    itensDisponiveis,
    mediaCobertura,
    disponibilidadePrateleira,
    disponibilidadePrateleiraFormatada: formatPercent(disponibilidadePrateleira),
    taxaRuptura,
    taxaRupturaFormatada: formatPercent(taxaRuptura),
    percentualRiscoAlto,
    percentualRiscoAltoFormatado: formatPercent(percentualRiscoAlto),
    vendaPerdidaEstimada: round(vendaPerdidaEstimada),
    vendaPerdidaEstimadaFormatada: formatCurrency(vendaPerdidaEstimada),
    vendaPerdidaEstimadaAltoRisco: round(vendaPerdidaEstimadaAltoRisco),
    vendaPerdidaEstimadaAltoRiscoFormatada: formatCurrency(vendaPerdidaEstimadaAltoRisco),
  };
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
  buscarResumoPrevisaoRuptura,
};
