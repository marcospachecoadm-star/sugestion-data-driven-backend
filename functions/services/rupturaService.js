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
  let somaCobertura = 0;
  let itensComCobertura = 0;

  for (const doc of snap.docs) {
    const item = doc.data();

    if (item.risco === "alto") {
      riscoAlto += 1;
    }

    const diasCobertura = Number(item.dias_cobertura);
    if (Number.isFinite(diasCobertura)) {
      somaCobertura += diasCobertura;
      itensComCobertura += 1;
    }
  }

  const mediaCobertura = itensComCobertura > 0 ?
    round(somaCobertura / itensComCobertura) :
    0;

  return {
    empresaId,
    totalItens: snap.size,
    riscoAlto,
    mediaCobertura,
  };
}

function round(value) {
  return Math.round((value + Number.EPSILON) * 100) / 100;
}

module.exports = {
  buscarResumoPrevisaoRuptura,
};
