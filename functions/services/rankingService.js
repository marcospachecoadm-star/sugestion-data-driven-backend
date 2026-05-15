const {getDb} = require("../repositories/firebaseRepository");
const {normalizeEmpresaId} = require("./tenantService");

const DEFAULT_LIMIT = 10;
const MAX_LIMIT = 100;

async function buscarRankingVendas(params = {}) {
  const empresaId = normalizeEmpresaId(params.empresaId);

  if (!empresaId) {
    throw new Error("empresaId e obrigatorio para consultar ranking de vendas.");
  }

  const limit = normalizeLimit(params.limit);
  const snap = await getDb()
    .collection("ranking_vendas")
    .where("empresa_id", "==", empresaId)
    .orderBy("ranking", "asc")
    .limit(limit)
    .get();

  const items = snap.docs.map((doc) => ({
    id: doc.id,
    ...doc.data(),
  }));

  return {
    empresaId,
    limit,
    total: items.length,
    items,
  };
}

function normalizeLimit(value) {
  const parsed = Number(value || DEFAULT_LIMIT);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_LIMIT;
  }

  return Math.min(Math.floor(parsed), MAX_LIMIT);
}

module.exports = {
  buscarRankingVendas,
};
