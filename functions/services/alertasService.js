const {getDb} = require("../repositories/firebaseRepository");
const {normalizeEmpresaId} = require("./tenantService");

const DEFAULT_LIMIT = 20;
const MAX_LIMIT = 100;

async function buscarAlertas(params = {}) {
  const empresaId = normalizeEmpresaId(params.empresaId);

  if (!empresaId) {
    throw new Error("empresaId e obrigatorio para consultar alertas.");
  }

  const limit = normalizeLimit(params.limit);
  let query = getDb()
    .collection("alertas")
    .where("empresa_id", "==", empresaId);

  if (params.tipo) {
    query = query.where("tipo", "==", params.tipo);
  }

  const snap = await query.limit(limit).get();
  const items = snap.docs.map((doc) => ({
    id: doc.id,
    ...doc.data(),
  }));

  return {
    empresaId,
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
  buscarAlertas,
};
