const {buscarRankingVendas} = require("../services/rankingService");
const {getEmpresaIdFromRequest} = require("../services/tenantService");

async function handleRankingVendas(req, res) {
  try {
    const ranking = await buscarRankingVendas({
      empresaId: getEmpresaIdFromRequest(req),
      limit: req.query.limit,
    });

    res.json({
      ok: true,
      ...ranking,
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({
      ok: false,
      error: error && error.message ? error.message : "Erro desconhecido",
    });
  }
}

module.exports = {
  handleRankingVendas,
};
