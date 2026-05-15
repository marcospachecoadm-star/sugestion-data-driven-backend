const {rebuildAnalytics} = require("../analytics/rebuildAnalytics");
const {getEmpresaIdFromRequest} = require("../services/tenantService");

async function handleRunAnalytics(req, res) {
  try {
    const empresaId = getEmpresaIdFromRequest(req);
    const summary = await rebuildAnalytics(empresaId);
    res.json({ok: true, summary});
  } catch (error) {
    console.error(error);
    res.status(500).json({
      ok: false,
      error: error && error.message ? error.message : "Erro desconhecido",
    });
  }
}

module.exports = {
  handleRunAnalytics,
};
