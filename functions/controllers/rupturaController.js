const {buscarResumoPrevisaoRuptura} = require("../services/rupturaService");
const {getEmpresaIdFromRequest} = require("../services/tenantService");

async function handleResumoPrevisaoRuptura(req, res) {
  try {
    const summary = await buscarResumoPrevisaoRuptura({
      empresaId: getEmpresaIdFromRequest(req),
    });

    res.json({
      ok: true,
      summary,
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
  handleResumoPrevisaoRuptura,
};
