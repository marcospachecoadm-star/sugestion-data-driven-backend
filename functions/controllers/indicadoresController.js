const {buscarResumoIndicadores} = require("../services/indicadoresService");
const {getEmpresaIdFromRequest} = require("../services/tenantService");

async function handleResumoIndicadores(req, res) {
  try {
    const summary = await buscarResumoIndicadores({
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
  handleResumoIndicadores,
};
