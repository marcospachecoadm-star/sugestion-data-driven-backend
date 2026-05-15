const {buscarAlertas} = require("../services/alertasService");
const {getEmpresaIdFromRequest} = require("../services/tenantService");

async function handleAlertas(req, res) {
  try {
    const result = await buscarAlertas({
      empresaId: getEmpresaIdFromRequest(req),
      tipo: req.query.tipo,
      limit: req.query.limit,
    });

    res.json({
      ok: true,
      ...result,
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
  handleAlertas,
};
