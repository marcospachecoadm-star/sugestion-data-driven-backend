const {buscarProdutosMortos} = require("../services/produtosMortosService");
const {getEmpresaIdFromRequest} = require("../services/tenantService");

async function handleProdutosMortos(req, res) {
  try {
    const result = await buscarProdutosMortos({
      empresaId: getEmpresaIdFromRequest(req),
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
  handleProdutosMortos,
};
