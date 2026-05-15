const {rebuildAnalytics} = require("../analytics/rebuildAnalytics");
const {getStorageBucket} = require("../repositories/firebaseRepository");
const {processarUploadsPendentes} = require("../services/csvImportService");
const {getEmpresaIdFromRequest} = require("../services/tenantService");

async function handleImportStorageCsv(req, res, shouldRunAnalytics) {
  try {
    const requestedEmpresaId = getEmpresaIdFromRequest(req);
    const importSummary = await processarUploadsPendentes(requestedEmpresaId);
    const empresas = requestedEmpresaId ?
      [requestedEmpresaId] :
      [...new Set(importSummary.resultados
        .filter((item) => item.empresaId)
        .map((item) => item.empresaId))];

    const analyticsSummary = [];
    if (shouldRunAnalytics) {
      for (const empresaId of empresas) {
        analyticsSummary.push(await rebuildAnalytics(empresaId));
      }
    }

    res.json({
      ok: true,
      importacao: importSummary,
      analytics: analyticsSummary,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      ok: false,
      error: error && error.message ? error.message : "Erro desconhecido",
    });
  }
}

async function handleDebugStorage(req, res) {
  try {
    const empresaId = getEmpresaIdFromRequest(req);
    const bucket = getStorageBucket();
    const prefix = empresaId ? `uploads/${empresaId}/` : "uploads/";
    const [files] = await bucket.getFiles({prefix});

    res.json({
      ok: true,
      bucket: bucket.name,
      prefix,
      total: files.length,
      files: files.map((file) => file.name),
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      ok: false,
      error: error && error.message ? error.message : "Erro desconhecido",
    });
  }
}

module.exports = {
  handleImportStorageCsv,
  handleDebugStorage,
};
