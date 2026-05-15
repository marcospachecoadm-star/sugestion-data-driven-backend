const functions = require("firebase-functions/v1");
const {
  initializeFirebase,
  getStorageBucket,
} = require("./repositories/firebaseRepository");
const {
  processarArquivoCsv,
  processarUploadsPendentes: processarUploadsPendentesService,
} = require("./services/csvImportService");
const {rebuildAnalytics} = require("./analytics/rebuildAnalytics");

initializeFirebase();

function getEmpresaIdFromRequest(req) {
  const value = req.query.empresaId || req.query.empresa_id || req.header("x-empresa-id");
  return value ? String(value).trim() : null;
}

function shouldExecute(req) {
  return req.query.executar === "sim";
}

function sendPendingExecution(res, message = "Processamento pendente. Para executar, use ?executar=sim") {
  res.status(200).send(message);
}

function legacyAnalyticsConsolidated(name) {
  return functions.pubsub
    .schedule("every 168 hours")
    .onRun(async () => {
      console.log(`${name}: rotina consolidada em calcularDashboard para evitar leituras duplicadas.`);
      return null;
    });
}

async function runAnalyticsForRequest(req, res) {
  if (!shouldExecute(req)) {
    sendPendingExecution(res);
    return;
  }

  try {
    const empresaId = getEmpresaIdFromRequest(req);
    const summary = await rebuildAnalytics(empresaId);
    res.status(200).json({
      ok: true,
      summary,
    });
  } catch (error) {
    console.error("Erro ao processar indicadores:", error);
    res.status(500).json({
      ok: false,
      erro: error && error.message ? error.message : String(error),
    });
  }
}

exports.testeUpload = functions
  .runWith({
    timeoutSeconds: 540,
    memory: "1GB",
  })
  .storage
  .object()
  .onFinalize(async (object) => {
    console.log("Arquivo enviado:", object.name);

    const bucket = getStorageBucket();
    await processarArquivoCsv(bucket, object.name);

    return null;
  });

exports.processarUploadsPendentes = functions
  .runWith({
    timeoutSeconds: 540,
    memory: "1GB",
  })
  .https.onRequest(async (req, res) => {
    if (!shouldExecute(req)) {
      sendPendingExecution(res);
      return;
    }

    try {
      const empresaId = getEmpresaIdFromRequest(req);
      const resultado = await processarUploadsPendentesService(empresaId);
      res.status(200).json({
        ok: true,
        ...resultado,
      });
    } catch (error) {
      console.error("Erro ao processar uploads pendentes:", error);
      res.status(500).json({
        ok: false,
        erro: error && error.message ? error.message : String(error),
      });
    }
  });

exports.calcularDashboard = functions.pubsub
  .schedule("every 168 hours")
  .onRun(async () => {
    console.log("Calculando analytics consolidado");
    const summary = await rebuildAnalytics();
    console.log("Analytics consolidado atualizado:", summary);
    return null;
  });

exports.calcularAlertas = legacyAnalyticsConsolidated("calcularAlertas");
exports.calcularProdutosMortos = legacyAnalyticsConsolidated("calcularProdutosMortos");
exports.calcularRankingVendas = legacyAnalyticsConsolidated("calcularRankingVendas");
exports.calcularCurvaABC = legacyAnalyticsConsolidated("calcularCurvaABC");
exports.calcularSugestoesCompra = legacyAnalyticsConsolidated("calcularSugestoesCompra");
exports.calcularPrevisaoRuptura = legacyAnalyticsConsolidated("calcularPrevisaoRuptura");

exports.testarAlertas = functions.https.onRequest(async (req, res) => {
  await runAnalyticsForRequest(req, res);
});

exports.processarIndicadoresAgora = functions
  .runWith({
    timeoutSeconds: 540,
    memory: "1GB",
  })
  .https.onRequest(async (req, res) => {
    await runAnalyticsForRequest(req, res);
  });

exports.processarDashboardAgora = functions
  .runWith({
    timeoutSeconds: 540,
    memory: "1GB",
  })
  .https.onRequest(async (req, res) => {
    await runAnalyticsForRequest(req, res);
  });
