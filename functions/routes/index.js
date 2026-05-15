const express = require("express");
const {handleAlertas} = require("../controllers/alertasController");
const {handleResumoIndicadores} = require("../controllers/indicadoresController");
const {handleRunAnalytics} = require("../controllers/analyticsController");
const {handleProdutosMortos} = require("../controllers/produtosMortosController");
const {handleRankingVendas} = require("../controllers/rankingController");
const {handleResumoPrevisaoRuptura} = require("../controllers/rupturaController");
const {
  handleDebugStorage,
  handleImportStorageCsv,
} = require("../controllers/storageController");
const {requireApiKey} = require("../middlewares/apiKey");

const router = express.Router();

router.get("/", (_req, res) => {
  res.json({
    ok: true,
    service: "SugestionDataDriven Backend",
    routes: [
      "/health",
      "/alertas",
      "/ranking-vendas",
      "/produtos-mortos",
      "/indicadores/resumo",
      "/previsao-ruptura/resumo",
      "/import-storage-csv",
      "/import-and-run",
      "/run-analytics",
    ],
  });
});

router.get("/health", (_req, res) => {
  res.json({ok: true, status: "online"});
});

router.get("/debug-storage", requireApiKey, handleDebugStorage);
router.get("/alertas", requireApiKey, handleAlertas);
router.get("/ranking-vendas", requireApiKey, handleRankingVendas);
router.get("/produtos-mortos", requireApiKey, handleProdutosMortos);
router.get("/indicadores/resumo", requireApiKey, handleResumoIndicadores);
router.get("/previsao-ruptura/resumo", requireApiKey, handleResumoPrevisaoRuptura);
router.get("/run-analytics", requireApiKey, handleRunAnalytics);
router.post("/run-analytics", requireApiKey, handleRunAnalytics);
router.get("/import-storage-csv", requireApiKey, (req, res) => {
  handleImportStorageCsv(req, res, false);
});
router.post("/import-storage-csv", requireApiKey, (req, res) => {
  handleImportStorageCsv(req, res, false);
});
router.get("/import-and-run", requireApiKey, (req, res) => {
  handleImportStorageCsv(req, res, true);
});
router.post("/import-and-run", requireApiKey, (req, res) => {
  handleImportStorageCsv(req, res, true);
});

module.exports = router;
