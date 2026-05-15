const express = require("express");
const {handleRunAnalytics} = require("../controllers/analyticsController");
const {handleRankingVendas} = require("../controllers/rankingController");
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
    routes: ["/health", "/ranking-vendas", "/import-storage-csv", "/import-and-run", "/run-analytics"],
  });
});

router.get("/health", (_req, res) => {
  res.json({ok: true, status: "online"});
});

router.get("/debug-storage", requireApiKey, handleDebugStorage);
router.get("/ranking-vendas", requireApiKey, handleRankingVendas);
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
