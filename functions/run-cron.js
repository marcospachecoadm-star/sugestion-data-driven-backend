require("dotenv").config();

const DEFAULT_BACKEND_URL = "https://sugestion-data-driven.onrender.com";

async function main() {
  const backendUrl = (process.env.BACKEND_PUBLIC_URL || DEFAULT_BACKEND_URL).replace(/\/$/, "");
  const empresaId = (process.env.CRON_EMPRESA_ID || "").trim();
  const apiKey = (process.env.SUGESTION_DATA_DRIVEN_API_KEY || "").trim();

  if (!empresaId) {
    throw new Error("CRON_EMPRESA_ID nao configurado.");
  }

  if (!apiKey) {
    throw new Error("SUGESTION_DATA_DRIVEN_API_KEY nao configurada.");
  }

  const url = `${backendUrl}/import-and-run?empresaId=${encodeURIComponent(empresaId)}`;
  const response = await fetch(url, {
    method: "GET",
    headers: {
      "x-api-key": apiKey,
    },
  });
  const text = await response.text();

  if (!response.ok) {
    throw new Error(`Cron falhou com HTTP ${response.status}: ${text}`);
  }

  console.log(text);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
