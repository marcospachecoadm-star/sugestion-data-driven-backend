require("dotenv").config();

const cors = require("cors");
const express = require("express");
const admin = require("firebase-admin");

const app = express();
app.use(cors());
app.use(express.json({limit: "2mb"}));

const WINDOW_DAYS = 30;
const DEFAULT_COVERAGE_TARGET_DAYS = 30;
const DEFAULT_SAFETY_DAYS = 7;
const DEAD_STOCK_MIN_UNITS = 1;

initializeFirebase();

const db = admin.firestore();

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    service: "SugestionDataDriven Backend",
    routes: ["/health", "/run-analytics"],
  });
});

app.get("/health", (_req, res) => {
  res.json({ok: true, status: "online"});
});

app.get("/run-analytics", requireApiKey, async (_req, res) => {
  await handleRunAnalytics(res);
});

app.post("/run-analytics", requireApiKey, async (_req, res) => {
  await handleRunAnalytics(res);
});

async function handleRunAnalytics(res) {
  try {
    const summary = await rebuildAnalytics();
    res.json({ok: true, summary});
  } catch (error) {
    console.error(error);
    res.status(500).json({
      ok: false,
      error: error && error.message ? error.message : "Erro desconhecido",
    });
  }
}

async function rebuildAnalytics() {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - WINDOW_DAYS);

  const [productsSnap, stockSnap, salesSnap] = await Promise.all([
    db.collection("produtos").get(),
    db.collection("estoque").get(),
    db.collection("vendas").get(),
  ]);

  const metricsByProduct = new Map();

  for (const doc of productsSnap.docs) {
    const data = doc.data();
    const produtoId = normalizeId(data, doc.id);
    metricsByProduct.set(produtoId, createEmptyMetrics(produtoId, data));
  }

  for (const doc of stockSnap.docs) {
    const data = doc.data();
    const produtoId = normalizeId(data, doc.id);
    const metrics = getOrCreateMetrics(metricsByProduct, produtoId, data);
    metrics.estoqueAtual = firstNumber(data, ["estoque_atual", "quantidade_estoque", "saldo"], metrics.estoqueAtual);
    metrics.estoqueDesejado = firstNumber(data, ["estoque_desejado"], metrics.estoqueDesejado);
    metrics.diasCoberturaAlvo = firstNumber(data, ["dias_cobertura_alvo"], metrics.diasCoberturaAlvo);
    metrics.diasSeguranca = firstNumber(data, ["dias_seguranca"], metrics.diasSeguranca);
    mergeProductIdentity(metrics, data);
  }

  let vendasProcessadas = 0;

  for (const doc of salesSnap.docs) {
    const data = doc.data();
    if (!isWithinWindow(data, cutoff)) {
      continue;
    }

    vendasProcessadas += 1;
    const produtoId = normalizeId(data, doc.id);
    const metrics = getOrCreateMetrics(metricsByProduct, produtoId, data);
    metrics.quantidadeVendida += firstNumber(data, ["quantidade_vendida", "quantidade", "qty"], 0);
    metrics.totalVendido += firstNumber(data, ["total_vendido", "valor_total", "total", "receita"], 0);
    mergeProductIdentity(metrics, data);
  }

  const metricsList = Array.from(metricsByProduct.values());
  applyCalculations(metricsList);

  const sugestoes = metricsList.filter((item) => item.quantidadeSugerida > 0);
  const alertas = buildAlerts(metricsList);
  const produtosMortos = metricsList.filter(
    (item) => item.estoqueAtual >= DEAD_STOCK_MIN_UNITS && item.quantidadeVendida <= 0,
  );

  await Promise.all([
    replaceCollection("ranking_vendas", metricsList, toRankingDoc),
    replaceCollection("curva_abc", metricsList, toCurvaAbcDoc),
    replaceCollection("previsao_ruptura", metricsList, toRupturaDoc),
    replaceCollection("sugestoes_compra", sugestoes, toSugestaoCompraDoc),
    replaceCollection("produtos_mortos", produtosMortos, toProdutoMortoDoc),
    replaceCollection("alertas", alertas, (alerta) => alerta),
  ]);

  const totalVendas = sum(metricsList, (item) => item.totalVendido);
  const investimentoSugerido = sum(sugestoes, (item) => item.investimentoSugerido);
  const giroMedio = average(metricsList, (item) => item.mediaVendaDia);
  const itensCriticos = metricsList.filter((item) => item.risco === "alto").length;

  await db.doc("insights/dashboard").set(
    {
      giro_medio: round(giroMedio),
      itens_criticos: itensCriticos,
      alertas_pendentes: alertas.length,
      investimento_sugerido: round(investimentoSugerido),
      investimento_sugerido_formatado: formatCurrency(investimentoSugerido),
      total_vendas: round(totalVendas),
      total_vendas_formatado: formatCurrency(totalVendas),
      produtos_processados: metricsList.length,
      vendas_processadas: vendasProcessadas,
      atualizado_em: admin.firestore.FieldValue.serverTimestamp(),
    },
    {merge: true},
  );

  return {
    produtosProcessados: metricsList.length,
    vendasProcessadas,
    sugestoesCompra: sugestoes.length,
    alertas: alertas.length,
    produtosMortos: produtosMortos.length,
    rankingVendas: metricsList.length,
    totalVendas: round(totalVendas),
    investimentoSugerido: round(investimentoSugerido),
  };
}

function initializeFirebase() {
  if (admin.apps.length > 0) {
    return;
  }

  const serviceAccountBase64 = process.env.FIREBASE_SERVICE_ACCOUNT_BASE64;
  const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;

  if (serviceAccountBase64) {
    const json = Buffer.from(serviceAccountBase64, "base64").toString("utf8");
    admin.initializeApp({
      credential: admin.credential.cert(JSON.parse(json)),
    });
    return;
  }

  if (serviceAccountJson) {
    admin.initializeApp({
      credential: admin.credential.cert(JSON.parse(serviceAccountJson)),
    });
    return;
  }

  admin.initializeApp({
    credential: admin.credential.applicationDefault(),
  });
}

function requireApiKey(req, res, next) {
  const expectedKey = process.env.SUGESTION_DATA_DRIVEN_API_KEY;
  if (!expectedKey) {
    next();
    return;
  }

  if (req.header("x-api-key") !== expectedKey) {
    res.status(401).json({ok: false, error: "API key invalida."});
    return;
  }

  next();
}

function createEmptyMetrics(produtoId, data) {
  return {
    produtoId,
    produtoNome: firstString(data, ["produto_nome", "nome", "descricao"], produtoId),
    categoria: firstString(data, ["categoria", "departamento"], "Sem categoria"),
    estoqueAtual: firstNumber(data, ["estoque_atual", "quantidade_estoque", "saldo"], 0),
    estoqueDesejado: firstNumber(data, ["estoque_desejado"], 0),
    diasCoberturaAlvo: firstNumber(data, ["dias_cobertura_alvo"], DEFAULT_COVERAGE_TARGET_DAYS),
    diasSeguranca: firstNumber(data, ["dias_seguranca"], DEFAULT_SAFETY_DAYS),
    quantidadeVendida: firstNumber(data, ["quantidade_vendida"], 0),
    totalVendido: firstNumber(data, ["total_vendido"], 0),
    custoUnitario: firstNumber(data, ["custo_unitario", "preco_compra", "preco"], 0),
    mediaVendaDia: 0,
    diasCobertura: null,
    risco: "baixo",
    prioridade: "baixa",
    quantidadeSugerida: 0,
    investimentoSugerido: 0,
    abcClass: "C",
    ranking: 0,
    percentualVendas: 0,
    percentualAcumulado: 0,
  };
}

function getOrCreateMetrics(metricsByProduct, produtoId, data) {
  const existing = metricsByProduct.get(produtoId);
  if (existing) {
    return existing;
  }

  const metrics = createEmptyMetrics(produtoId, data);
  metricsByProduct.set(produtoId, metrics);
  return metrics;
}

function mergeProductIdentity(metrics, data) {
  metrics.produtoNome = firstString(data, ["produto_nome", "nome", "descricao"], metrics.produtoNome);
  metrics.categoria = firstString(data, ["categoria", "departamento"], metrics.categoria);
  metrics.custoUnitario = firstNumber(data, ["custo_unitario", "preco_compra", "preco"], metrics.custoUnitario);
}

function applyCalculations(metricsList) {
  const totalVendasGeral = sum(metricsList, (item) => item.totalVendido);
  const sortedBySales = [...metricsList].sort((a, b) => b.totalVendido - a.totalVendido);
  let acumulado = 0;

  sortedBySales.forEach((item, index) => {
    item.ranking = index + 1;
    item.percentualVendas = totalVendasGeral > 0 ? (item.totalVendido / totalVendasGeral) * 100 : 0;
    acumulado += item.percentualVendas;
    item.percentualAcumulado = acumulado;
    item.abcClass = acumulado <= 80 ? "A" : acumulado <= 95 ? "B" : "C";
  });

  for (const item of metricsList) {
    item.mediaVendaDia = item.quantidadeVendida / WINDOW_DAYS;
    item.diasCobertura = item.mediaVendaDia > 0 ? item.estoqueAtual / item.mediaVendaDia : null;

    const alvoEstoque = item.estoqueDesejado > 0 ?
      item.estoqueDesejado :
      item.mediaVendaDia * (item.diasCoberturaAlvo + item.diasSeguranca);

    item.quantidadeSugerida = Math.max(0, Math.ceil(alvoEstoque - item.estoqueAtual));
    item.investimentoSugerido = item.quantidadeSugerida * item.custoUnitario;
    item.risco = calculateRisk(item);
    item.prioridade = calculatePriority(item);
  }
}

function calculateRisk(item) {
  if (item.mediaVendaDia <= 0) {
    return "baixo";
  }

  if (item.diasCobertura !== null && item.diasCobertura <= item.diasSeguranca) {
    return "alto";
  }

  if (item.diasCobertura !== null && item.diasCobertura <= item.diasSeguranca * 2) {
    return "medio";
  }

  return "baixo";
}

function calculatePriority(item) {
  if (item.risco === "alto" || (item.abcClass === "A" && item.quantidadeSugerida > 0)) {
    return "alta";
  }

  if (item.risco === "medio" || item.quantidadeSugerida > 0) {
    return "media";
  }

  return "baixa";
}

function buildAlerts(metricsList) {
  const alerts = [];

  for (const item of metricsList) {
    if (item.risco === "alto") {
      alerts.push({
        id: `${safeDocId(item.produtoId)}_ruptura`,
        tipo: "ruptura",
        produto_id: item.produtoId,
        produto_nome: item.produtoNome,
        categoria: item.categoria,
        prioridade: "alta",
        estoque_atual: round(item.estoqueAtual),
        dias_cobertura: item.diasCobertura === null ? null : round(item.diasCobertura),
        status: "pendente",
        criado_em: admin.firestore.FieldValue.serverTimestamp(),
      });
    }

    if (item.mediaVendaDia > 0 && item.diasCobertura !== null && item.diasCobertura > item.diasCoberturaAlvo * 2) {
      alerts.push({
        id: `${safeDocId(item.produtoId)}_excesso`,
        tipo: "excesso_estoque",
        produto_id: item.produtoId,
        produto_nome: item.produtoNome,
        categoria: item.categoria,
        prioridade: "media",
        estoque_atual: round(item.estoqueAtual),
        dias_cobertura: round(item.diasCobertura),
        status: "pendente",
        criado_em: admin.firestore.FieldValue.serverTimestamp(),
      });
    }
  }

  return alerts;
}

async function replaceCollection(collectionName, rows, mapper) {
  const collectionRef = db.collection(collectionName);
  const existing = await collectionRef.get();
  const writer = new BatchWriter(db);

  for (const doc of existing.docs) {
    await writer.delete(doc.ref);
  }

  for (const row of rows) {
    const rawId = row.id || row.produtoId || cryptoSafeId();
    await writer.set(collectionRef.doc(safeDocId(rawId)), {
      ...mapper(row),
      atualizado_em: admin.firestore.FieldValue.serverTimestamp(),
    });
  }

  await writer.commit();
}

function toRankingDoc(item) {
  return {
    produto_id: item.produtoId,
    produto_nome: item.produtoNome,
    categoria: item.categoria,
    quantidade_vendida: round(item.quantidadeVendida),
    total_vendido: round(item.totalVendido),
    total_vendido_formatado: formatCurrency(item.totalVendido),
    ranking: item.ranking,
    classe: item.abcClass,
  };
}

function toCurvaAbcDoc(item) {
  return {
    produto_id: item.produtoId,
    produto_nome: item.produtoNome,
    categoria: item.categoria,
    total_vendido: round(item.totalVendido),
    percentual_vendas: round(item.percentualVendas),
    percentual_acumulado: round(item.percentualAcumulado),
    classe: item.abcClass,
  };
}

function toRupturaDoc(item) {
  return {
    produto_id: item.produtoId,
    produto_nome: item.produtoNome,
    categoria: item.categoria,
    estoque_atual: round(item.estoqueAtual),
    media_venda_dia: round(item.mediaVendaDia),
    dias_cobertura: item.diasCobertura === null ? null : round(item.diasCobertura),
    risco: item.risco,
    prioridade: item.prioridade,
  };
}

function toSugestaoCompraDoc(item) {
  return {
    produto_id: item.produtoId,
    produto_nome: item.produtoNome,
    categoria: item.categoria,
    estoque_atual: round(item.estoqueAtual),
    estoque_desejado: round(item.estoqueDesejado),
    media_venda_dia: round(item.mediaVendaDia),
    dias_cobertura_alvo: round(item.diasCoberturaAlvo),
    dias_seguranca: round(item.diasSeguranca),
    dias_cobertura: item.diasCobertura === null ? null : round(item.diasCobertura),
    quantidade_vendida: round(item.quantidadeVendida),
    quantidade_sugerida: round(item.quantidadeSugerida),
    investimento_sugerido: round(item.investimentoSugerido),
    prioridade: item.prioridade,
  };
}

function toProdutoMortoDoc(item) {
  return {
    produto_id: item.produtoId,
    produto_nome: item.produtoNome,
    categoria: item.categoria,
    estoque_atual: round(item.estoqueAtual),
    quantidade_vendida: round(item.quantidadeVendida),
    total_vendido: round(item.totalVendido),
    prioridade: item.estoqueAtual > 20 ? "alta" : "media",
  };
}

class BatchWriter {
  constructor(firestore) {
    this.firestore = firestore;
    this.batch = this.firestore.batch();
    this.count = 0;
  }

  async set(ref, data) {
    this.batch.set(ref, data, {merge: true});
    this.count += 1;
    await this.flushIfNeeded();
  }

  async delete(ref) {
    this.batch.delete(ref);
    this.count += 1;
    await this.flushIfNeeded();
  }

  async commit() {
    if (this.count > 0) {
      await this.batch.commit();
      this.batch = this.firestore.batch();
      this.count = 0;
    }
  }

  async flushIfNeeded() {
    if (this.count >= 450) {
      await this.commit();
    }
  }
}

function normalizeId(data, fallback) {
  return firstString(data, ["produto_id", "id", "sku", "codigo"], fallback);
}

function firstString(data, keys, fallback = "") {
  for (const key of keys) {
    const value = data[key];
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      return String(value).trim();
    }
  }
  return fallback;
}

function firstNumber(data, keys, fallback = 0) {
  for (const key of keys) {
    const value = data[key];
    const parsed = asNumber(value);
    if (parsed !== null) {
      return parsed;
    }
  }
  return fallback;
}

function asNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string") {
    const normalized = value
      .replace("R$", "")
      .replace(/\./g, "")
      .replace(",", ".")
      .trim();
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : null;
  }

  return null;
}

function isWithinWindow(data, cutoff) {
  const rawDate = data.data || data.data_venda || data.criado_em || data.created_at;
  if (!rawDate) {
    return true;
  }

  let date = null;

  if (typeof rawDate.toDate === "function") {
    date = rawDate.toDate();
  } else if (typeof rawDate === "string" || typeof rawDate === "number") {
    const parsed = new Date(rawDate);
    date = Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  return date ? date >= cutoff : true;
}

function safeDocId(value) {
  return String(value).replace(/[\/#[\]?]/g, "_").slice(0, 1400) || cryptoSafeId();
}

function cryptoSafeId() {
  return `doc_${Date.now()}_${Math.random().toString(36).slice(2)}`;
}

function sum(rows, selector) {
  return rows.reduce((total, row) => total + selector(row), 0);
}

function average(rows, selector) {
  return rows.length === 0 ? 0 : sum(rows, selector) / rows.length;
}

function round(value) {
  return Math.round((value + Number.EPSILON) * 100) / 100;
}

function formatCurrency(value) {
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
  }).format(value);
}

const port = Number(process.env.PORT || 3000);
app.listen(port, () => {
  console.log(`SugestionDataDriven backend rodando na porta ${port}`);
});


