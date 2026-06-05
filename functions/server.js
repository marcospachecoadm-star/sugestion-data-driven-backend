try {
  require("dotenv").config();
} catch (error) {
  if (error.code !== "MODULE_NOT_FOUND") {
    throw error;
  }
}

const cors = require("cors");
const express = require("express");
const admin = require("firebase-admin");
const csv = require("csv-parser");
const fs = require("fs");
const os = require("os");
const path = require("path");

const app = express();
app.use(cors());
app.use(express.json({limit: "2mb"}));

const WINDOW_DAYS = 45;
const TARGET_COVERAGE_DAYS = Number(process.env.TARGET_COVERAGE_DAYS || 30);
const SAFETY_STOCK_DAYS = Number(process.env.SAFETY_STOCK_DAYS || 7);
const CRITICAL_COVERAGE_DAYS = Number(process.env.CRITICAL_COVERAGE_DAYS || 7);
const WARNING_COVERAGE_DAYS = Number(process.env.WARNING_COVERAGE_DAYS || 15);
const OSA_TARGET_PERCENT = Number(process.env.OSA_TARGET_PERCENT || 97);
const DEFAULT_STORAGE_BUCKET = "datadriven-4816c.firebasestorage.app";
const REQUIRE_API_KEY = process.env.REQUIRE_API_KEY !== "false";
const ALLOW_API_KEY_QUERY = process.env.ALLOW_API_KEY_QUERY === "true";
const ALLOW_GLOBAL_JOBS = process.env.ALLOW_GLOBAL_JOBS === "true";
const MAX_UPLOAD_FILES_PER_RUN = Number(process.env.MAX_UPLOAD_FILES_PER_RUN || 10);
const MAX_CSV_ROWS_PER_FILE = Number(process.env.MAX_CSV_ROWS_PER_FILE || 5000);
const MAX_PRODUCTS_PER_ANALYTICS = Number(process.env.MAX_PRODUCTS_PER_ANALYTICS || 5000);
const MAX_STOCK_ROWS_PER_ANALYTICS = Number(process.env.MAX_STOCK_ROWS_PER_ANALYTICS || 5000);
const MAX_SALES_ROWS_PER_ANALYTICS = Number(process.env.MAX_SALES_ROWS_PER_ANALYTICS || 20000);
const MAX_ITEMS_PER_INDICATOR = Number(process.env.MAX_ITEMS_PER_INDICATOR || 250);
const OUTLIER_STD_DEV_FACTOR = Number(process.env.OUTLIER_STD_DEV_FACTOR || 3);
const OUTLIER_MEDIAN_FACTOR = Number(process.env.OUTLIER_MEDIAN_FACTOR || 3);
const TREND_MIN_FACTOR = Number(process.env.TREND_MIN_FACTOR || 0.75);
const TREND_MAX_FACTOR = Number(process.env.TREND_MAX_FACTOR || 1.4);
const TREND_CONFIRMATION_THRESHOLD = Number(process.env.TREND_CONFIRMATION_THRESHOLD || 1.15);

const PROFILE_LABELS = {
  admin: "Admin",
  gestor: "Gestor",
  compras: "Compras",
  consulta: "Consulta",
};

const DEFAULT_EMPRESA_LABELS = {
  superparanaloja1: "SuperParana Loja 1",
};

const DEFAULT_LOJA_LABELS = {
  superparanaloja1: "Matriz Centro Curitiba",
};

const DEFAULT_CATEGORIA_LABELS = {
  mercearia: "Mercearia",
  bazar: "Bazar",
  pereciveis: "Pereciveis",
  produtos_nao_vendaveis: "Produtos nao vendaveis",
  gastronomia: "Gastronomia",
};

const RAW_COLLECTIONS = {
  produtos: "produtos",
  estoque: "estoque",
  vendas: "vendas",
};

const OUTPUT_COLLECTIONS = {
  indicadoresResumo: "indicadoresResumo",
  indicadoresItens: "indicadoresItens",
  alertas: "alertas",
  acoesRecomendadas: "acoesRecomendadas",
  sugestoesCompra: "sugestoesCompra",
  historicoProcessamentos: "historicoProcessamentos",
};

const PRODUCT_ID_KEYS = [
  "produto_id",
  "id",
  "sku",
  "codigo",
  "cod_produto",
  "codproduto",
  "cod_prod",
  "codprod",
  "cod_item",
  "coditem",
  "ean",
  "gtin",
];
const PRODUCT_NAME_KEYS = ["produto_nome", "nome", "descricao", "descricao_produto", "produto", "item"];
const CATEGORY_KEYS = ["categoria", "departamento", "grupo"];
const SUPPLIER_KEYS = ["fornecedor", "distribuidor", "supplier"];
const BRAND_KEYS = ["marca", "brand"];
const STOCK_QUANTITY_KEYS = [
  "estoque_atual",
  "quantidade_estoque",
  "quantidade",
  "qtd",
  "qtde",
  "saldo",
  "estoque",
  "estoque_disponivel",
  "estoque_total",
];
const MIN_STOCK_KEYS = ["estoque_minimo", "minimo", "estoqueMinimo", "min"];
const SALES_QUANTITY_KEYS = ["quantidade_vendida", "quantidade", "qtd", "qtde", "qty", "qtd_vendida", "qtde_vendida"];
const SALES_TOTAL_KEYS = [
  "total_vendido",
  "valor_total",
  "total",
  "receita",
  "valor",
  "valor_venda",
  "preco_total",
  "vl_total",
  "vlr_total",
  "valor_liquido",
  "valor_bruto",
  "total_item",
  "valor_item",
  "subtotal",
];
const UNIT_PRICE_KEYS = ["preco_unitario", "valor_unitario", "preco", "valor_produto", "preco_venda", "preco_de_venda"];
const UNIT_COST_KEYS = ["custo_unitario", "preco_compra", "preco_custo", "custo", "preco", "preco_de_custo"];
const DATE_KEYS = ["data", "data_venda", "criado_em", "created_at", "ultima_venda_em"];

let firestoreDb = null;
const db = new Proxy({}, {
  get(_target, property) {
    return getDb()[property];
  },
});

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    service: "Estoqueia Data Driven Backend",
    methodology: "Nielsen OSA 45 dias",
    routes: [
      "/health",
      "/debug-storage",
      "/import-storage-csv",
      "/import-and-run",
      "/run-analytics",
      "/indicadores/itens",
      "/search-indicadores",
    ],
  });
});

app.get("/health", (_req, res) => {
  res.json({ok: true, status: "online"});
});

app.get("/debug-storage", requireApiKey, async (req, res) => {
  try {
    const empresaId = getEmpresaIdFromRequest(req);
    assertTenantScope(empresaId);
    const prefix = `uploads/${empresaId}/`;
    const [files] = await getStorageBucket().getFiles({prefix, maxResults: MAX_UPLOAD_FILES_PER_RUN});

    res.json({
      ok: true,
      bucket: getStorageBucket().name,
      prefix,
      total: files.length,
      files: files.map((file) => file.name),
    });
  } catch (error) {
    sendError(res, error);
  }
});

app.get("/import-storage-csv", requireApiKey, async (req, res) => {
  await handleImport(req, res, false);
});

app.post("/import-storage-csv", requireApiKey, async (req, res) => {
  await handleImport(req, res, false);
});

app.get("/import-and-run", requireApiKey, async (req, res) => {
  await handleImport(req, res, true);
});

app.post("/import-and-run", requireApiKey, async (req, res) => {
  await handleImport(req, res, true);
});

app.get("/run-analytics", requireApiKey, async (req, res) => {
  await handleAnalytics(req, res);
});

app.post("/run-analytics", requireApiKey, async (req, res) => {
  await handleAnalytics(req, res);
});

app.get("/indicadores/itens", requireApiKey, async (req, res) => {
  await handleIndicatorItemsSearch(req, res);
});

app.get("/search-indicadores", requireApiKey, async (req, res) => {
  await handleIndicatorItemsSearch(req, res);
});

app.post("/admin/usuarios", requireApiKey, async (req, res) => {
  await handleAdminCreateUser(req, res);
});

app.patch("/admin/usuarios/:uid", requireApiKey, async (req, res) => {
  await handleAdminUpdateUser(req, res);
});

app.patch("/admin/usuarios/:uid/status", requireApiKey, async (req, res) => {
  await handleAdminUpdateUserStatus(req, res);
});

async function handleImport(req, res, shouldRunAnalytics) {
  try {
    const requestedEmpresaId = getEmpresaIdFromRequest(req);
    assertTenantScope(requestedEmpresaId);
    const importacao = await processPendingUploads(requestedEmpresaId);
    const empresas = requestedEmpresaId ?
      [requestedEmpresaId] :
      [...new Set(importacao.resultados.filter((item) => item.empresaId).map((item) => item.empresaId))];
    const analytics = [];

    if (shouldRunAnalytics) {
      for (const empresaId of empresas) {
        analytics.push(await rebuildAnalytics(empresaId));
      }
    }

    res.json({ok: true, importacao, analytics});
  } catch (error) {
    sendError(res, error);
  }
}

async function handleAnalytics(req, res) {
  try {
    const empresaId = getEmpresaIdFromRequest(req);
    assertTenantScope(empresaId);
    const summary = await rebuildAnalytics(empresaId);
    res.json({ok: true, summary});
  } catch (error) {
    sendError(res, error);
  }
}

async function handleIndicatorItemsSearch(req, res) {
  try {
    const empresaId = getEmpresaIdFromRequest(req);
    assertTenantScope(empresaId);

    const indicadorTipo = String(req.query.indicadorTipo || req.query.indicador_tipo || "").trim();
    if (!indicadorTipo) {
      const error = new Error("indicadorTipo obrigatorio. Exemplo: ?indicadorTipo=giro_medio");
      error.statusCode = 400;
      throw error;
    }

    const limit = getSearchLimitFromRequest(req);
    const rawSearch = String(req.query.q || req.query.busca || req.query.search || "").trim();
    const searchTokens = isSearchAllQuery(rawSearch) ? [] : getQuerySearchTokens(rawSearch);

    const indicatorCollection = db.collection(OUTPUT_COLLECTIONS.indicadoresItens);
    let query = indicatorCollection
      .where("empresa_id", "==", empresaId)
      .where("indicador_tipo", "==", indicadorTipo);

    if (searchTokens.length === 1) {
      query = query.where("busca_tokens", "array-contains", searchTokens[0]);
    } else if (searchTokens.length > 1) {
      query = query.where("busca_tokens", "array-contains-any", searchTokens);
    } else if (indicadorTipo === "giro_medio") {
      query = query.orderBy("ranking", "asc").limit(limit);
    } else {
      query = query.orderBy("abc_prioridade", "asc").orderBy("prioridade_score", "desc").limit(limit);
    }

    let snapshot;
    try {
      snapshot = await query.get();
    } catch (error) {
      if (searchTokens.length === 0 && isFirestoreIndexError(error)) {
        const fallbackQuery = indicatorCollection
          .where("empresa_id", "==", empresaId)
          .where("indicador_tipo", "==", indicadorTipo);
        snapshot = await fallbackQuery.get();
      } else {
        throw error;
      }
    }

    const items = snapshot.docs
      .map((doc) => ({
        id: doc.id,
        ...doc.data(),
      }))
      .sort(compareApiIndicatorItem)
      .slice(0, limit);

    res.json({
      ok: true,
      empresaId,
      indicadorTipo,
      q: rawSearch,
      tokens: searchTokens,
      total: items.length,
      items,
    });
  } catch (error) {
    sendError(res, error);
  }
}

async function handleAdminCreateUser(req, res) {
  try {
    const payload = normalizeAdminUserPayload(req.body || {}, {isCreate: true});

    initializeFirebase();

    let userRecord;
    let createdAuthUser = false;

    try {
      userRecord = await admin.auth().getUserByEmail(payload.email);
      const authUpdate = {
        displayName: payload.nome,
        disabled: !payload.ativo,
      };

      if (payload.senha) {
        authUpdate.password = payload.senha;
      }

      userRecord = await admin.auth().updateUser(userRecord.uid, authUpdate);
    } catch (error) {
      if (error.code !== "auth/user-not-found") {
        throw error;
      }

      userRecord = await admin.auth().createUser({
        email: payload.email,
        password: payload.senha,
        displayName: payload.nome,
        disabled: !payload.ativo,
      });
      createdAuthUser = true;
    }

    await saveAdminUserAccess(userRecord.uid, payload, {merge: true});

    res.status(createdAuthUser ? 201 : 200).json({
      ok: true,
      createdAuthUser,
      uid: userRecord.uid,
      usuario: publicAdminUserResponse(userRecord.uid, payload),
    });
  } catch (error) {
    sendError(res, error);
  }
}

async function handleAdminUpdateUser(req, res) {
  try {
    const uid = String(req.params.uid || "").trim();
    if (!uid) {
      const error = new Error("uid obrigatorio.");
      error.statusCode = 400;
      throw error;
    }

    const payload = normalizeAdminUserPayload(req.body || {}, {isCreate: false});
    initializeFirebase();

    const authUpdate = {};
    if (payload.nome) {
      authUpdate.displayName = payload.nome;
    }
    if (typeof payload.ativo === "boolean") {
      authUpdate.disabled = !payload.ativo;
    }
    if (payload.senha) {
      authUpdate.password = payload.senha;
    }

    let userRecord = await admin.auth().getUser(uid);
    if (Object.keys(authUpdate).length > 0) {
      userRecord = await admin.auth().updateUser(uid, authUpdate);
    }

    const existingDoc = await db.collection("usuarios").doc(uid).get();
    const existingData = existingDoc.exists ? existingDoc.data() : {};
    const mergedPayload = {
      nome: payload.nome || existingData.nome || userRecord.displayName || "",
      email: existingData.email || userRecord.email || payload.email || "",
      perfil: payload.perfil || existingData.perfil || existingData.role || "consulta",
      empresa_id: payload.empresa_id || existingData.empresa_id || existingData.empresaId || null,
      empresa_nome: payload.empresa_nome || existingData.empresa_nome || null,
      lojas_ids: payload.lojas_ids || existingData.lojas_ids || [],
      lojas_nomes: payload.lojas_nomes || existingData.lojas_nomes || [],
      categorias_ids: payload.categorias_ids || existingData.categorias_ids || existingData.categoria_ids || [],
      categorias_nomes: payload.categorias_nomes || existingData.categorias_nomes || [],
      ativo: typeof payload.ativo === "boolean" ? payload.ativo : existingData.ativo !== false,
    };
    mergedPayload.role = mergedPayload.perfil;

    await saveAdminUserAccess(uid, mergedPayload, {merge: true});

    res.json({
      ok: true,
      uid,
      usuario: publicAdminUserResponse(uid, mergedPayload),
    });
  } catch (error) {
    sendError(res, error);
  }
}

async function handleAdminUpdateUserStatus(req, res) {
  try {
    const uid = String(req.params.uid || "").trim();
    if (!uid) {
      const error = new Error("uid obrigatorio.");
      error.statusCode = 400;
      throw error;
    }

    const docRef = db.collection("usuarios").doc(uid);
    const snapshot = await docRef.get();
    const currentData = snapshot.exists ? snapshot.data() : {};
    const ativo = typeof req.body.ativo === "boolean" ? req.body.ativo : currentData.ativo === false;

    initializeFirebase();
    await admin.auth().updateUser(uid, {disabled: !ativo});
    await docRef.set({
      ativo,
      status_texto: getStatusLabel(ativo),
      status_color: ativo ? "#166534" : "#991B1B",
      status_bg_color: ativo ? "#DCFCE7" : "#FEE2E2",
      atualizado_em: admin.firestore.FieldValue.serverTimestamp(),
    }, {merge: true});

    res.json({ok: true, uid, ativo});
  } catch (error) {
    sendError(res, error);
  }
}

async function saveAdminUserAccess(uid, payload, options = {merge: true}) {
  const perfil = payload.perfil || payload.role || "consulta";
  const lojasIds = normalizeStringArray(payload.lojas_ids);
  const categoriasIds = normalizeStringArray(payload.categorias_ids || payload.categoria_ids);
  const lojasNomes = normalizeDisplayArray(payload.lojas_nomes, lojasIds, DEFAULT_LOJA_LABELS);
  const categoriasNomes = normalizeDisplayArray(payload.categorias_nomes, categoriasIds, DEFAULT_CATEGORIA_LABELS);
  const ativo = payload.ativo !== false;

  const cleanPayload = {
    uid,
    nome: payload.nome,
    email: payload.email,
    perfil,
    perfil_label: getProfileLabel(perfil),
    role: perfil,
    empresa_id: payload.empresa_id,
    empresa_nome: payload.empresa_nome || DEFAULT_EMPRESA_LABELS[payload.empresa_id] || payload.empresa_id,
    lojas_ids: lojasIds,
    lojas_nomes: lojasNomes,
    lojas_texto: lojasNomes.join(", "),
    categorias_ids: categoriasIds,
    categoria_ids: categoriasIds,
    categorias_nomes: categoriasNomes,
    categorias_texto: categoriasNomes.join(", "),
    ativo,
    status_texto: getStatusLabel(ativo),
    status_color: ativo ? "#166534" : "#991B1B",
    status_bg_color: ativo ? "#DCFCE7" : "#FEE2E2",
    atualizado_em: admin.firestore.FieldValue.serverTimestamp(),
  };

  await admin.auth().setCustomUserClaims(uid, {
    empresa_id: cleanPayload.empresa_id,
    role: cleanPayload.role,
    admin: cleanPayload.role === "admin",
  });

  await db.collection("usuarios").doc(uid).set(cleanPayload, options);
}

function publicAdminUserResponse(uid, payload) {
  const perfil = payload.perfil || payload.role || "consulta";
  const lojasIds = normalizeStringArray(payload.lojas_ids);
  const categoriasIds = normalizeStringArray(payload.categorias_ids || payload.categoria_ids);
  const lojasNomes = normalizeDisplayArray(payload.lojas_nomes, lojasIds, DEFAULT_LOJA_LABELS);
  const categoriasNomes = normalizeDisplayArray(payload.categorias_nomes, categoriasIds, DEFAULT_CATEGORIA_LABELS);
  const ativo = payload.ativo !== false;

  return {
    uid,
    nome: payload.nome,
    email: payload.email,
    perfil,
    perfil_label: getProfileLabel(perfil),
    role: perfil,
    empresa_id: payload.empresa_id,
    empresa_nome: payload.empresa_nome || DEFAULT_EMPRESA_LABELS[payload.empresa_id] || payload.empresa_id,
    lojas_ids: lojasIds,
    lojas_nomes: lojasNomes,
    lojas_texto: lojasNomes.join(", "),
    categorias_ids: categoriasIds,
    categoria_ids: categoriasIds,
    categorias_nomes: categoriasNomes,
    categorias_texto: categoriasNomes.join(", "),
    ativo,
    status_texto: getStatusLabel(ativo),
    status_color: ativo ? "#166534" : "#991B1B",
    status_bg_color: ativo ? "#DCFCE7" : "#FEE2E2",
  };
}

function normalizeAdminUserPayload(body, {isCreate}) {
  const perfil = stringOrNull(body.perfil || body.role);
  const payload = {
    nome: stringOrNull(body.nome || body.name || body.displayName),
    email: stringOrNull(body.email),
    senha: stringOrNull(body.senha || body.password),
    perfil,
    empresa_id: stringOrNull(body.empresa_id || body.empresaId || body.tenant_id),
    empresa_nome: stringOrNull(body.empresa_nome || body.empresaNome || body.company || body.empresa),
    lojas_ids: normalizeStringArray(body.lojas_ids || body.lojasIds || body.lojas || body.loja_id || body.lojaId),
    lojas_nomes: normalizeStringArray(body.lojas_nomes || body.lojasNomes || body.stores || body.lojas_texto),
    categorias_ids: normalizeStringArray(
      body.categorias_ids || body.categoria_ids || body.categoriasIds || body.categoriaIds ||
        body.categorias || body.categoria_id || body.categoriaId,
    ),
    categorias_nomes: normalizeStringArray(
      body.categorias_nomes || body.categoriasNomes || body.categorias_texto || body.categories,
    ),
    ativo: typeof body.ativo === "boolean" ? body.ativo : undefined,
  };

  payload.role = payload.perfil;

  if (isCreate) {
    requireStringField(payload.nome, "nome");
    requireStringField(payload.email, "email");
    requireStringField(payload.senha, "senha");
    requireStringField(payload.perfil, "perfil");
    requireStringField(payload.empresa_id, "empresa_id");
    requireArrayField(payload.lojas_ids, "lojas_ids");
    requireArrayField(payload.categorias_ids, "categorias_ids");
    requireBooleanField(payload.ativo, "ativo");
  } else {
    requireStringField(payload.nome, "nome");
    requireStringField(payload.perfil, "perfil");
    requireStringField(payload.empresa_id, "empresa_id");
    requireArrayField(payload.lojas_ids, "lojas_ids");
    requireArrayField(payload.categorias_ids, "categorias_ids");
    requireBooleanField(payload.ativo, "ativo");
  }

  if (payload.email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(payload.email)) {
    const error = new Error("email invalido.");
    error.statusCode = 400;
    throw error;
  }

  if (payload.senha && payload.senha.length < 6) {
    const error = new Error("senha deve ter pelo menos 6 caracteres.");
    error.statusCode = 400;
    throw error;
  }

  const allowedRoles = new Set(["admin", "gestor", "compras", "consulta"]);
  if (payload.perfil && !allowedRoles.has(payload.perfil)) {
    const error = new Error("perfil invalido. Use admin, gestor, compras ou consulta.");
    error.statusCode = 400;
    throw error;
  }

  return payload;
}

function requireStringField(value, fieldName) {
  if (!value) {
    const error = new Error(`${fieldName} obrigatorio.`);
    error.statusCode = 400;
    throw error;
  }
}

function requireArrayField(value, fieldName) {
  if (!Array.isArray(value) || value.length === 0) {
    const error = new Error(`${fieldName} obrigatorio.`);
    error.statusCode = 400;
    throw error;
  }
}

function requireBooleanField(value, fieldName) {
  if (typeof value !== "boolean") {
    const error = new Error(`${fieldName} obrigatorio.`);
    error.statusCode = 400;
    throw error;
  }
}

function stringOrNull(value) {
  if (value === undefined || value === null) {
    return null;
  }

  const text = String(value).trim();
  return text || null;
}

function normalizeStringArray(value) {
  if (value === undefined || value === null || value === "") {
    return [];
  }

  if (Array.isArray(value)) {
    return value.map((item) => String(item).trim()).filter(Boolean);
  }

  return String(value)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeDisplayArray(value, ids, labelsById) {
  const explicitValues = normalizeStringArray(value);
  if (explicitValues.length > 0) {
    return explicitValues;
  }

  return normalizeStringArray(ids).map((id) => labelsById[id] || id);
}

function getProfileLabel(value) {
  const profile = String(value || "consulta").trim().toLowerCase();
  return PROFILE_LABELS[profile] || profile.charAt(0).toUpperCase() + profile.slice(1);
}

function getStatusLabel(ativo) {
  return ativo ? "Ativo" : "Inativo";
}

async function processPendingUploads(empresaId = null) {
  assertTenantScope(empresaId);
  const bucket = getStorageBucket();
  const prefix = `uploads/${empresaId}/`;
  const [files] = await bucket.getFiles({prefix, maxResults: MAX_UPLOAD_FILES_PER_RUN});
  const resultados = [];

  for (const file of files) {
    if (file.name.endsWith("/") || !file.name.toLowerCase().endsWith(".csv")) {
      continue;
    }

    resultados.push(await processCsvFile(bucket, file.name));
  }

  return {total: resultados.length, resultados};
}

async function processCsvFile(bucket, filePath) {
  const fileName = path.basename(filePath);
  const tempFilePath = path.join(os.tmpdir(), `${Date.now()}_${fileName}`);
  let errorPath = `erro/${fileName}`;

  try {
    const fileInfo = identifyFile(filePath);
    const collectionName = collectionForFileType(fileInfo.tipoArquivo);
    const processedPath = `processada/${fileInfo.empresaId}/${fileInfo.fileName}`;
    errorPath = `erro/${fileInfo.empresaId}/${fileInfo.fileName}`;

    await bucket.file(filePath).download({destination: tempFilePath});

    const rows = await readCsv(tempFilePath, fileInfo.empresaId);
    await saveRows(collectionName, rows);
    await bucket.file(filePath).move(processedPath);

    return {
      status: "processado",
      empresaId: fileInfo.empresaId,
      filePath,
      destino: processedPath,
      colecao: collectionName,
      linhas: rows.length,
    };
  } catch (error) {
    try {
      await bucket.file(filePath).move(errorPath);
    } catch (moveError) {
      console.error("Erro ao mover CSV para erro:", moveError);
    }

    return {
      status: "erro",
      filePath,
      destino: errorPath,
      erro: error.message,
    };
  } finally {
    if (fs.existsSync(tempFilePath)) {
      fs.unlinkSync(tempFilePath);
    }
  }
}

function identifyFile(filePath) {
  if (!filePath || !filePath.startsWith("uploads/")) {
    throw new Error(`Arquivo fora de uploads/: ${filePath}`);
  }

  const parts = filePath.split("/");
  const fileName = path.basename(filePath);
  const nameWithoutExtension = path.basename(fileName, ".csv");
  const match = nameWithoutExtension.match(/^(.+)_(vendas|estoque|produto|produtos)_\d{2}_\d{2}_\d{4}$/i);

  if (match) {
    return {empresaId: match[1], tipoArquivo: match[2].toLowerCase(), fileName};
  }

  if (parts.length >= 3) {
    return {empresaId: parts[1], tipoArquivo: nameWithoutExtension.toLowerCase(), fileName};
  }

  throw new Error(`Nome de arquivo invalido: ${fileName}`);
}

function collectionForFileType(fileType) {
  if (fileType === "produto" || fileType === "produtos") {
    return RAW_COLLECTIONS.produtos;
  }

  if (fileType === "vendas") {
    return RAW_COLLECTIONS.vendas;
  }

  if (fileType === "estoque") {
    return RAW_COLLECTIONS.estoque;
  }

  throw new Error(`Tipo de arquivo invalido: ${fileType}`);
}

function readCsv(tempFilePath, empresaId) {
  return new Promise((resolve, reject) => {
    const rows = [];
    let rejected = false;

    fs.createReadStream(tempFilePath)
      .pipe(csv({
        separator: ",",
        mapHeaders: ({header}) => normalizeHeader(header),
        mapValues: ({value}) => typeof value === "string" ? value.trim() : value,
      }))
      .on("data", (data) => {
        if (rows.length >= MAX_CSV_ROWS_PER_FILE) {
          if (!rejected) {
            rejected = true;
            reject(new Error(`CSV excede o limite de ${MAX_CSV_ROWS_PER_FILE} linhas por arquivo.`));
          }
          return;
        }

        const row = {};

        for (const originalKey in data) {
          const key = normalizeHeader(originalKey);
          row[key] = parseCsvValue(key, data[originalKey]);
        }

        row.empresa_id = empresaId;
        rows.push(row);
      })
      .on("end", () => {
        if (!rejected) {
          resolve(rows);
        }
      })
      .on("error", (error) => {
        if (!rejected) {
          rejected = true;
          reject(error);
        }
      });
  });
}

async function saveRows(collectionName, rows) {
  const writer = new BatchWriter(db);

  for (const row of rows) {
    const rawId = getRawDocumentId(collectionName, row);
    const docId = `${safeDocId(row.empresa_id)}_${safeDocId(rawId)}`;
    await writer.set(db.collection(collectionName).doc(docId), row);
  }

  await writer.commit();
}

function getRawDocumentId(collectionName, row) {
  if (collectionName === RAW_COLLECTIONS.vendas) {
    return row.venda_id || row.id || cryptoSafeId();
  }

  return firstString(row, PRODUCT_ID_KEYS, cryptoSafeId());
}

async function rebuildAnalytics(empresaId = null) {
  assertTenantScope(empresaId);
  const [productsSnap, stockSnap, salesSnap] = await Promise.all([
    getTenantDocs(RAW_COLLECTIONS.produtos, empresaId),
    getTenantDocs(RAW_COLLECTIONS.estoque, empresaId),
    getTenantDocs(RAW_COLLECTIONS.vendas, empresaId),
  ]);

  assertSnapshotSize(productsSnap, MAX_PRODUCTS_PER_ANALYTICS, RAW_COLLECTIONS.produtos);
  assertSnapshotSize(stockSnap, MAX_STOCK_ROWS_PER_ANALYTICS, RAW_COLLECTIONS.estoque);
  assertSnapshotSize(salesSnap, MAX_SALES_ROWS_PER_ANALYTICS, RAW_COLLECTIONS.vendas);

  const metricsByProduct = new Map();

  for (const doc of productsSnap.docs) {
    const data = normalizeRecordKeys(doc.data());
    const productId = normalizeProductId(firstString(data, PRODUCT_ID_KEYS, doc.id));
    metricsByProduct.set(productId, createMetrics(productId, data));
  }

  for (const doc of stockSnap.docs) {
    const data = normalizeRecordKeys(doc.data());
    const productId = normalizeProductId(firstString(data, PRODUCT_ID_KEYS, doc.id));
    const metrics = getOrCreateMetrics(metricsByProduct, productId, data);

    metrics.estoqueAtual = firstNumber(data, STOCK_QUANTITY_KEYS, metrics.estoqueAtual);
    metrics.estoqueMinimo = firstNumber(data, MIN_STOCK_KEYS, metrics.estoqueMinimo);
    mergeProductIdentity(metrics, data);
  }

  const analysisEndDate = getLatestSalesDate(salesSnap.docs) || new Date();
  analysisEndDate.setHours(0, 0, 0, 0);
  const cutoffDate = new Date(analysisEndDate);
  cutoffDate.setDate(cutoffDate.getDate() - WINDOW_DAYS + 1);

  let vendasProcessadas = 0;

  for (const doc of salesSnap.docs) {
    const data = normalizeRecordKeys(doc.data());
    const saleDate = getRecordDate(data);
    if (saleDate && saleDate < cutoffDate) {
      continue;
    }

    vendasProcessadas += 1;
    const productId = normalizeProductId(firstString(data, PRODUCT_ID_KEYS, doc.id));
    const metrics = getOrCreateMetrics(metricsByProduct, productId, data);
    const quantity = firstNumber(data, SALES_QUANTITY_KEYS, 0);
    const explicitTotal = firstNumber(data, SALES_TOTAL_KEYS, null);
    const unitPrice = firstNumber(data, UNIT_PRICE_KEYS, 0);
    const total = explicitTotal !== null ? explicitTotal : quantity * unitPrice;
    const salesDateKey = formatDateKey(saleDate || analysisEndDate);

    metrics.vendas45d += quantity;
    metrics.receita45d += total;
    metrics.vendasPorDia[salesDateKey] = (metrics.vendasPorDia[salesDateKey] || 0) + quantity;
    metrics.ultimaVendaEm = maxDate(metrics.ultimaVendaEm, saleDate);
    mergeProductIdentity(metrics, data);
  }

  const metricsList = Array.from(metricsByProduct.values());
  calculateNielsenMetrics(metricsList, analysisEndDate);

  const alertas = buildAlerts(metricsList);
  const sugestoesCompra = limitRows(
    metricsList.filter((item) => item.quantidadeSugerida > 0).sort(compareBusinessPriority),
  );
  const acoesRecomendadas = buildRecommendedActions(metricsList);
  const indicadoresItens = buildIndicatorItems(metricsList, alertas, acoesRecomendadas);
  const resumo = buildSummary(empresaId, metricsList, alertas, sugestoesCompra, acoesRecomendadas, vendasProcessadas);
  const runResult = {
    empresaId: empresaId || null,
    periodoDias: WINDOW_DAYS,
    dataFinalAnalise: formatDateKey(analysisEndDate),
    produtosProcessados: metricsList.length,
    vendasProcessadas,
    indicadoresItens: indicadoresItens.length,
    alertas: alertas.length,
    acoesRecomendadas: acoesRecomendadas.length,
    sugestoesCompra: sugestoesCompra.length,
  };

  const docId = empresaId ? `${safeDocId(empresaId)}_dashboard` : "dashboard";
  await Promise.all([
    db.collection(OUTPUT_COLLECTIONS.indicadoresResumo).doc(docId).set(resumo, {merge: true}),
    replaceOutputCollection(OUTPUT_COLLECTIONS.indicadoresItens, indicadoresItens, empresaId),
    replaceOutputCollection(OUTPUT_COLLECTIONS.alertas, alertas, empresaId),
    replaceOutputCollection(OUTPUT_COLLECTIONS.acoesRecomendadas, acoesRecomendadas, empresaId),
    replaceOutputCollection(OUTPUT_COLLECTIONS.sugestoesCompra, sugestoesCompra.map(toPurchaseSuggestionDoc), empresaId),
    saveProcessingHistory(empresaId, resumo, runResult),
  ]);

  return runResult;
}

function createMetrics(productId, data) {
  return {
    produtoId: productId,
    empresaId: firstString(data, ["empresa_id", "empresaId", "tenant_id"], null),
    sku: firstString(data, PRODUCT_ID_KEYS, productId),
    produtoNome: firstString(data, PRODUCT_NAME_KEYS, productId),
    categoria: firstString(data, CATEGORY_KEYS, "Sem categoria"),
    fornecedor: firstString(data, SUPPLIER_KEYS, ""),
    marca: firstString(data, BRAND_KEYS, ""),
    estoqueAtual: firstNumber(data, STOCK_QUANTITY_KEYS, 0),
    estoqueMinimo: firstNumber(data, MIN_STOCK_KEYS, 0),
    estoqueMinimoOriginal: firstNumber(data, MIN_STOCK_KEYS, 0),
    estoqueMinimoCalculado: 0,
    custoUnitario: firstNumber(data, UNIT_COST_KEYS, 0),
    vendas45d: 0,
    vendas15d: 0,
    vendas7d: 0,
    receita45d: 0,
    vendasPorDia: {},
    ultimaVendaEm: getRecordDate(data),
    giroDiario: 0,
    giroDiarioBruto: 0,
    mediaDiariaAjustada45d: 0,
    mediaDiaria15d: 0,
    mediaDiaria7d: 0,
    fatorTendencia: 1,
    outlierDetectado: false,
    diasOutlier: [],
    sazonalidadeDetectada: false,
    coberturaDias: null,
    estoqueAlvo: 0,
    quantidadeSugerida: 0,
    investimentoSugerido: 0,
    valorParado: 0,
    vendaPerdidaEstimada: 0,
    disponibilidadeOsa: 100,
    taxaRupturaSku: 0,
    statusGiro: "sem_giro",
    statusEstoque: "normal",
    prioridade: "baixa",
    prioridadeScore: 0,
    ranking: 0,
    abcClasse: "C",
    percentualReceita: 0,
    percentualAcumulado: 0,
  };
}

function getOrCreateMetrics(metricsByProduct, productId, data) {
  const existing = metricsByProduct.get(productId);
  if (existing) {
    return existing;
  }

  const metrics = createMetrics(productId, data);
  metricsByProduct.set(productId, metrics);
  return metrics;
}

function mergeProductIdentity(metrics, data) {
  metrics.sku = firstString(data, PRODUCT_ID_KEYS, metrics.sku);
  metrics.produtoNome = firstString(data, PRODUCT_NAME_KEYS, metrics.produtoNome);
  metrics.categoria = firstString(data, CATEGORY_KEYS, metrics.categoria);
  metrics.fornecedor = firstString(data, SUPPLIER_KEYS, metrics.fornecedor);
  metrics.marca = firstString(data, BRAND_KEYS, metrics.marca);
  metrics.custoUnitario = firstNumber(data, UNIT_COST_KEYS, metrics.custoUnitario);
  metrics.estoqueMinimo = firstNumber(data, MIN_STOCK_KEYS, metrics.estoqueMinimo);
}

function getLatestSalesDate(salesDocs) {
  let latestDate = null;

  for (const doc of salesDocs) {
    const data = normalizeRecordKeys(doc.data());
    latestDate = maxDate(latestDate, getRecordDate(data));
  }

  return latestDate;
}

function getAnalysisEndDate(metricsList) {
  const latestDate = metricsList.reduce(
    (currentDate, item) => maxDate(currentDate, item.ultimaVendaEm),
    null,
  );

  return latestDate ? formatDateKey(latestDate) : null;
}

function calculateNielsenMetrics(metricsList, analysisEndDate = new Date()) {
  for (const item of metricsList) {
    if (item.receita45d <= 0 && item.vendas45d > 0 && item.custoUnitario > 0) {
      item.receita45d = item.vendas45d * item.custoUnitario;
    }
  }

  const totalRevenue = sum(metricsList, (item) => item.receita45d);
  const sortedByRevenue = [...metricsList].sort((a, b) => b.receita45d - a.receita45d);
  let accumulatedRevenuePercent = 0;

  sortedByRevenue.forEach((item, index) => {
    item.ranking = index + 1;
    item.percentualReceita = totalRevenue > 0 ? (item.receita45d / totalRevenue) * 100 : 0;
    accumulatedRevenuePercent += item.percentualReceita;
    item.percentualAcumulado = accumulatedRevenuePercent;
    item.abcClasse = accumulatedRevenuePercent <= 80 ? "A" : accumulatedRevenuePercent <= 95 ? "B" : "C";
  });

  for (const item of metricsList) {
    const demandProfile = buildDemandProfile(item.vendasPorDia, analysisEndDate);
    item.vendas15d = demandProfile.vendas15d;
    item.vendas7d = demandProfile.vendas7d;
    item.giroDiarioBruto = item.vendas45d / WINDOW_DAYS;
    item.mediaDiariaAjustada45d = demandProfile.mediaDiariaAjustada45d;
    item.mediaDiaria15d = demandProfile.mediaDiaria15d;
    item.mediaDiaria7d = demandProfile.mediaDiaria7d;
    item.fatorTendencia = demandProfile.fatorTendencia;
    item.outlierDetectado = demandProfile.outlierDetectado;
    item.diasOutlier = demandProfile.diasOutlier;
    item.sazonalidadeDetectada = demandProfile.sazonalidadeDetectada;
    item.giroDiario = demandProfile.giroDiarioCalculado;
    item.estoqueMinimoCalculado = calculateMinimumStock(item);
    item.estoqueMinimoOriginal = item.estoqueMinimo;
    item.estoqueMinimo = item.estoqueMinimo > 0 ?
      item.estoqueMinimo :
      item.estoqueMinimoCalculado;
    item.coberturaDias = item.giroDiario > 0 ? Math.max(0, item.estoqueAtual) / item.giroDiario : null;
    item.valorParado = Math.max(0, item.estoqueAtual) * getEstimatedUnitValue(item);
    const abcPolicy = getAbcCoveragePolicy(item);
    item.coberturaAlvoDias = abcPolicy.coverageDays;
    item.estoqueSegurancaDias = abcPolicy.safetyDays;
    item.estoqueAlvo = Math.ceil(item.giroDiario * abcPolicy.totalDays);
    item.quantidadeSugerida = Math.max(0, item.estoqueAlvo - Math.max(0, item.estoqueAtual));
    item.investimentoSugerido = item.quantidadeSugerida * item.custoUnitario;
    item.disponibilidadeOsa = item.estoqueAtual > 0 ? 100 : 0;
    item.taxaRupturaSku = item.giroDiario > 0 && item.estoqueAtual <= 0 ? 100 : 0;
    item.vendaPerdidaEstimada = calculateLostSales(item);
    item.diasSemVenda = calculateDaysSinceLastSale(item, analysisEndDate);
    item.statusEstoque = calculateStockStatus(item);
    item.statusGiro = calculateTurnoverStatus(item);
    item.prioridadeScore = calculatePriorityScore(item);
    item.prioridade = calculatePriority(item.prioridadeScore);
  }
}

function calculateLostSales(item) {
  if (item.giroDiario <= 0) {
    return 0;
  }

  const unitValue = getEstimatedUnitValue(item);
  const coverage = item.coberturaDias === null ? 0 : item.coberturaDias;
  const riskDays = Math.max(0, getAbcCoveragePolicy(item).safetyDays - coverage);
  return riskDays * item.giroDiario * unitValue;
}

function calculateDaysSinceLastSale(item, analysisEndDate) {
  if (!item.ultimaVendaEm) {
    return null;
  }

  const endDate = new Date(analysisEndDate);
  const lastSaleDate = new Date(item.ultimaVendaEm);
  endDate.setHours(0, 0, 0, 0);
  lastSaleDate.setHours(0, 0, 0, 0);

  return Math.max(0, Math.floor((endDate.getTime() - lastSaleDate.getTime()) / 86400000));
}

function getEstimatedUnitValue(item) {
  if (item.vendas45d > 0 && item.receita45d > 0) {
    return item.receita45d / item.vendas45d;
  }

  return item.custoUnitario || 0;
}

function calculateMinimumStock(item) {
  if (item.giroDiario <= 0) {
    return 0;
  }

  return Math.ceil(item.giroDiario * getAbcCoveragePolicy(item).safetyDays);
}

function getAbcCoveragePolicy(item) {
  const classe = item.abcClasse || item.abc_classe || "C";

  if (classe === "A") {
    return {
      coverageDays: Number(process.env.ABC_A_COVERAGE_DAYS || 15),
      safetyDays: Number(process.env.ABC_A_SAFETY_DAYS || 5),
      totalDays: Number(process.env.ABC_A_COVERAGE_DAYS || 15) + Number(process.env.ABC_A_SAFETY_DAYS || 5),
    };
  }

  if (classe === "B") {
    return {
      coverageDays: Number(process.env.ABC_B_COVERAGE_DAYS || 10),
      safetyDays: Number(process.env.ABC_B_SAFETY_DAYS || 3),
      totalDays: Number(process.env.ABC_B_COVERAGE_DAYS || 10) + Number(process.env.ABC_B_SAFETY_DAYS || 3),
    };
  }

  return {
    coverageDays: Number(process.env.ABC_C_COVERAGE_DAYS || 7),
    safetyDays: Number(process.env.ABC_C_SAFETY_DAYS || 2),
    totalDays: Number(process.env.ABC_C_COVERAGE_DAYS || 7) + Number(process.env.ABC_C_SAFETY_DAYS || 2),
  };
}

function buildDemandProfile(salesByDate, analysisEndDate = new Date()) {
  const series45d = buildDailyQuantitySeries(WINDOW_DAYS, salesByDate, analysisEndDate);
  const quantities45d = series45d.map((item) => item.quantidade);
  const sales15d = sum(series45d.slice(-15), (item) => item.quantidade);
  const sales7d = sum(series45d.slice(-7), (item) => item.quantidade);
  const rawAverage45d = average(quantities45d, (value) => value);
  const positiveQuantities = quantities45d.filter((value) => value > 0);
  const medianPositive = median(positiveQuantities);
  const stdDev45d = standardDeviation(quantities45d);
  const stdDevLimit = rawAverage45d + OUTLIER_STD_DEV_FACTOR * stdDev45d;
  const medianLimit = medianPositive > 0 ? medianPositive * OUTLIER_MEDIAN_FACTOR : stdDevLimit;
  const outlierLimit = Math.max(medianLimit, stdDevLimit);
  const adjustedQuantities = [];
  const outlierDays = [];

  for (const item of series45d) {
    const isOutlier = item.quantidade > 0 && outlierLimit > 0 && item.quantidade > outlierLimit;
    if (isOutlier) {
      outlierDays.push({
        data: item.data,
        quantidade: round(item.quantidade),
        limite: round(outlierLimit),
      });
      adjustedQuantities.push(outlierLimit);
    } else {
      adjustedQuantities.push(item.quantidade);
    }
  }

  const adjustedAverage45d = average(adjustedQuantities, (value) => value);
  const average15d = sales15d / 15;
  const average7d = sales7d / 7;
  const trendFactor = calculateTrendFactor(adjustedAverage45d, average15d, average7d);
  const calculatedDailyTurnover = adjustedAverage45d * trendFactor;

  return {
    vendas15d: round(sales15d),
    vendas7d: round(sales7d),
    mediaDiariaAjustada45d: round(adjustedAverage45d),
    mediaDiaria15d: round(average15d),
    mediaDiaria7d: round(average7d),
    fatorTendencia: round(trendFactor),
    giroDiarioCalculado: round(calculatedDailyTurnover),
    outlierDetectado: outlierDays.length > 0,
    diasOutlier: outlierDays,
    sazonalidadeDetectada: outlierDays.length > 0 && average15d < adjustedAverage45d * TREND_CONFIRMATION_THRESHOLD,
  };
}

function calculateTrendFactor(adjustedAverage45d, average15d, average7d) {
  if (adjustedAverage45d <= 0) {
    return 1;
  }

  const factor15d = average15d / adjustedAverage45d;
  const factor7d = average7d / adjustedAverage45d;

  if (factor7d >= TREND_CONFIRMATION_THRESHOLD && factor15d >= TREND_CONFIRMATION_THRESHOLD) {
    return clamp((factor7d * 0.4) + (factor15d * 0.6), TREND_MIN_FACTOR, TREND_MAX_FACTOR);
  }

  if (factor15d >= TREND_CONFIRMATION_THRESHOLD) {
    return clamp(factor15d, TREND_MIN_FACTOR, TREND_MAX_FACTOR);
  }

  if (factor7d < 1 && factor15d < 1) {
    return clamp((factor7d * 0.4) + (factor15d * 0.6), TREND_MIN_FACTOR, 1);
  }

  return 1;
}

function calculateStockStatus(item) {
  if (item.estoqueAtual < 0) {
    return "estoque_negativo";
  }

  if (item.estoqueAtual === 0 && item.giroDiario > 0) {
    return "ruptura";
  }

  if (item.estoqueMinimo > 0 && item.estoqueAtual < item.estoqueMinimo) {
    return "abaixo_minimo";
  }

  if (item.giroDiario > 0 && item.coberturaDias !== null && item.coberturaDias <= CRITICAL_COVERAGE_DAYS) {
    return "critico";
  }

  if (item.giroDiario > 0 && item.coberturaDias !== null && item.coberturaDias <= WARNING_COVERAGE_DAYS) {
    return "atencao";
  }

  if (item.giroDiario <= 0 && item.estoqueAtual > 0) {
    return "sem_vendas";
  }

  return "normal";
}

function calculateTurnoverStatus(item) {
  if (item.giroDiario <= 0) {
    return item.estoqueAtual > 0 ? "sem_vendas" : "sem_giro";
  }

  if (item.statusEstoque === "ruptura" || item.statusEstoque === "critico") {
    return "critico";
  }

  if (item.statusEstoque === "estoque_negativo") {
    return "estoque_negativo";
  }

  if (item.statusEstoque === "atencao" || item.statusEstoque === "abaixo_minimo") {
    return "atencao";
  }

  return "saudavel";
}

function turnoverStatusLabel(status) {
  const labels = {
    saudavel: "Saudavel",
    atencao: "Atencao",
    critico: "Critico",
    estoque_negativo: "Estoque negativo",
    sem_vendas: "Sem venda",
    sem_giro: "Sem giro",
  };

  return labels[status] || "Nao classificado";
}

function isRuptureItem(item) {
  return item.statusEstoque === "ruptura";
}

function isPreRuptureItem(item) {
  return ["abaixo_minimo", "critico", "atencao"].includes(item.statusEstoque);
}

function calculatePreRuptureImpactValue(item) {
  const coverageGap = item.coberturaDias === null || item.coberturaDias === undefined ?
    getAbcCoveragePolicy(item).safetyDays :
    Math.max(0, getAbcCoveragePolicy(item).safetyDays - item.coberturaDias);
  const projectedRisk = coverageGap * item.giroDiario * getEstimatedUnitValue(item);
  return Math.max(projectedRisk, item.investimentoSugerido || 0);
}
function negativeStockFields(item) {
  const estoqueAtual = roundUnits(item.estoqueAtual);
  const estoqueNegativo = estoqueAtual < 0;
  const quantidadeNegativa = estoqueNegativo ? Math.abs(estoqueAtual) : 0;
  const vendaProjetadaEstoqueNegativo = calculateNegativeStockProjectedSale(item);

  return {
    estoque_negativo: estoqueNegativo,
    quantidade_estoque_negativo: quantidadeNegativa,
    quantidade_estoque_negativo_formatada: `${quantidadeNegativa} un`,
    venda_projetada_estoque_negativo: round(vendaProjetadaEstoqueNegativo),
    venda_projetada_estoque_negativo_formatada: formatCurrency(vendaProjetadaEstoqueNegativo),
  };
}

function calculateNegativeStockProjectedSale(item) {
  const estoqueAtual = roundUnits(item.estoqueAtual);
  const quantidadeNegativa = estoqueAtual < 0 ? Math.abs(estoqueAtual) : 0;
  return quantidadeNegativa * getEstimatedUnitValue(item);
}

function coverageDaysLabel(item) {
  if (item.estoqueAtual < 0 && item.giroDiario > 0) {
    return "Estoque negativo";
  }

  if (item.estoqueAtual === 0 && item.giroDiario > 0) {
    return "Ruptura";
  }

  if (item.coberturaDias !== null && item.coberturaDias !== undefined) {
    return `${round(item.coberturaDias)} dias`;
  }

  if (item.estoqueAtual > 0 && item.giroDiario <= 0) {
    return "Sem venda no periodo";
  }

  return "Sem cobertura calculada";
}

function dailyTurnoverLabel(item) {
  if (item.giroDiario > 0) {
    return `${round(item.giroDiario)} un/dia`;
  }

  return "Sem giro diario";
}

function averageDailySalesLabel(item) {
  if (item.giroDiario > 0) {
    return `Venda media: ${round(item.giroDiario)} un/dia`;
  }

  return "Venda media: sem venda no periodo";
}

function salesFrequencyLabel(item) {
  if (item.giroDiario > 0) {
    const daysPerUnit = Math.max(1, Math.round(1 / item.giroDiario));
    return `1 un / ${daysPerUnit} dias`;
  }

  return "Sem venda no periodo";
}

function calculatePriorityScore(item) {
  let score = 0;

  if (item.abcClasse === "A") {
    score += 40;
  } else if (item.abcClasse === "B") {
    score += 25;
  } else {
    score += 10;
  }

  if (item.statusEstoque === "ruptura") {
    score += 45;
  } else if (item.statusEstoque === "estoque_negativo") {
    score += 42;
  } else if (item.statusEstoque === "critico") {
    score += 35;
  } else if (item.statusEstoque === "abaixo_minimo" || item.statusEstoque === "atencao") {
    score += 20;
  } else if (item.statusEstoque === "sem_vendas") {
    score += item.abcClasse === "A" ? 36 : 15;
  }

  if (item.vendaPerdidaEstimada > 0) {
    score += Math.min(20, Math.log10(item.vendaPerdidaEstimada + 1) * 5);
  }

  if (item.valorParado > 0 && item.statusEstoque === "sem_vendas") {
    score += Math.min(20, Math.log10(item.valorParado + 1) * 4);
  }

  return round(Math.min(100, score));
}

function calculatePriority(score) {
  if (score >= 80) {
    return "critica";
  }

  if (score >= 60) {
    return "alta";
  }

  if (score >= 35) {
    return "media";
  }

  return "baixa";
}

function summarizeAbc(metricsList) {
  return metricsList.reduce((summary, item) => {
    if (item.abcClasse === "A") {
      summary.classeA += 1;
      summary.faturamentoA += item.receita45d;
    } else if (item.abcClasse === "B") {
      summary.classeB += 1;
      summary.faturamentoB += item.receita45d;
    } else {
      summary.classeC += 1;
      summary.faturamentoC += item.receita45d;
    }

    return summary;
  }, {
    classeA: 0,
    classeB: 0,
    classeC: 0,
    faturamentoA: 0,
    faturamentoB: 0,
    faturamentoC: 0,
  });
}

function countByAbc(items, classe) {
  return items.filter((item) => item.abcClasse === classe || item.abc_classe === classe).length;
}

function filterByAbc(items, classe) {
  return items.filter((item) => item.abcClasse === classe || item.abc_classe === classe);
}

function getAbcPriority(item) {
  const classe = item.abcClasse || item.abc_classe || "C";
  if (classe === "A") {
    return 1;
  }

  if (classe === "B") {
    return 2;
  }

  return 3;
}

function buildSummary(empresaId, metricsList, alertas, sugestoesCompra, acoesRecomendadas, vendasProcessadas) {
  const activeProducts = metricsList.filter((item) => item.giroDiario > 0);
  const availableActiveProducts = activeProducts.filter((item) => item.estoqueAtual > 0);
  const stockoutProducts = activeProducts.filter((item) => item.estoqueAtual <= 0);
  const ruptureProducts = metricsList.filter(isRuptureItem);
  const preRuptureProducts = metricsList.filter(isPreRuptureItem);
  const totalVendas = sum(metricsList, (item) => item.receita45d);
  const vendaPerdida = sum(metricsList, (item) => item.vendaPerdidaEstimada);
  const perdaRuptura = sum(ruptureProducts, (item) => item.vendaPerdidaEstimada);
  const valorPreRuptura = sum(preRuptureProducts, calculatePreRuptureImpactValue);
  const investimento = sum(sugestoesCompra, (item) => item.investimentoSugerido);
  const itensSemVendas = metricsList.filter((item) => item.statusEstoque === "sem_vendas");
  const valorTotalItensSemVenda = sum(itensSemVendas, (item) => item.valorParado);
  const itensEstoqueNegativo = metricsList.filter((item) => item.estoqueAtual < 0);
  const valorTotalItensEstoqueNegativo = sum(itensEstoqueNegativo, calculateNegativeStockProjectedSale);
  const abcResumo = summarizeAbc(metricsList);
  const giroMedio = average(activeProducts, (item) => item.giroDiario);
  const giroMedioBruto = average(activeProducts, (item) => item.giroDiarioBruto);
  const giroMedioAjustado = average(activeProducts, (item) => item.mediaDiariaAjustada45d);
  const coberturaMedia = average(activeProducts.filter((item) => item.coberturaDias !== null), (item) => item.coberturaDias);
  const osa = activeProducts.length > 0 ? (availableActiveProducts.length / activeProducts.length) * 100 : 100;
  const taxaRuptura = activeProducts.length > 0 ? (stockoutProducts.length / activeProducts.length) * 100 : 0;
  const produtosComOutlier = metricsList.filter((item) => item.outlierDetectado).length;
  const produtosComSazonalidade = metricsList.filter((item) => item.sazonalidadeDetectada).length;
  const unidadesVendidas45d = sum(metricsList, (item) => item.vendas45d);
  const giroMedio45d = giroMedio * WINDOW_DAYS;
  const skusAtivos45d = metricsList.filter((item) => item.estoqueAtual > 0 || item.vendas45d > 0).length;
  const skusComVenda45d = metricsList.filter((item) => item.vendas45d > 0).length;
  const percentualSkusComVenda45d = skusAtivos45d > 0 ? (skusComVenda45d / skusAtivos45d) * 100 : 0;
  const giroMedioStatus = percentualSkusComVenda45d >= 70 ?
    "saudavel" :
    percentualSkusComVenda45d >= 40 ? "atencao" : "critico";
  const giroMedioStatusLabel = giroMedioStatus === "saudavel" ?
    "Saudavel" :
    giroMedioStatus === "atencao" ? "Atencao" : "Critico";

  return {
    empresa_id: empresaId || null,
    periodo_dias: WINDOW_DAYS,
    data_final_analise: getAnalysisEndDate(metricsList),
    metodologia: "Nielsen OSA",
    metodologia_indicadores:
      "Nielsen OSA 45 dias: disponibilidade em gondola, risco de ruptura, cobertura, venda perdida e acao por SKU.",
    meta_osa: OSA_TARGET_PERCENT,
    meta_osa_formatada: formatPercent(OSA_TARGET_PERCENT),
    disponibilidade_osa: round(osa),
    disponibilidade_osa_formatada: formatPercent(osa),
    taxa_ruptura: round(taxaRuptura),
    taxa_ruptura_formatada: formatPercent(taxaRuptura),
    total_vendas: round(totalVendas),
    total_vendas_formatado: formatCurrency(totalVendas),
    unidades_vendidas_45d: round(unidadesVendidas45d),
    unidades_vendidas_45d_formatado: `${round(unidadesVendidas45d)} un`,
    skus_ativos_45d: skusAtivos45d,
    skus_com_venda_45d: skusComVenda45d,
    percentual_skus_com_venda_45d: round(percentualSkusComVenda45d),
    percentual_skus_com_venda_45d_formatado: formatPercent(percentualSkusComVenda45d),
    giro_medio: round(giroMedio45d),
    giro_medio_formatado: `${round(giroMedio45d)} un/SKU em 45 dias`,
    giro_medio_diario: round(giroMedio),
    giro_medio_diario_formatado: `${round(giroMedio)} un/dia por SKU`,
    giro_medio_45d: round(giroMedio45d),
    giro_medio_45d_formatado: `${round(giroMedio45d)} un/SKU em 45 dias`,
    giro_medio_status: giroMedioStatus,
    giro_medio_status_label: giroMedioStatusLabel,
    giro_medio_bruto: round(giroMedioBruto),
    giro_medio_ajustado: round(giroMedioAjustado),
    giro_medio_dias: round(coberturaMedia),
    cobertura_media_dias: round(coberturaMedia),
    itens_criticos: metricsList.filter((item) => ["ruptura", "estoque_negativo", "critico", "abaixo_minimo", "atencao"].includes(item.statusEstoque)).length,
    itens_abaixo_minimo: metricsList.filter((item) => item.statusEstoque === "abaixo_minimo").length,
    itens_pre_ruptura: preRuptureProducts.length,
    itens_pre_ruptura_classe_a: countByAbc(preRuptureProducts, "A"),
    itens_pre_ruptura_classe_b: countByAbc(preRuptureProducts, "B"),
    itens_pre_ruptura_classe_c: countByAbc(preRuptureProducts, "C"),
    valor_pre_ruptura: round(valorPreRuptura),
    valor_pre_ruptura_formatado: formatCurrency(valorPreRuptura),
    itens_ruptura: ruptureProducts.length,
    itens_ruptura_classe_a: countByAbc(ruptureProducts, "A"),
    itens_ruptura_classe_b: countByAbc(ruptureProducts, "B"),
    itens_ruptura_classe_c: countByAbc(ruptureProducts, "C"),
    itens_estoque_negativo: itensEstoqueNegativo.length,
    itens_estoque_negativo_classe_a: countByAbc(itensEstoqueNegativo, "A"),
    itens_estoque_negativo_classe_b: countByAbc(itensEstoqueNegativo, "B"),
    itens_estoque_negativo_classe_c: countByAbc(itensEstoqueNegativo, "C"),
    valor_total_itens_estoque_negativo: round(valorTotalItensEstoqueNegativo),
    valor_total_itens_estoque_negativo_formatado: formatCurrency(valorTotalItensEstoqueNegativo),
    venda_projetada_estoque_negativo: round(valorTotalItensEstoqueNegativo),
    venda_projetada_estoque_negativo_formatada: formatCurrency(valorTotalItensEstoqueNegativo),
    perda_estimada_estoque_negativo: round(valorTotalItensEstoqueNegativo),
    perda_estimada_estoque_negativo_formatada: formatCurrency(valorTotalItensEstoqueNegativo),
    alertas_pendentes: alertas.length,
    itens_sem_vendas: itensSemVendas.length,
    itens_sem_vendas_classe_a: countByAbc(itensSemVendas, "A"),
    itens_sem_vendas_classe_b: countByAbc(itensSemVendas, "B"),
    itens_sem_vendas_classe_c: countByAbc(itensSemVendas, "C"),
    valor_parado: round(valorTotalItensSemVenda),
    valor_parado_formatado: formatCurrency(valorTotalItensSemVenda),
    valor_total_itens_sem_venda: round(valorTotalItensSemVenda),
    valor_total_itens_sem_venda_formatado: formatCurrency(valorTotalItensSemVenda),
    venda_perdida_estimada: round(vendaPerdida),
    venda_perdida_estimada_formatada: formatCurrency(vendaPerdida),
    perda_ruptura: round(perdaRuptura),
    perda_ruptura_formatada: formatCurrency(perdaRuptura),
    valor_perda_ruptura: round(perdaRuptura),
    valor_perda_ruptura_formatado: formatCurrency(perdaRuptura),
    perda_ruptura_classe_a: round(sum(filterByAbc(ruptureProducts, "A"), (item) => item.vendaPerdidaEstimada)),
    perda_ruptura_classe_a_formatada: formatCurrency(sum(filterByAbc(ruptureProducts, "A"), (item) => item.vendaPerdidaEstimada)),
    perda_ruptura_classe_b: round(sum(filterByAbc(ruptureProducts, "B"), (item) => item.vendaPerdidaEstimada)),
    perda_ruptura_classe_b_formatada: formatCurrency(sum(filterByAbc(ruptureProducts, "B"), (item) => item.vendaPerdidaEstimada)),
    perda_ruptura_classe_c: round(sum(filterByAbc(ruptureProducts, "C"), (item) => item.vendaPerdidaEstimada)),
    perda_ruptura_classe_c_formatada: formatCurrency(sum(filterByAbc(ruptureProducts, "C"), (item) => item.vendaPerdidaEstimada)),
    produtos_com_outlier: produtosComOutlier,
    produtos_com_sazonalidade: produtosComSazonalidade,
    investimento_sugerido: round(investimento),
    investimento_sugerido_formatado: formatCurrency(investimento),
    acoes_recomendadas: acoesRecomendadas.length,
    sugestoes_compra: sugestoesCompra.length,
    reposicao_urgente: sugestoesCompra.filter((item) => item.prioridade === "critica" || item.prioridade === "alta").length,
    abc_metodologia: "Curva ABC Nielsen/Pareto por faturamento acumulado: A ate 80%, B ate 95%, C ate 100%.",
    abc_itens_classe_a: abcResumo.classeA,
    abc_itens_classe_b: abcResumo.classeB,
    abc_itens_classe_c: abcResumo.classeC,
    abc_faturamento_classe_a: round(abcResumo.faturamentoA),
    abc_faturamento_classe_a_formatado: formatCurrency(abcResumo.faturamentoA),
    abc_faturamento_classe_b: round(abcResumo.faturamentoB),
    abc_faturamento_classe_b_formatado: formatCurrency(abcResumo.faturamentoB),
    abc_faturamento_classe_c: round(abcResumo.faturamentoC),
    abc_faturamento_classe_c_formatado: formatCurrency(abcResumo.faturamentoC),
    produtos_processados: metricsList.length,
    vendas_processadas: vendasProcessadas,
    indicadores_disponiveis: [
      "giro_medio",
      "itens_criticos",
      "estoque_negativo",
      "ruptura",
      "abaixo_minimo",
      "pre_ruptura",
      "alertas",
      "sugestao_compra",
      "itens_sem_vendas",
      "acao_recomendada",
    ],
    atualizado_em: admin.firestore.FieldValue.serverTimestamp(),
  };
}

function buildAlerts(metricsList) {
  const alerts = [];

  for (const item of metricsList) {
    const alertType = item.statusEstoque;

    if (["ruptura", "estoque_negativo", "critico", "abaixo_minimo", "atencao"].includes(alertType)) {
      alerts.push({
        id: `${safeDocId(item.produtoId)}_${alertType}`,
        empresa_id: item.empresaId || null,
        indicador_tipo: getIndicatorTypeForStockStatus(alertType),
        tipo: alertType,
        produto_id: item.produtoId,
        produto_nome: item.produtoNome,
        sku: item.sku,
        categoria: item.categoria,
        prioridade: item.prioridade,
        prioridade_score: round(item.prioridadeScore),
        status: "pendente",
        estoque_atual: roundUnits(item.estoqueAtual),
        estoque_minimo: roundUnits(item.estoqueMinimo),
        estoque_minimo_original: roundUnits(item.estoqueMinimoOriginal),
        estoque_minimo_calculado: roundUnits(item.estoqueMinimoCalculado),
        estoque_minimo_origem: item.estoqueMinimoOriginal > 0 ? "importado" : "calculado",
        ...negativeStockFields(item),
        cobertura_dias: item.coberturaDias === null ? null : round(item.coberturaDias),
        cobertura_dias_formatado: coverageDaysLabel(item),
        vendas_45d: round(item.vendas45d),
        vendas_45d_formatado: `${round(item.vendas45d)} un`,
        giro_diario: round(item.giroDiario),
        giro_diario_calculado: round(item.giroDiario),
        giro_diario_formatado: dailyTurnoverLabel(item),
        venda_media_diaria_formatada: averageDailySalesLabel(item),
        frequencia_venda_formatada: salesFrequencyLabel(item),
        giro_45d: round(item.giroDiario * WINDOW_DAYS),
        giro_45d_formatado: `${round(item.giroDiario * WINDOW_DAYS)} un em 45 dias`,
        status_giro: item.statusGiro,
        status_giro_label: turnoverStatusLabel(item.statusGiro),
        ...abcFields(item),
        venda_perdida_estimada: round(item.vendaPerdidaEstimada),
        venda_perdida_estimada_formatada: formatCurrency(item.vendaPerdidaEstimada),
        ...lastSaleFields(item),
        titulo: alertTitle(alertType),
        descricao: getActionDescription({...item, statusEstoque: alertType}),
        criado_em: admin.firestore.FieldValue.serverTimestamp(),
      });
    }
  }

  return limitRows(alerts.sort(compareBusinessPriority));
}

function getIndicatorTypeForStockStatus(statusEstoque) {
  if (statusEstoque === "estoque_negativo") {
    return "estoque_negativo";
  }

  if (statusEstoque === "abaixo_minimo") {
    return "abaixo_minimo";
  }

  return "alertas";
}

function buildRecommendedActions(metricsList) {
  const actions = [];
  const itensSemVendas = metricsList.filter((item) => item.statusEstoque === "sem_vendas");
  const itensSemVendasTotal = itensSemVendas.length;
  const valorTotalItensSemVenda = sum(itensSemVendas, (item) => item.valorParado);

  for (const item of metricsList) {
    if (["ruptura", "estoque_negativo", "critico", "abaixo_minimo", "atencao"].includes(item.statusEstoque)) {
      const action = getStockActionByAbc(item);
      actions.push(toActionDoc(item, {
        tipo: action.tipo,
        titulo: action.titulo,
        descricao: action.descricao,
        impacto: item.vendaPerdidaEstimada > 0 ? `Evitar ${formatCurrency(item.vendaPerdidaEstimada)} em perda` : "Evitar ruptura",
        itensSemVendasTotal,
        valorTotalItensSemVenda,
      }));
    }

    if (item.statusEstoque === "sem_vendas" && item.valorParado > 0) {
      const action = getNoSalesActionByAbc(item);
      actions.push(toActionDoc(item, {
        tipo: action.tipo,
        titulo: action.titulo,
        descricao: action.descricao,
        impacto: `${formatCurrency(item.valorParado)} em estoque parado`,
        itensSemVendasTotal,
        valorTotalItensSemVenda,
      }));
    }
  }

  return limitRows(actions.sort(compareBusinessPriority));
}

function getStockActionByAbc(item) {
  if (item.abcClasse === "A") {
    if (item.statusEstoque === "ruptura") {
      return {
        tipo: "reposicao_urgente",
        titulo: "Reposicao imediata - Classe A",
        descricao: `${roundUnits(item.quantidadeSugerida)} unidades sugeridas para recuperar disponibilidade de item A.`,
      };
    }

    if (item.statusEstoque === "estoque_negativo") {
      return {
        tipo: "correcao_estoque",
        titulo: "Corrigir estoque negativo - Classe A",
        descricao: "Revisar baixa, venda e saldo fisico de item A com prioridade maxima.",
      };
    }

    return {
      tipo: "reposicao_prioritaria",
      titulo: "Corrigir cobertura - Classe A",
      descricao: `${roundUnits(item.quantidadeSugerida)} unidades sugeridas para evitar ruptura em item A.`,
    };
  }

  if (item.abcClasse === "B") {
    return {
      tipo: "monitorar_reposicao",
      titulo: "Monitorar e avaliar reposicao - Classe B",
      descricao: "Avaliar cobertura, demanda e reposicao antes da ruptura.",
    };
  }

  return {
    tipo: "revisao_baixa_prioridade",
    titulo: "Revisar compra - Classe C",
    descricao: "Tratar como baixa prioridade, revisar compra e ajustar reposicao.",
  };
}

function getNoSalesActionByAbc(item) {
  if (item.abcClasse === "A") {
    return {
      tipo: "acao_item_a_sem_venda",
      titulo: "Item A sem venda",
      descricao: `${roundUnits(item.estoqueAtual)} unidades de item A sem venda nos ultimos ${WINDOW_DAYS} dias. Revisar preco, exposicao e sortimento.`,
    };
  }

  if (item.abcClasse === "B") {
    return {
      tipo: "monitorar_item_sem_venda",
      titulo: "Monitorar item sem venda - Classe B",
      descricao: `${roundUnits(item.estoqueAtual)} unidades sem venda no periodo. Avaliar reposicao e sortimento.`,
    };
  }

  return {
    tipo: "liquidacao",
    titulo: "Liquidar ou reduzir compra - Classe C",
    descricao: `${roundUnits(item.estoqueAtual)} unidades sem venda no periodo. Revisar, liquidar ou reduzir compra.`,
  };
}

function buildIndicatorItems(metricsList, alertas, acoesRecomendadas) {
  const items = [];
  const giroItems = [];
  const criticalItems = [];
  const negativeStockItems = [];
  const ruptureItems = [];
  const preRuptureItems = [];
  const noSalesItems = [];
  const purchaseItems = [];

  for (const item of metricsList) {
    giroItems.push(toIndicatorItemDoc("giro_medio", item, {
      status: item.statusGiro,
      valor: item.giroDiario,
      valorFormatado: `${round(item.coberturaDias || 0)} dias`,
      descricao: "Giro, estoque e cobertura no periodo de 45 dias",
    }));

    if (["ruptura", "estoque_negativo", "critico", "abaixo_minimo", "atencao"].includes(item.statusEstoque)) {
      criticalItems.push(toIndicatorItemDoc("itens_criticos", item, {
        status: isPreRuptureItem(item) ? "pre_ruptura" : item.statusEstoque,
        valor: isPreRuptureItem(item) ? calculatePreRuptureImpactValue(item) : item.estoqueAtual,
        valorFormatado: isPreRuptureItem(item) ? formatCurrency(calculatePreRuptureImpactValue(item)) : `${round(item.coberturaDias || 0)} dias`,
        descricao: getActionDescription(item),
      }));
    }

    if (item.statusEstoque === "estoque_negativo") {
      negativeStockItems.push(toIndicatorItemDoc("estoque_negativo", item, {
        status: "estoque_negativo",
        valor: item.estoqueAtual,
        valorFormatado: `${roundUnits(item.estoqueAtual)} un`,
        descricao: getActionDescription(item),
      }));
    }

    if (isRuptureItem(item)) {
      ruptureItems.push(toIndicatorItemDoc("ruptura", item, {
        status: "ruptura",
        valor: item.vendaPerdidaEstimada,
        valorFormatado: formatCurrency(item.vendaPerdidaEstimada),
        descricao: getActionDescription(item),
      }));
    }

    if (isPreRuptureItem(item)) {
      preRuptureItems.push(toIndicatorItemDoc("pre_ruptura", item, {
        status: "pre_ruptura",
        valor: calculatePreRuptureImpactValue(item),
        valorFormatado: formatCurrency(calculatePreRuptureImpactValue(item)),
        descricao: getActionDescription(item),
      }));
    }

    if (item.statusEstoque === "sem_vendas") {
      noSalesItems.push(toIndicatorItemDoc("itens_sem_vendas", item, {
        status: "sem_vendas",
        valor: item.valorParado,
        valorFormatado: formatCurrency(item.valorParado),
        descricao: `Sem venda nos ultimos ${WINDOW_DAYS} dias`,
      }));
    }

    if (item.quantidadeSugerida > 0) {
      purchaseItems.push(toIndicatorItemDoc("sugestao_compra", item, {
        status: item.prioridade,
        valor: item.investimentoSugerido,
        valorFormatado: formatCurrency(item.investimentoSugerido),
        descricao: `${round(item.quantidadeSugerida)} unidades sugeridas`,
      }));
    }
  }

  items.push(...limitRows(giroItems.sort(compareIndicatorRanking)));
  items.push(...limitRows(criticalItems.sort(compareBusinessPriority)));
  items.push(...limitRows(negativeStockItems.sort(compareBusinessPriority)));
  items.push(...limitRows(ruptureItems.sort(compareBusinessPriority)));
  items.push(...limitRows(preRuptureItems.sort(compareBusinessPriority)));
  items.push(...limitRows(noSalesItems.sort(compareBusinessPriority)));
  items.push(...limitRows(purchaseItems.sort(compareBusinessPriority)));

  return [
    ...items,
    ...alertas,
    ...acoesRecomendadas.map((item) => ({...item, indicador_tipo: "acao_recomendada"})),
  ];
}

function toIndicatorItemDoc(indicadorTipo, item, options) {
  return {
    id: `${safeDocId(indicadorTipo)}_${safeDocId(item.produtoId)}`,
    empresa_id: item.empresaId || null,
    indicador_tipo: indicadorTipo,
    produto_id: item.produtoId,
    produto_nome: item.produtoNome,
    sku: item.sku,
    categoria: item.categoria,
    fornecedor: item.fornecedor,
    prioridade: item.prioridade,
    prioridade_score: round(item.prioridadeScore),
    status: options.status,
    titulo: item.produtoNome,
    descricao: options.descricao,
    recomendacao_abc: getAbcRecommendation(item),
    valor: round(options.valor),
    valor_formatado: options.valorFormatado,
    vendas_45d: round(item.vendas45d),
    vendas_45d_formatado: `${round(item.vendas45d)} un`,
    vendas_15d: round(item.vendas15d),
    vendas_7d: round(item.vendas7d),
    total_vendido_45d: round(item.receita45d),
    total_vendido_45d_formatado: formatCurrency(item.receita45d),
    media_diaria_bruta_45d: round(item.giroDiarioBruto),
    media_diaria_ajustada_45d: round(item.mediaDiariaAjustada45d),
    media_diaria_15d: round(item.mediaDiaria15d),
    media_diaria_7d: round(item.mediaDiaria7d),
    fator_tendencia: round(item.fatorTendencia),
    giro_diario: round(item.giroDiario),
    giro_diario_calculado: round(item.giroDiario),
    giro_diario_formatado: dailyTurnoverLabel(item),
    venda_media_diaria_formatada: averageDailySalesLabel(item),
    frequencia_venda_formatada: salesFrequencyLabel(item),
    giro_45d: round(item.giroDiario * WINDOW_DAYS),
    giro_45d_formatado: `${round(item.giroDiario * WINDOW_DAYS)} un em 45 dias`,
    outlier_detectado: item.outlierDetectado,
    dias_outlier: item.diasOutlier,
    sazonalidade_detectada: item.sazonalidadeDetectada,
    estoque_atual: roundUnits(item.estoqueAtual),
    estoque_minimo: roundUnits(item.estoqueMinimo),
    estoque_minimo_original: roundUnits(item.estoqueMinimoOriginal),
    estoque_minimo_calculado: roundUnits(item.estoqueMinimoCalculado),
    estoque_minimo_origem: item.estoqueMinimoOriginal > 0 ? "importado" : "calculado",
    ...negativeStockFields(item),
    cobertura_dias: item.coberturaDias === null ? null : round(item.coberturaDias),
    cobertura_dias_formatado: coverageDaysLabel(item),
    cobertura_alvo_dias: round(item.coberturaAlvoDias || 0),
    estoque_seguranca_dias: round(item.estoqueSegurancaDias || 0),
    quantidade_sugerida: roundUnits(item.quantidadeSugerida),
    investimento_sugerido: round(item.investimentoSugerido),
    investimento_sugerido_formatado: formatCurrency(item.investimentoSugerido),
    valor_parado: round(item.valorParado),
    valor_parado_formatado: formatCurrency(item.valorParado),
    venda_perdida_estimada: round(item.vendaPerdidaEstimada),
    venda_perdida_estimada_formatada: formatCurrency(item.vendaPerdidaEstimada),
    status_giro: item.statusGiro,
    status_giro_label: turnoverStatusLabel(item.statusGiro),
    status_estoque: item.statusEstoque,
    ...abcFields(item),
    ...lastSaleFields(item),
  };
}

function abcFields(item) {
  return {
    abc_classe: item.abcClasse,
    abc_prioridade: getAbcPriority(item),
    abc_percentual_participacao: round(item.percentualReceita),
    abc_percentual_acumulado: round(item.percentualAcumulado),
    abc_faturamento_base: round(item.receita45d),
    abc_faturamento_base_formatado: formatCurrency(item.receita45d),
    abc_metodologia: "Curva ABC Nielsen/Pareto por faturamento acumulado: A ate 80%, B ate 95%, C ate 100%.",
    ranking: item.ranking,
  };
}

function getAbcRecommendation(item) {
  if (item.abcClasse === "A") {
    if (item.statusEstoque === "ruptura" || item.statusEstoque === "estoque_negativo") {
      return "Acao urgente: reposicao imediata ou correcao prioritaria.";
    }

    if (item.statusEstoque === "sem_vendas") {
      return "Acao urgente: item A sem venda, revisar preco, exposicao e sortimento.";
    }

    return "Prioridade alta: acompanhar cobertura e disponibilidade.";
  }

  if (item.abcClasse === "B") {
    return "Monitorar e avaliar reposicao conforme cobertura e demanda.";
  }

  return "Revisar, liquidar, reduzir compra ou tratar como baixa prioridade.";
}

function lastSaleFields(item) {
  return {
    dias_sem_venda: item.diasSemVenda,
    dias_sem_venda_formatado: formatDaysWithoutSale(item.diasSemVenda),
    ultima_venda_em: item.ultimaVendaEm || null,
    ultima_venda_em_formatada: formatDateForDisplay(item.ultimaVendaEm),
  };
}

function toActionDoc(item, options) {
  return {
    id: `${safeDocId(item.produtoId)}_${safeDocId(options.tipo)}`,
    empresa_id: item.empresaId || null,
    produto_id: item.produtoId,
    produto_nome: item.produtoNome,
    sku: item.sku,
    categoria: item.categoria,
    fornecedor: item.fornecedor,
    indicador_tipo: "acao_recomendada",
    tipo: options.tipo,
    titulo: options.titulo,
    descricao: options.descricao,
    impacto: options.impacto,
    prioridade: item.prioridade,
    prioridade_score: round(item.prioridadeScore),
    status: "pendente",
    status_giro: item.statusGiro,
    status_giro_label: turnoverStatusLabel(item.statusGiro),
    ...abcFields(item),
    recomendacao_abc: getAbcRecommendation(item),
    total_itens_sem_venda: options.itensSemVendasTotal || 0,
    total_itens_sem_venda_formatado: `${options.itensSemVendasTotal || 0} itens`,
    valor_total_itens_sem_venda: round(options.valorTotalItensSemVenda),
    valor_total_itens_sem_venda_formatado: formatCurrency(options.valorTotalItensSemVenda),
    valor_impacto: round(item.vendaPerdidaEstimada || item.valorParado || item.investimentoSugerido),
    valor_impacto_formatado: formatCurrency(item.vendaPerdidaEstimada || item.valorParado || item.investimentoSugerido),
    vendas_45d: round(item.vendas45d),
    vendas_45d_formatado: `${round(item.vendas45d)} un`,
    quantidade_sugerida: roundUnits(item.quantidadeSugerida),
    estoque_atual: roundUnits(item.estoqueAtual),
    estoque_minimo: roundUnits(item.estoqueMinimo),
    estoque_minimo_original: roundUnits(item.estoqueMinimoOriginal),
    estoque_minimo_calculado: roundUnits(item.estoqueMinimoCalculado),
    estoque_minimo_origem: item.estoqueMinimoOriginal > 0 ? "importado" : "calculado",
    ...negativeStockFields(item),
    cobertura_dias: item.coberturaDias === null ? null : round(item.coberturaDias),
    cobertura_dias_formatado: coverageDaysLabel(item),
    giro_diario: round(item.giroDiario),
    giro_diario_calculado: round(item.giroDiario),
    giro_diario_formatado: dailyTurnoverLabel(item),
    venda_media_diaria_formatada: averageDailySalesLabel(item),
    frequencia_venda_formatada: salesFrequencyLabel(item),
    giro_45d: round(item.giroDiario * WINDOW_DAYS),
    giro_45d_formatado: `${round(item.giroDiario * WINDOW_DAYS)} un em 45 dias`,
    ...lastSaleFields(item),
    criado_em: admin.firestore.FieldValue.serverTimestamp(),
  };
}

function toPurchaseSuggestionDoc(item) {
  return {
    id: `${safeDocId(item.produtoId)}_sugestao_compra`,
    empresa_id: item.empresaId || null,
    indicador_tipo: "sugestao_compra",
    produto_id: item.produtoId,
    produto_nome: item.produtoNome,
    sku: item.sku,
    categoria: item.categoria,
    fornecedor: item.fornecedor,
    prioridade: item.prioridade,
    prioridade_score: round(item.prioridadeScore),
    status: "pendente",
    titulo: item.produtoNome,
    descricao: getAbcRecommendation(item),
    acao_recomendada: getAbcRecommendation(item),
    estoque_atual: roundUnits(item.estoqueAtual),
    estoque_minimo: roundUnits(item.estoqueMinimo),
    estoque_minimo_original: roundUnits(item.estoqueMinimoOriginal),
    estoque_minimo_calculado: roundUnits(item.estoqueMinimoCalculado),
    estoque_minimo_origem: item.estoqueMinimoOriginal > 0 ? "importado" : "calculado",
    estoque_alvo: roundUnits(item.estoqueAlvo),
    cobertura_alvo_dias: round(item.coberturaAlvoDias || 0),
    estoque_seguranca_dias: round(item.estoqueSegurancaDias || 0),
    ...negativeStockFields(item),
    cobertura_dias: item.coberturaDias === null ? null : round(item.coberturaDias),
    cobertura_dias_formatado: coverageDaysLabel(item),
    vendas_45d: round(item.vendas45d),
    vendas_45d_formatado: `${round(item.vendas45d)} un`,
    vendas_15d: round(item.vendas15d),
    vendas_7d: round(item.vendas7d),
    media_diaria_bruta_45d: round(item.giroDiarioBruto),
    media_diaria_ajustada_45d: round(item.mediaDiariaAjustada45d),
    media_diaria_15d: round(item.mediaDiaria15d),
    media_diaria_7d: round(item.mediaDiaria7d),
    fator_tendencia: round(item.fatorTendencia),
    giro_diario: round(item.giroDiario),
    giro_diario_calculado: round(item.giroDiario),
    giro_diario_formatado: dailyTurnoverLabel(item),
    venda_media_diaria_formatada: averageDailySalesLabel(item),
    frequencia_venda_formatada: salesFrequencyLabel(item),
    giro_45d: round(item.giroDiario * WINDOW_DAYS),
    giro_45d_formatado: `${round(item.giroDiario * WINDOW_DAYS)} un em 45 dias`,
    status_giro: item.statusGiro,
    status_giro_label: turnoverStatusLabel(item.statusGiro),
    ...abcFields(item),
    recomendacao_abc: getAbcRecommendation(item),
    outlier_detectado: item.outlierDetectado,
    dias_outlier: item.diasOutlier,
    sazonalidade_detectada: item.sazonalidadeDetectada,
    quantidade_sugerida: roundUnits(item.quantidadeSugerida),
    quantidade_selecionada: roundUnits(item.quantidadeSugerida),
    custo_unitario: round(item.custoUnitario),
    custo_unitario_formatado: formatCurrency(item.custoUnitario),
    investimento_sugerido: round(item.investimentoSugerido),
    investimento_sugerido_formatado: formatCurrency(item.investimentoSugerido),
    venda_perdida_estimada: round(item.vendaPerdidaEstimada),
    venda_perdida_estimada_formatada: formatCurrency(item.vendaPerdidaEstimada),
    ...lastSaleFields(item),
    criado_em: admin.firestore.FieldValue.serverTimestamp(),
  };
}

function getActionDescription(item) {
  if (item.statusEstoque === "estoque_negativo") {
    return "Saldo de estoque negativo. Revisar baixa, venda ou ajuste operacional.";
  }

  if (item.statusEstoque === "ruptura") {
    return "Produto com venda recente e estoque zerado.";
  }

  if (item.statusEstoque === "critico") {
    return `Pre-ruptura: cobertura menor ou igual a ${CRITICAL_COVERAGE_DAYS} dias.`;
  }

  if (item.statusEstoque === "abaixo_minimo") {
    return "Pre-ruptura: estoque abaixo do minimo indicado para compra.";
  }

  if (item.statusEstoque === "atencao") {
    return "Pre-ruptura: cobertura baixa, monitorar reposicao.";
  }

  return "Acompanhar indicador.";
}

function alertTitle(statusEstoque) {
  const titles = {
    estoque_negativo: "Estoque negativo",
    ruptura: "Ruptura detectada",
    critico: "Pre-ruptura",
    abaixo_minimo: "Pre-ruptura",
    atencao: "Pre-ruptura",
  };

  return titles[statusEstoque] || "Alerta de estoque";
}

async function replaceOutputCollection(collectionName, rows, empresaId = null) {
  assertTenantScope(empresaId);
  const collectionRef = db.collection(collectionName);
  const existing = await getCurrentOutputDocs(collectionRef, empresaId);
  const writer = new BatchWriter(db);

  for (const doc of existing) {
    await writer.delete(doc.ref);
  }

  for (const row of rows) {
    const rawId = row.id || row.produto_id || cryptoSafeId();
    const tenantPrefix = empresaId ? `${safeDocId(empresaId)}_` : "";
    const rowWithSearch = addSearchText(row);
    await writer.set(collectionRef.doc(`${tenantPrefix}${safeDocId(rawId)}`), {
      ...rowWithSearch,
      empresa_id: empresaId || row.empresa_id || null,
      atualizado_em: admin.firestore.FieldValue.serverTimestamp(),
    });
  }

  await writer.commit();
}

async function getCurrentOutputDocs(collectionRef, empresaId) {
  const docsByPath = new Map();
  const byTenant = await collectionRef.where("empresa_id", "==", empresaId).get();

  for (const doc of byTenant.docs) {
    docsByPath.set(doc.ref.path, doc);
  }

  if (empresaId) {
    const tenantPrefix = `${safeDocId(empresaId)}_`;
    const byIdPrefix = await collectionRef
      .where(admin.firestore.FieldPath.documentId(), ">=", tenantPrefix)
      .where(admin.firestore.FieldPath.documentId(), "<", `${tenantPrefix}\uf8ff`)
      .get();

    for (const doc of byIdPrefix.docs) {
      docsByPath.set(doc.ref.path, doc);
    }
  }

  return Array.from(docsByPath.values());
}

function addSearchText(row) {
  return {
    ...row,
    busca_tokens: buildSearchTokens(row),
    busca_texto: admin.firestore.FieldValue.delete(),
  };
}

function buildSearchTokens(row) {
  const searchText = normalizeSearchText([
    row.produto_nome,
    row.titulo,
    row.descricao,
    row.sku,
    row.produto_id,
    row.categoria,
    row.fornecedor,
    row.marca,
    row.tipo,
    row.indicador_tipo,
    row.status,
    row.status_giro_label,
    row.status_estoque,
    row.prioridade,
  ].filter((value) => value !== undefined && value !== null).join(" "));

  return tokenizeSearchText(searchText);
}

function normalizeSearchText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function tokenizeSearchText(value) {
  const tokens = new Set();
  const words = normalizeSearchText(value).split(" ").filter(Boolean);

  for (const word of words) {
    tokens.add(word);

    if (word.length <= 3) {
      continue;
    }

    const maxPrefixLength = Math.min(word.length - 1, 12);
    for (let length = 3; length <= maxPrefixLength; length++) {
      tokens.add(word.slice(0, length));
    }
  }

  return Array.from(tokens).slice(0, 200);
}

function getQuerySearchTokens(value) {
  const normalized = normalizeSearchText(value);
  if (!normalized) {
    return [];
  }

  const tokens = new Set();
  const words = normalized.split(" ").filter(Boolean);

  for (const word of words) {
    tokens.add(word);
  }

  return Array.from(tokens).slice(0, 10);
}

function isSearchAllQuery(value) {
  const normalized = normalizeSearchText(value);
  return ["", "__todos__", "todos", "all", "*"].includes(normalized) || String(value || "").trim() === "*";
}

function getSearchLimitFromRequest(req) {
  const parsed = Number(req.query.limit || 50);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 50;
  }

  return Math.min(Math.floor(parsed), 100);
}

async function saveProcessingHistory(empresaId, resumo, runResult) {
  assertTenantScope(empresaId);

  if (!empresaId) {
    return;
  }

  const processedAt = new Date();
  const docId = `${safeDocId(empresaId)}_${formatHistoryDateKey(processedAt)}`;

  await db.collection(OUTPUT_COLLECTIONS.historicoProcessamentos).doc(docId).set({
    ...resumo,
    empresa_id: empresaId,
    tipo: "analytics_run",
    resultado: runResult,
    processado_em: admin.firestore.FieldValue.serverTimestamp(),
  });
}

async function getTenantDocs(collectionName, empresaId) {
  assertTenantScope(empresaId);
  const collectionRef = db.collection(collectionName);
  return collectionRef.where("empresa_id", "==", empresaId).get();
}

function compareBusinessPriority(a, b) {
  const abcDiff = getAbcSortScore(b) - getAbcSortScore(a);
  if (abcDiff !== 0) {
    return abcDiff;
  }

  const statusDiff = getStatusSortScore(b) - getStatusSortScore(a);
  if (statusDiff !== 0) {
    return statusDiff;
  }

  const priorityDiff = (b.prioridade_score || 0) - (a.prioridade_score || 0);
  if (priorityDiff !== 0) {
    return priorityDiff;
  }

  return (b.venda_perdida_estimada || b.investimentoSugerido || b.investimento_sugerido || b.valorParado || b.valor_parado || 0) -
    (a.venda_perdida_estimada || a.investimentoSugerido || a.investimento_sugerido || a.valorParado || a.valor_parado || 0);
}

function compareApiIndicatorItem(a, b) {
  if (a.indicador_tipo === "giro_medio" && b.indicador_tipo === "giro_medio") {
    return compareIndicatorRanking(a, b);
  }

  return compareBusinessPriority(a, b);
}

function getAbcSortScore(item) {
  const classe = item.abcClasse || item.abc_classe || "C";
  if (classe === "A") {
    return 300;
  }

  if (classe === "B") {
    return 200;
  }

  return 100;
}

function getStatusSortScore(item) {
  const status = item.statusEstoque || item.status_estoque || item.status || "";
  const scores = {
    ruptura: 60,
    estoque_negativo: 55,
    sem_vendas: 50,
    critico: 45,
    abaixo_minimo: 45,
    atencao: 40,
  };

  return scores[status] || 0;
}

function compareIndicatorRanking(a, b) {
  const rankA = a.ranking || 999999;
  const rankB = b.ranking || 999999;
  return rankA - rankB;
}

function limitRows(rows) {
  return rows.slice(0, MAX_ITEMS_PER_INDICATOR);
}

function assertTenantScope(empresaId) {
  if (empresaId) {
    return;
  }

  if (ALLOW_GLOBAL_JOBS) {
    return;
  }

  const error = new Error("empresaId obrigatorio para execucoes SaaS. Envie ?empresaId=... ou header x-empresa-id.");
  error.statusCode = 400;
  throw error;
}

function assertSnapshotSize(snapshot, maxRows, collectionName) {
  if (snapshot.size <= maxRows) {
    return;
  }

  const error = new Error(
    `Limite de custo excedido em ${collectionName}: ${snapshot.size} docs encontrados, maximo ${maxRows}. ` +
      "Filtre por empresa, reduza o lote ou aumente o limite via variavel de ambiente.",
  );
  error.statusCode = 413;
  throw error;
}

class BatchWriter {
  constructor(firestore) {
    this.firestore = firestore;
    this.batch = firestore.batch();
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

function initializeFirebase() {
  if (admin.apps.length > 0) {
    return;
  }

  const serviceAccountBase64 = process.env.FIREBASE_SERVICE_ACCOUNT_BASE64;
  const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;

  if (serviceAccountBase64) {
    const serviceAccount = JSON.parse(Buffer.from(serviceAccountBase64, "base64").toString("utf8"));
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      storageBucket: getStorageBucketName(serviceAccount.project_id),
    });
    return;
  }

  if (serviceAccountJson) {
    const serviceAccount = JSON.parse(serviceAccountJson);
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      storageBucket: getStorageBucketName(serviceAccount.project_id),
    });
    return;
  }

  admin.initializeApp({
    credential: admin.credential.applicationDefault(),
    storageBucket: getStorageBucketName(),
  });
}

function getDb() {
  if (!firestoreDb) {
    initializeFirebase();
    firestoreDb = admin.firestore();
  }

  return firestoreDb;
}

function getStorageBucketName(projectId = null) {
  const explicitBucket = (process.env.FIREBASE_STORAGE_BUCKET || "").trim();
  if (explicitBucket) {
    return explicitBucket;
  }

  if (projectId) {
    return `${projectId}.firebasestorage.app`;
  }

  return DEFAULT_STORAGE_BUCKET;
}

function getStorageBucket() {
  initializeFirebase();
  return admin.storage().bucket(getStorageBucketName());
}

function requireApiKey(req, res, next) {
  const expectedKey = process.env.SUGESTION_DATA_DRIVEN_API_KEY;
  const providedKey = req.header("x-api-key") || (ALLOW_API_KEY_QUERY ? req.query.apiKey : null);
  if (!expectedKey && !REQUIRE_API_KEY) {
    next();
    return;
  }

  if (!expectedKey) {
    res.status(500).json({ok: false, error: "SUGESTION_DATA_DRIVEN_API_KEY nao configurada."});
    return;
  }

  if (providedKey !== expectedKey) {
    res.status(401).json({ok: false, error: "API key invalida."});
    return;
  }

  next();
}

function getEmpresaIdFromRequest(req) {
  const value = req.query.empresaId || req.query.empresa_id || req.header("x-empresa-id");
  return value ? String(value).trim() : null;
}

function parseCsvValue(key, value) {
  const isIdField = key === "id" || key === "produto_id" || key === "venda_id" || key.endsWith("_id");

  if (typeof value === "string") {
    value = value.trim();
  }

  if (value === "") {
    return "";
  }

  if (value === "true") {
    return true;
  }

  if (value === "false") {
    return false;
  }

  if (isIdField) {
    return String(value).trim();
  }

  if (typeof value === "string") {
    let normalized = value.replace("R$", "").trim();
    const hasComma = normalized.includes(",");
    const hasDot = normalized.includes(".");

    if (hasComma && hasDot && normalized.lastIndexOf(",") > normalized.lastIndexOf(".")) {
      normalized = normalized.replace(/\./g, "").replace(",", ".");
    } else if (hasComma && hasDot && normalized.lastIndexOf(".") > normalized.lastIndexOf(",")) {
      normalized = normalized.replace(/,/g, "");
    } else if (hasComma && !hasDot) {
      normalized = normalized.replace(",", ".");
    }

    const number = Number(normalized);
    if (Number.isFinite(number)) {
      return number;
    }
  }

  return value;
}

function normalizeRecordKeys(data) {
  const normalized = {};

  for (const key in data) {
    normalized[normalizeHeader(key)] = data[key];
  }

  return normalized;
}

function normalizeHeader(header) {
  return String(header || "")
    .trim()
    .replace(/^\uFEFF/, "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function normalizeProductId(value) {
  if (value === undefined || value === null) {
    return "";
  }

  let text = String(value).trim();

  if (text.endsWith(".0")) {
    text = text.slice(0, -2);
  }

  if (/^0+\d+$/.test(text)) {
    text = text.replace(/^0+/, "") || "0";
  }

  return text;
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
    const parsed = asNumber(data[key]);
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

  if (typeof value === "string" && value.trim() !== "") {
    const normalized = value.replace("R$", "").replace(/\./g, "").replace(",", ".").trim();
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : null;
  }

  return null;
}

function getRecordDate(data) {
  for (const key of DATE_KEYS) {
    const value = data[key];
    const parsed = parseDateValue(value);
    if (parsed) {
      return parsed;
    }
  }

  return null;
}

function parseDateValue(value) {
  if (!value) {
    return null;
  }

  if (typeof value.toDate === "function") {
    return value.toDate();
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    const brazilianDate = trimmed.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})(?:\s+.*)?$/);

    if (brazilianDate) {
      const day = Number(brazilianDate[1]);
      const month = Number(brazilianDate[2]);
      const rawYear = Number(brazilianDate[3]);
      const year = rawYear < 100 ? 2000 + rawYear : rawYear;
      const parsed = new Date(year, month - 1, day);

      if (
        parsed.getFullYear() === year &&
        parsed.getMonth() === month - 1 &&
        parsed.getDate() === day
      ) {
        return parsed;
      }
    }
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function maxDate(currentDate, nextDate) {
  if (!nextDate) {
    return currentDate || null;
  }

  if (!currentDate) {
    return nextDate;
  }

  return nextDate > currentDate ? nextDate : currentDate;
}

function buildDailyQuantitySeries(days, salesByDate, endDate = new Date()) {
  const series = [];
  const anchorDate = new Date(endDate);
  anchorDate.setHours(0, 0, 0, 0);

  for (let index = days - 1; index >= 0; index -= 1) {
    const date = new Date(anchorDate);
    date.setDate(date.getDate() - index);
    const key = formatDateKey(date);
    series.push({
      data: key,
      quantidade: Number(salesByDate[key] || 0),
    });
  }

  return series;
}

function formatDateKey(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function formatHistoryDateKey(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  const seconds = String(date.getSeconds()).padStart(2, "0");
  return `${year}${month}${day}_${hours}${minutes}${seconds}`;
}

function sum(rows, selector) {
  return rows.reduce((total, row) => total + selector(row), 0);
}

function average(rows, selector) {
  return rows.length === 0 ? 0 : sum(rows, selector) / rows.length;
}

function median(values) {
  if (values.length === 0) {
    return 0;
  }

  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);

  if (sorted.length % 2 === 0) {
    return (sorted[middle - 1] + sorted[middle]) / 2;
  }

  return sorted[middle];
}

function standardDeviation(values) {
  if (values.length === 0) {
    return 0;
  }

  const avg = average(values, (value) => value);
  const variance = average(values, (value) => Math.pow(value - avg, 2));
  return Math.sqrt(variance);
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function round(value) {
  return Math.round((Number(value || 0) + Number.EPSILON) * 100) / 100;
}

function roundUnits(value) {
  return Math.round(Number(value || 0));
}

function formatCurrency(value) {
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
  }).format(value || 0);
}

function formatPercent(value) {
  return `${round(value).toLocaleString("pt-BR", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  })}%`;
}

function formatDaysWithoutSale(value) {
  if (value === null || value === undefined) {
    return "Sem venda registrada";
  }

  const days = roundUnits(value);
  return days === 1 ? "1 dia sem venda" : `${days} dias sem venda`;
}

function formatDateForDisplay(value) {
  const date = value ? new Date(value) : null;
  if (!date || Number.isNaN(date.getTime())) {
    return "";
  }

  return new Intl.DateTimeFormat("pt-BR").format(date);
}

function safeDocId(value) {
  return String(value).replace(/[\/#[\]?]/g, "_").slice(0, 1400) || cryptoSafeId();
}

function cryptoSafeId() {
  return `doc_${Date.now()}_${Math.random().toString(36).slice(2)}`;
}

function isFirestoreIndexError(error) {
  const message = String(error && error.message ? error.message : "").toLowerCase();
  return error && (error.code === 9 || error.code === "failed-precondition" || message.includes("index"));
}

function sendError(res, error) {
  console.error(error);
  res.status(error.statusCode || 500).json({
    ok: false,
    error: error && error.message ? error.message : "Erro desconhecido",
  });
}

const port = Number(process.env.PORT || 3000);
app.listen(port, () => {
  console.log(`Estoqueia backend rodando na porta ${port}`);
});
