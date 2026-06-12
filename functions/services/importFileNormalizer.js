const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");
const {converterValorCsv} = require("../analytics/utils");

const EXCEL_EXTENSIONS = new Set([".xlsx", ".xls"]);
const SUPPORTED_EXTENSIONS = new Set([".csv", ...EXCEL_EXTENSIONS]);

const COLUMN_ALIASES = {
  produtos: {
    produto_id: [
      "produto_id", "id_produto", "codigo_produto", "cod_produto",
      "codproduto", "cod_prod", "codprod", "codigo", "sku", "ean",
      "gtin", "codigo_barras", "cod_barras", "referencia", "ref",
    ],
    produto_nome: [
      "produto_nome", "nome_produto", "descricao_produto", "descricao",
      "nome", "produto", "item", "mercadoria", "nome_item",
    ],
    categoria: ["categoria", "departamento", "grupo", "secao", "familia", "linha"],
    fornecedor: ["fornecedor", "distribuidor", "supplier", "fabricante"],
    marca: ["marca", "brand"],
    custo_unitario: [
      "custo_unitario", "custo", "preco_custo", "preco_compra",
      "valor_custo", "custo_medio", "cmv",
    ],
    estoque_minimo: [
      "estoque_minimo", "minimo", "estoque_min", "min", "qtd_minima",
      "quantidade_minima",
    ],
  },
  estoque: {
    produto_id: [
      "produto_id", "id_produto", "codigo_produto", "cod_produto",
      "codproduto", "cod_prod", "codprod", "codigo", "sku", "ean",
      "gtin", "codigo_barras", "cod_barras", "referencia", "ref",
    ],
    produto_nome: [
      "produto_nome", "nome_produto", "descricao_produto", "descricao",
      "nome", "produto", "item", "mercadoria", "nome_item",
    ],
    estoque_atual: [
      "estoque_atual", "estoque", "saldo", "saldo_estoque",
      "quantidade_estoque", "quantidade", "qtd", "qtde", "qtd_estoque",
      "estoque_disponivel", "estoque_total",
    ],
    estoque_minimo: [
      "estoque_minimo", "minimo", "estoque_min", "min", "qtd_minima",
      "quantidade_minima",
    ],
    custo_unitario: [
      "custo_unitario", "custo", "preco_custo", "preco_compra",
      "valor_custo", "custo_medio", "cmv",
    ],
  },
  vendas: {
    venda_id: [
      "venda_id", "id_venda", "codigo_venda", "cupom", "numero_cupom",
      "documento", "pedido",
    ],
    produto_id: [
      "produto_id", "id_produto", "codigo_produto", "cod_produto",
      "codproduto", "cod_prod", "codprod", "codigo", "sku", "ean",
      "gtin", "codigo_barras", "cod_barras", "referencia", "ref",
    ],
    produto_nome: [
      "produto_nome", "nome_produto", "descricao_produto", "descricao",
      "nome", "produto", "item", "mercadoria", "nome_item",
    ],
    data: [
      "data", "data_venda", "dt_venda", "emissao", "data_emissao",
      "created_at", "criado_em",
    ],
    quantidade: [
      "quantidade", "quantidade_vendida", "qtd", "qtde", "qty",
      "qtd_vendida", "qtde_vendida", "unidades", "volume",
    ],
    total: [
      "total", "valor_total", "total_vendido", "receita", "valor",
      "valor_venda", "preco_total", "vl_total", "vlr_total",
      "valor_liquido", "valor_bruto", "total_item", "subtotal",
    ],
    preco_unitario: [
      "preco_unitario", "valor_unitario", "preco", "preco_venda",
      "valor_produto", "unitario",
    ],
    custo_unitario: [
      "custo_unitario", "custo", "preco_custo", "preco_compra",
      "valor_custo", "custo_medio", "cmv",
    ],
  },
};

const REQUIRED_COLUMNS = {
  produtos: [
    {field: "produto_id", label: "codigo/sku do produto"},
    {field: "produto_nome", label: "nome/descricao do produto"},
  ],
  estoque: [
    {field: "produto_id", label: "codigo/sku do produto"},
    {field: "estoque_atual", label: "estoque/quantidade atual"},
  ],
  vendas: [
    {field: "produto_id", label: "codigo/sku do produto"},
    {field: "data", label: "data da venda"},
    {field: "quantidade", label: "quantidade vendida"},
  ],
};

function isSupportedImportFile(filePath) {
  return SUPPORTED_EXTENSIONS.has(path.extname(filePath || "").toLowerCase());
}

function getImportFileExtension(filePath) {
  return path.extname(filePath || "").toLowerCase();
}

async function parseImportFile(tempFilePath, tipoArquivo, empresaId, options = {}) {
  const extension = getImportFileExtension(tempFilePath);
  const rows = EXCEL_EXTENSIONS.has(extension) ?
    readExcelRows(tempFilePath) :
    await readCsvRows(tempFilePath);

  return normalizeImportRows(rows, tipoArquivo, empresaId, options);
}

function readExcelRows(tempFilePath) {
  let XLSX;
  try {
    XLSX = require("xlsx");
  } catch (error) {
    throw new Error("Dependencia xlsx nao instalada para importar Excel.");
  }

  const workbook = XLSX.readFile(tempFilePath, {cellDates: true});
  const sheetName = workbook.SheetNames[0];
  if (!sheetName) {
    throw new Error("Planilha Excel sem abas.");
  }

  return XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], {
    defval: "",
    raw: false,
  });
}

function readCsvRows(tempFilePath) {
  return new Promise((resolve, reject) => {
    const rows = [];
    const separator = detectCsvSeparator(tempFilePath);

    fs.createReadStream(tempFilePath)
      .pipe(csv({
        separator,
        mapHeaders: ({header}) => cleanHeader(header),
        mapValues: ({value}) => typeof value === "string" ? value.trim() : value,
      }))
      .on("data", (data) => rows.push(data))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

function detectCsvSeparator(tempFilePath) {
  const sample = fs.readFileSync(tempFilePath, "utf8").split(/\r?\n/, 1)[0] || "";
  const commas = (sample.match(/,/g) || []).length;
  const semicolons = (sample.match(/;/g) || []).length;
  return semicolons > commas ? ";" : ",";
}

function normalizeImportRows(rows, tipoArquivo, empresaId, options = {}) {
  const canonicalType = normalizeFileType(tipoArquivo);
  const aliases = COLUMN_ALIASES[canonicalType];
  const maxRows = Number(options.maxRows || 0);

  if (!aliases) {
    throw new Error(`Tipo de arquivo invalido: ${tipoArquivo}`);
  }

  if (!Array.isArray(rows) || rows.length === 0) {
    throw new Error("Arquivo sem linhas para importar.");
  }

  const normalizedRows = [];
  let headersChecked = false;

  for (const rawRow of rows) {
    if (maxRows > 0 && normalizedRows.length >= maxRows) {
      throw new Error(`Arquivo excede o limite de ${maxRows} linhas por arquivo.`);
    }

    const row = normalizeRow(rawRow, aliases);
    if (!headersChecked) {
      assertRequiredColumns(row, canonicalType);
      headersChecked = true;
    }

    if (isEmptyRow(row)) {
      continue;
    }

    row.empresa_id = empresaId;
    normalizedRows.push(row);
  }

  if (normalizedRows.length === 0) {
    throw new Error("Arquivo sem linhas validas para importar.");
  }

  return normalizedRows;
}

function normalizeRow(rawRow, aliases) {
  const source = {};

  for (const key in rawRow) {
    source[normalizeHeader(key)] = rawRow[key];
  }

  const row = {};

  for (const canonicalKey in aliases) {
    const foundKey = aliases[canonicalKey]
      .map((alias) => normalizeHeader(alias))
      .find((alias) => Object.prototype.hasOwnProperty.call(source, alias));

    if (foundKey) {
      row[canonicalKey] = converterValorCsv(canonicalKey, source[foundKey]);
    }
  }

  return row;
}

function assertRequiredColumns(row, tipoArquivo) {
  const missing = REQUIRED_COLUMNS[tipoArquivo]
    .filter(({field}) => row[field] === undefined)
    .map(({label}) => label);

  if (missing.length > 0) {
    throw new Error(
      `Arquivo ${tipoArquivo} sem colunas obrigatorias: ${missing.join(", ")}.`,
    );
  }
}

function isEmptyRow(row) {
  return Object.values(row).every((value) => {
    if (value === undefined || value === null) {
      return true;
    }

    return String(value).trim() === "";
  });
}

function cleanHeader(header) {
  return String(header || "").trim().replace(/^\uFEFF/, "");
}

function normalizeHeader(header) {
  return cleanHeader(header)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function normalizeFileType(tipoArquivo) {
  const value = String(tipoArquivo || "").trim().toLowerCase();
  return value === "produto" ? "produtos" : value;
}

module.exports = {
  parseImportFile,
  isSupportedImportFile,
  getImportFileExtension,
  normalizeFileType,
  normalizeHeader,
};
