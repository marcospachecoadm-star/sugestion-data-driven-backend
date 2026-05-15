function converterValorCsv(chave, valor) {
  const campoId =
    chave === "id" ||
    chave === "produto_id" ||
    chave === "venda_id" ||
    chave.endsWith("_id");

  if (typeof valor === "string") {
    valor = valor.trim();
  }

  if (valor === "") {
    return "";
  }

  if (valor === "true") {
    return true;
  }

  if (valor === "false") {
    return false;
  }

  if (campoId) {
    return String(valor).trim();
  }

  if (typeof valor === "string") {
    let numero = valor;
    const temVirgula = numero.includes(",");
    const temPonto = numero.includes(".");

    if (temVirgula && temPonto && numero.lastIndexOf(",") > numero.lastIndexOf(".")) {
      numero = numero.replace(/\./g, "").replace(",", ".");
    } else if (temVirgula && temPonto && numero.lastIndexOf(".") > numero.lastIndexOf(",")) {
      numero = numero.replace(/,/g, "");
    } else if (temVirgula && !temPonto) {
      numero = numero.replace(",", ".");
    }

    if (!isNaN(numero) && numero !== "") {
      return Number(numero);
    }
  }

  return valor;
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
  const date = getRecordDate(data);

  if (!date) {
    return true;
  }

  return date >= cutoff;
}

function isWithinWindowStrict(data, cutoff) {
  const date = getRecordDate(data);
  return date ? date >= cutoff : false;
}

function getRecordDate(data) {
  const rawDate = data.data_venda || data.data || data.criado_em || data.created_at || data.createdAt;

  if (typeof rawDate.toDate === "function") {
    return rawDate.toDate();
  }

  if (rawDate instanceof Date) {
    return Number.isNaN(rawDate.getTime()) ? null : rawDate;
  }

  if (typeof rawDate === "number") {
    const parsed = new Date(rawDate);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  if (typeof rawDate !== "string") {
    return null;
  }

  const value = rawDate.trim();

  if (!value) {
    return null;
  }

  const brDate = value.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{2,4})$/);
  if (brDate) {
    const day = Number(brDate[1]);
    const month = Number(brDate[2]) - 1;
    const year = Number(brDate[3].length === 2 ? `20${brDate[3]}` : brDate[3]);
    const parsed = new Date(year, month, day);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
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

function formatPercent(value) {
  return `${round(value).toLocaleString("pt-BR", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  })}%`;
}

module.exports = {
  converterValorCsv,
  normalizeProductId,
  firstString,
  firstNumber,
  isWithinWindow,
  isWithinWindowStrict,
  getRecordDate,
  safeDocId,
  cryptoSafeId,
  sum,
  average,
  round,
  formatCurrency,
  formatPercent,
};
