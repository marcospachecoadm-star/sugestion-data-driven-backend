const WINDOW_DAYS = 30;
const RECENT_WINDOW_DAYS = 7;
const DEFAULT_COVERAGE_TARGET_DAYS = 30;
const DEFAULT_SAFETY_DAYS = 7;
const DEAD_STOCK_MIN_UNITS = 1;
const NIQ_AVAILABILITY_TARGET = 97;

const PRODUCT_ID_KEYS = ["produto_id", "id", "sku", "codigo", "cod_produto"];
const STOCK_QUANTITY_KEYS = [
  "estoque_atual",
  "quantidade_estoque",
  "quantidade",
  "qtd",
  "qtde",
  "saldo",
  "estoque",
];
const SALES_QUANTITY_KEYS = ["quantidade_vendida", "quantidade", "qtd", "qtde", "qty"];
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
  "vl_item",
  "vlr_item",
  "valor_final",
  "valor_pago",
  "faturamento",
  "subtotal",
];
const UNIT_PRICE_KEYS = [
  "preco_unitario",
  "valor_unitario",
  "preco",
  "valor_produto",
  "preco_venda",
  "vl_unitario",
  "vlr_unitario",
];
const UNIT_COST_KEYS = ["custo_unitario", "preco_compra", "preco_custo", "custo", "preco"];

module.exports = {
  WINDOW_DAYS,
  RECENT_WINDOW_DAYS,
  DEFAULT_COVERAGE_TARGET_DAYS,
  DEFAULT_SAFETY_DAYS,
  DEAD_STOCK_MIN_UNITS,
  NIQ_AVAILABILITY_TARGET,
  PRODUCT_ID_KEYS,
  STOCK_QUANTITY_KEYS,
  SALES_QUANTITY_KEYS,
  SALES_TOTAL_KEYS,
  UNIT_PRICE_KEYS,
  UNIT_COST_KEYS,
};
