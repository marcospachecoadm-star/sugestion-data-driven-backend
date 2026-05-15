function getEmpresaIdFromRequest(req) {
  const value = req.query.empresaId || req.query.empresa_id || req.header("x-empresa-id");
  return normalizeEmpresaId(value);
}

function normalizeEmpresaId(value) {
  if (!value) {
    return null;
  }

  const empresaId = String(value).trim();

  if (!empresaId) {
    return null;
  }

  if (!/^[a-zA-Z0-9_-]{3,80}$/.test(empresaId)) {
    throw new Error("empresaId invalido. Use apenas letras, numeros, _ ou -.");
  }

  return empresaId;
}

module.exports = {
  getEmpresaIdFromRequest,
  normalizeEmpresaId,
};
