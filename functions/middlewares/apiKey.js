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

module.exports = {
  requireApiKey,
};
