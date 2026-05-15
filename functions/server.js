require("dotenv").config();

const cors = require("cors");
const express = require("express");
const routes = require("./routes");
const {initializeFirebase} = require("./repositories/firebaseRepository");

initializeFirebase();

const app = express();

app.use(cors());
app.use(express.json({limit: "2mb"}));
app.use(routes);

const port = Number(process.env.PORT || 3000);
app.listen(port, () => {
  console.log(`SugestionDataDriven backend rodando na porta ${port}`);
});

module.exports = app;
