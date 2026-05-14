const functions = require("firebase-functions/v1");
const admin = require("firebase-admin");
const csv = require("csv-parser");
const fs = require("fs");
const os = require("os");
const path = require("path");

admin.initializeApp();

// =====================================================
// IMPORTACAO CSV -> FIRESTORE
// =====================================================


function formatarMoedaBR(valor) {
  return "R$ " + Number(valor || 0).toLocaleString("pt-BR", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}
function identificarArquivo(filePath) {
  const partes = filePath.split("/");
  const fileName = path.basename(filePath);
  const nomeSemExtensao = path.basename(fileName, ".csv");

  const match = nomeSemExtensao.match(
    /^(.+)_(vendas|estoque|produto|produtos)_\d{2}_\d{2}_\d{4}$/
  );

  if (match) {
    return {
      empresaId: match[1],
      tipoArquivo: match[2],
      fileName: fileName,
    };
  }

  if (partes.length === 3) {
    return {
      empresaId: partes[1],
      tipoArquivo: nomeSemExtensao,
      fileName: fileName,
    };
  }

  throw new Error("Nome de arquivo invalido: " + fileName);
}

function obterNomeColecao(tipoArquivo) {
  if (tipoArquivo === "produto" || tipoArquivo === "produtos") {
    return "produtos";
  }

  if (tipoArquivo === "vendas") {
    return "vendas";
  }

  if (tipoArquivo === "estoque") {
    return "estoque";
  }

  throw new Error("Tipo de arquivo invalido: " + tipoArquivo);
}

function obterNumero(...valores) {
  for (const valor of valores) {
    const numero = Number(valor);

    if (!isNaN(numero) && numero > 0) {
      return numero;
    }
  }

  return 0;
}

function obterCustoUnitario(produto, ranking) {
  const custoProduto = obterNumero(
    produto && produto.custo,
    produto && produto.preco_custo,
    produto && produto.custo_unitario,
    produto && produto.valor_custo,
    produto && produto.preco_compra
  );

  if (custoProduto > 0) {
    return custoProduto;
  }

  const quantidadeVendida = obterNumero(ranking && ranking.quantidade_vendida);
  const totalVendido = obterNumero(ranking && ranking.total_vendido);

  if (quantidadeVendida > 0 && totalVendido > 0) {
    return totalVendido / quantidadeVendida;
  }

  return 0;
}

function adicionarProdutoNoMapa(mapa, produto, docId) {
  const possiveisIds = [
    produto.id,
    produto.produto_id,
    produto.codigo,
    produto.cod_produto,
    docId,
  ];

  for (const id of possiveisIds) {
    if (id !== undefined && id !== null && id !== "") {
      mapa[String(id)] = produto;
    }
  }
}

async function calcularDadosDashboard(db) {
  const periodoDias = 30;
  const diasRupturaCritica = 7;

  const estoqueSnap = await db.collection("estoque").get();
  const vendasSnap = await db.collection("vendas").get();
  const rankingSnap = await db.collection("ranking_vendas").get();
  const sugestoesSnap = await db.collection("sugestoes_compra").get();
  const alertasSnap = await db.collection("alertas").get();
  const produtosSnap = await db.collection("produtos").get();

  const rankingMap = {};
  const produtosMap = {};

  rankingSnap.forEach((doc) => {
    const ranking = doc.data();
    const produtoId = ranking.produto_id || doc.id;

    rankingMap[String(produtoId)] = ranking;
  });

  produtosSnap.forEach((doc) => {
    adicionarProdutoNoMapa(produtosMap, doc.data(), doc.id);
  });

  let itensCriticos = 0;
  let totalGiro = 0;
  let totalCoberturaDias = 0;
  let produtosComGiro = 0;

  estoqueSnap.forEach((doc) => {
    const item = doc.data();
    const produtoId = String(item.produto_id || item.id || doc.id);
    const estoqueAtual = Number(item.quantidade || 0);
    const ranking = rankingMap[produtoId];
    const quantidadeVendida = ranking
      ? Number(ranking.quantidade_vendida || 0)
      : 0;
    const mediaVendaDia = quantidadeVendida / periodoDias;
    const diasCobertura = mediaVendaDia > 0
      ? estoqueAtual / mediaVendaDia
      : null;

    if (
      estoqueAtual <= 0 ||
      (diasCobertura !== null && diasCobertura < diasRupturaCritica) ||
      (diasCobertura === null && estoqueAtual < 20)
    ) {
      itensCriticos++;
    }

    if (quantidadeVendida > 0) {
      totalGiro += quantidadeVendida / Math.max(estoqueAtual, 1);
      produtosComGiro++;
    }

    if (diasCobertura !== null && diasCobertura >= 0) {
      totalCoberturaDias += diasCobertura;
    }
  });

  let investimentoSugerido = 0;

  sugestoesSnap.forEach((doc) => {
    const sugestao = doc.data();
    const produtoId = String(sugestao.produto_id || doc.id);
    const produto = produtosMap[produtoId];
    const ranking = rankingMap[produtoId];
    const quantidadeSugerida = Number(sugestao.quantidade_sugerida || 0);
    const custoUnitario = obterCustoUnitario(produto, ranking);

    investimentoSugerido += quantidadeSugerida * custoUnitario;
  });

  const giroMedio = produtosComGiro > 0
    ? totalGiro / produtosComGiro
    : 0;
  const coberturaMediaDias = produtosComGiro > 0
    ? totalCoberturaDias / produtosComGiro
    : 0;
  const valorVendidoTotal = Object.values(rankingMap).reduce((total, item) => {
    return total + Number(item.total_vendido || 0);
  }, 0);

  return {
    tipo: "dashboard",
    periodo_dias: periodoDias,
    giro_medio: Number(giroMedio.toFixed(2)),
    cobertura_media_dias: Number(coberturaMediaDias.toFixed(1)),
    itens_criticos: itensCriticos,
    alertas_pendentes: alertasSnap.size || itensCriticos,
    investimento_sugerido: Number(investimentoSugerido.toFixed(2)),
    investimento_sugerido_formatado: formatarMoedaBR(investimentoSugerido),
    total_vendas: vendasSnap.size,
    valor_vendido_total: Number(valorVendidoTotal.toFixed(2)),
    valor_vendido_total_formatado: formatarMoedaBR(valorVendidoTotal),
    produtos_analisados: estoqueSnap.size,
    atualizado_em: new Date(),
  };
}

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
    }
    else if (temVirgula && temPonto && numero.lastIndexOf(".") > numero.lastIndexOf(",")) {
      numero = numero.replace(/,/g, "");
    }
    else if (temVirgula && !temPonto) {
      numero = numero.replace(",", ".");
    }

    if (!isNaN(numero) && numero !== "") {
      return Number(numero);
    }
  }

  return valor;
}


function obterNumeroVenda(venda, campos) {
  for (const campo of campos) {
    const valor = venda[campo];

    if (valor !== undefined && valor !== null && valor !== "") {
      const convertido = converterValorCsv(campo, valor);
      const numero = Number(convertido);

      if (!isNaN(numero)) {
        return numero;
      }
    }
  }

  return 0;
}
async function processarArquivoCsv(bucket, filePath) {
  if (!filePath || !filePath.startsWith("uploads/")) {
    console.log("Arquivo ignorado:", filePath);
    return { status: "ignorado", filePath: filePath };
  }

  const fileName = path.basename(filePath);

  let empresaId = "sem_empresa";
  let nomeColecao = "";
  let tempFilePath = path.join(os.tmpdir(), Date.now() + "_" + fileName);
  let destinoProcessada = "";
  let destinoErro = "erro/" + fileName;

  try {
    const dadosArquivo = identificarArquivo(filePath);

    empresaId = dadosArquivo.empresaId;
    nomeColecao = obterNomeColecao(dadosArquivo.tipoArquivo);
    destinoProcessada = "processada/" + empresaId + "/" + dadosArquivo.fileName;
    destinoErro = "erro/" + empresaId + "/" + dadosArquivo.fileName;

    if (!empresaId) {
      throw new Error("empresaId nao encontrado no caminho do arquivo");
    }

    console.log("Nome:", filePath);
    console.log("Empresa:", empresaId);
    console.log("Colecao:", nomeColecao);

    await bucket.file(filePath).download({
      destination: tempFilePath,
    });

    console.log("CSV baixado");

    const resultados = [];

    await new Promise((resolve, reject) => {
      fs.createReadStream(tempFilePath)
        .pipe(csv({
          separator: ",",
          mapHeaders: ({ header }) => header.trim().replace(/^\uFEFF/, ""),
          mapValues: ({ value }) => typeof value === "string" ? value.trim() : value,
        }))
        .on("data", (data) => {
          const itemTratado = {};

          for (const chaveOriginal in data) {
            const chave = chaveOriginal.trim().replace(/^\uFEFF/, "");
            const valor = converterValorCsv(chave, data[chaveOriginal]);

            itemTratado[chave] = valor;
          }

          itemTratado.empresa_id = empresaId;
          resultados.push(itemTratado);
        })
        .on("end", resolve)
        .on("error", reject);
    });

    console.log("Linhas lidas:", resultados.length);
    console.log("Salvando Firestore");

    const db = admin.firestore();

    for (const item of resultados) {
      let documentId = item.id;

      if (nomeColecao === "estoque") {
        documentId = item.produto_id;
      }

      if (nomeColecao === "produtos") {
        documentId = item.id || item.produto_id || item.codigo || item.cod_produto;
      }

      if (nomeColecao === "vendas") {
        documentId = item.venda_id || db.collection(nomeColecao).doc().id;
      }

      if (!documentId) {
        documentId = db.collection(nomeColecao).doc().id;
      }

      await db
        .collection(nomeColecao)
        .doc(String(documentId).trim())
        .set(item);
    }

    console.log("Dados salvos");

    await bucket.file(filePath).move(destinoProcessada);

    console.log("Arquivo movido para:", destinoProcessada);

    return {
      status: "processado",
      filePath: filePath,
      destino: destinoProcessada,
      colecao: nomeColecao,
      linhas: resultados.length,
    };
  }
  catch (error) {
    console.error("Erro ao processar CSV:", error);

    try {
      await bucket.file(filePath).move(destinoErro);
      console.log("Arquivo movido para:", destinoErro);
    }
    catch (moveError) {
      console.error("Erro ao mover arquivo para erro:", moveError);
    }

    return {
      status: "erro",
      filePath: filePath,
      destino: destinoErro,
      erro: error.message,
    };
  }
  finally {
    if (fs.existsSync(tempFilePath)) {
      fs.unlinkSync(tempFilePath);
    }
  }
}

exports.testeUpload = functions
  .runWith({
    timeoutSeconds: 540,
    memory: "1GB",
  })
  .storage
  .object()
  .onFinalize(async (object) => {
    console.log("Arquivo enviado");

    const bucket = admin.storage().bucket(object.bucket);

    await processarArquivoCsv(bucket, object.name);

    return null;
  });

exports.processarUploadsPendentes = functions
  .runWith({
    timeoutSeconds: 540,
    memory: "1GB",
  })
  .https.onRequest(async (req, res) => {
    if (req.query.executar !== "sim") {
      res.status(200).send("Processamento pendente. Para executar, use ?executar=sim");
      return;
    }

    const bucket = admin.storage().bucket();

    try {
      const [files] = await bucket.getFiles({ prefix: "uploads/" });
      const resultados = [];

      for (const file of files) {
        if (file.name.endsWith("/")) {
          continue;
        }

        if (!file.name.toLowerCase().endsWith(".csv")) {
          continue;
        }

        const resultado = await processarArquivoCsv(bucket, file.name);
        resultados.push(resultado);
      }

      res.status(200).json({
        ok: true,
        total: resultados.length,
        resultados: resultados,
      });
    }
    catch (error) {
      console.error("Erro ao processar uploads pendentes:", error);
      res.status(500).json({
        ok: false,
        erro: error.message,
      });
    }
  });
// =====================================================
// DASHBOARD ANALYTICS
// =====================================================

exports.calcularDashboard = functions.pubsub
  .schedule("every 168 hours")
  .onRun(async () => {

    console.log("ðŸš€ Calculando dashboard");

    const db = admin.firestore();

    const dashboard = await calcularDadosDashboard(db);

    await db
      .collection("insights")
      .doc("dashboard")
      .set(dashboard);

    console.log("âœ… Dashboard atualizado");

    return null;

  });

// =====================================================
// ALERTAS AUTOMÃTICOS
// =====================================================

exports.calcularAlertas = functions.pubsub
  .schedule("every 168 hours")
  .onRun(async () => {
    console.log("Calculando alertas");

    const db = admin.firestore();

    const estoqueSnap = await db.collection("estoque").get();
    const produtosSnap = await db.collection("produtos").get();

    const produtosMap = {};

    produtosSnap.forEach((doc) => {
      const produto = doc.data();

      const possiveisIds = [
        produto.id,
        produto.produto_id,
        produto.codigo,
        produto.cod_produto,
        doc.id,
      ];

      for (const id of possiveisIds) {
        if (id !== undefined && id !== null && id !== "") {
          const idTexto = String(id).trim();
          const idSemZeros = idTexto.replace(/^0+/, "") || "0";

          produtosMap[idTexto] = produto;
          produtosMap[idSemZeros] = produto;
        }
      }
    });

    const alertasAntigos = await db.collection("alertas").get();

    for (const doc of alertasAntigos.docs) {
      await doc.ref.delete();
    }

    for (const doc of estoqueSnap.docs) {
      const item = doc.data();

      const produtoId = String(item.produto_id).trim();
      const quantidade = Number(item.quantidade || 0);
      const produto = produtosMap[produtoId];

      const produtoNome =
        (produto && produto.nome) ||
        (produto && produto.produto_nome) ||
        (produto && produto.descricao) ||
        (produto && produto.descricao_produto) ||
        "Produto";

      const categoria =
        (produto && produto.categoria) ||
        (produto && produto.grupo) ||
        (produto && produto.departamento) ||
        "Sem categoria";

      if (quantidade < 20) {
        await db
          .collection("alertas")
          .doc(produtoId + "_ruptura")
          .set({
            tipo: "ruptura",
            produto_id: produtoId,
            produto_nome: produtoNome,
            categoria: categoria,
            prioridade: "alta",
            estoque: quantidade,
            criado_em: new Date(),
          });
      }

      if (quantidade > 100) {
        await db
          .collection("alertas")
          .doc(produtoId + "_excesso")
          .set({
            tipo: "excesso",
            produto_id: produtoId,
            produto_nome: produtoNome,
            categoria: categoria,
            prioridade: "media",
            estoque: quantidade,
            criado_em: new Date(),
          });
      }
    }

    console.log("Alertas criados");

    return null;
  });


// =====================================================
// PRODUTOS MORTOS
// =====================================================

exports.calcularProdutosMortos = functions.pubsub
  .schedule("every 168 hours")
  .onRun(async () => {

    console.log("ðŸ’€ Calculando produtos mortos");

    const db = admin.firestore();

    const estoqueSnap = await db.collection("estoque").get();
    const vendasSnap = await db.collection("vendas").get();
    const produtosSnap = await db.collection("produtos").get();

    // mapa produtos
    const produtosMap = {};

    produtosSnap.forEach((doc) => {

      const produto = doc.data();

      produtosMap[produto.id] = produto;

    });

    // mapa vendas
    const vendasMap = {};

    vendasSnap.forEach((doc) => {

      const venda = doc.data();

      const produtoId = venda.produto_id;

      vendasMap[produtoId] = true;

    });

    // limpa coleÃ§Ã£o antiga
    const antigos = await db.collection("produtos_mortos").get();

    for (const doc of antigos.docs) {
      await doc.ref.delete();
    }

    // identifica produtos mortos
    for (const doc of estoqueSnap.docs) {

      const item = doc.data();

      const produtoId = item.produto_id;

      const quantidade = item.quantidade || 0;

      const teveVenda = vendasMap[produtoId];

      const produto = produtosMap[produtoId];

      if (quantidade > 20 && !teveVenda) {

        await db
          .collection("produtos_mortos")
          .doc(String(produtoId))
          .set({

            produto_id: produtoId,

            produto_nome: produto?.nome || "Produto",

            categoria: produto?.categoria || "Sem categoria",

            estoque: quantidade,

            status: "produto_morto",

            criado_em: new Date(),

          });

      }

    }

    console.log("âœ… Produtos mortos calculados");

    return null;

  });

// =====================================================
// TESTE MANUAL ALERTAS
// =====================================================

exports.testarAlertas = functions.https.onRequest(async (req, res) => {

  const db = admin.firestore();

  const estoqueSnap = await db.collection("estoque").get();

  for (const doc of estoqueSnap.docs) {

    const item = doc.data();

    const quantidade = item.quantidade || 0;

    if (quantidade < 20) {

      await db.collection("alertas").doc(String(item.produto_id) + "_ruptura").set({

        tipo: "ruptura",

        produto_id: item.produto_id,

        prioridade: "alta",

        estoque: quantidade,

        criado_em: new Date(),

      });

    }

  }

  res.send("âœ… Alertas criados");

});

exports.calcularRankingVendas = functions.pubsub
  .schedule("every 168 hours")
  .onRun(async () => {
    console.log("Calculando ranking vendas");

    const db = admin.firestore();

    const vendasSnap = await db.collection("vendas").get();
    const produtosSnap = await db.collection("produtos").get();

    const produtosMap = {};
    const rankingMap = {};

    produtosSnap.forEach((doc) => {
      const produto = doc.data();

      const possiveisIds = [
        produto.id,
        produto.produto_id,
        produto.codigo,
        produto.cod_produto,
        doc.id,
      ];

      for (const id of possiveisIds) {
        if (id !== undefined && id !== null && id !== "") {
          const idTexto = String(id).trim();
          const idSemZeros = idTexto.replace(/^0+/, "") || "0";

          produtosMap[idTexto] = produto;
          produtosMap[idSemZeros] = produto;
        }
      }
    });

    vendasSnap.forEach((doc) => {
      const venda = doc.data();

      const produtoId = String(venda.produto_id).trim();
      const quantidade = obterNumeroVenda(venda, [
        "quantidade",
        "qtd",
        "qtde",
        "quantidade_vendida",
      ]);

      let valor = obterNumeroVenda(venda, [
        "valor_total",
        "total_vendido",
        "valor",
        "total",
        "valor_venda",
        "preco_total",
        "vl_total",
        "vlr_total",
        "valor_liquido",
        "valor_bruto",
        "total_item",
        "subtotal",
      ]);

      if (!valor) {
        const precoUnitario = obterNumeroVenda(venda, [
          "preco_unitario",
          "valor_unitario",
          "preco",
          "valor_produto",
          "preco_venda",
          "vl_unitario",
          "vlr_unitario",
        ]);

        valor = quantidade * precoUnitario;

        if (!valor && quantidade > 0) {
          valor = quantidade;
        }
      }

      if (!rankingMap[produtoId]) {
        rankingMap[produtoId] = {
          produto_id: produtoId,
          quantidade_vendida: 0,
          total_vendido: 0,
        };
      }

      rankingMap[produtoId].quantidade_vendida += quantidade;
      rankingMap[produtoId].total_vendido += valor;
    });

    const rankingArray = Object.values(rankingMap);

    rankingArray.sort((a, b) => {
      return b.quantidade_vendida - a.quantidade_vendida;
    });

    const rankingAntigo = await db.collection("ranking_vendas").get();

    for (const doc of rankingAntigo.docs) {
      await doc.ref.delete();
    }

    for (let i = 0; i < rankingArray.length; i++) {
      const item = rankingArray[i];
      const produto = produtosMap[item.produto_id];

      const produtoNome =
        (produto && produto.nome) ||
        (produto && produto.produto_nome) ||
        (produto && produto.descricao) ||
        (produto && produto.descricao_produto) ||
        "Produto";

      const categoria =
        (produto && produto.categoria) ||
        (produto && produto.grupo) ||
        (produto && produto.departamento) ||
        "Sem categoria";

      await db.collection("ranking_vendas").doc(item.produto_id).set({
        ranking: i + 1,
        produto_id: item.produto_id,
        produto_nome: produtoNome,
        categoria: categoria,
        quantidade_vendida: item.quantidade_vendida,
        total_vendido: Number(item.total_vendido.toFixed(2)),
        atualizado_em: new Date(),
      });
    }

    console.log("Ranking atualizado");

    return null;
  });

  exports.calcularCurvaABC = functions.pubsub
  .schedule("every 168 hours")
  .onRun(async () => {
    console.log("Calculando curva ABC");

    const db = admin.firestore();

    const rankingSnap = await db.collection("ranking_vendas").get();

    const ranking = [];
    let totalGeral = 0;

    rankingSnap.forEach((doc) => {
      const item = doc.data();
      const totalVendido = Number(item.total_vendido || 0);

      totalGeral += totalVendido;

      ranking.push({
        produto_id: String(item.produto_id).trim(),
        produto_nome: item.produto_nome || "Produto",
        categoria: item.categoria || "Sem categoria",
        total_vendido: totalVendido,
        quantidade_vendida: Number(item.quantidade_vendida || 0),
      });
    });

    const curvaAntiga = await db.collection("curva_abc").get();

    for (const doc of curvaAntiga.docs) {
      await doc.ref.delete();
    }

    if (ranking.length === 0) {
      await db.collection("curva_abc").doc("_status").set({
        status: "sem_ranking",
        mensagem: "Ranking de vendas vazio. Curva ABC nao calculada.",
        total_itens: 0,
        total_geral: 0,
        atualizado_em: new Date(),
      });

      console.log("Curva ABC sem itens de ranking");
      return null;
    }

    ranking.sort((a, b) => {
      return b.total_vendido - a.total_vendido;
    });

    let acumulado = 0;

    for (const item of ranking) {
      acumulado += item.total_vendido;

      const percentual = totalGeral > 0
        ? (acumulado / totalGeral) * 100
        : 0;

      let classe = "C";

      if (percentual <= 80) {
        classe = "A";
      }
      else if (percentual <= 95) {
        classe = "B";
      }

      const percentualAcumulado = Number(percentual.toFixed(2));

      await db.collection("curva_abc").doc(item.produto_id).set({
        produto_id: item.produto_id,
        produto_nome: item.produto_nome,
        categoria: item.categoria,
        total_vendido: Number(item.total_vendido.toFixed(2)),
        quantidade_vendida: item.quantidade_vendida,
        percentual_acumulado: `${percentualAcumulado}%`,
        percentual_acumulado_numero: percentualAcumulado,
        classe: classe,
        atualizado_em: new Date(),
      });
    }

    await db.collection("curva_abc").doc("_status").set({
      status: "calculado",
      total_itens: ranking.length,
      total_geral: Number(totalGeral.toFixed(2)),
      atualizado_em: new Date(),
    });

    console.log("Curva ABC calculada. Itens:", ranking.length, "Total geral:", totalGeral);

    return null;
  });

 
  exports.calcularSugestoesCompra = functions.pubsub
  .schedule("every 168 hours")
  .onRun(async () => {

    console.log("ðŸ›’ Calculando sugestÃµes compra");

    const db = admin.firestore();

    const estoqueSnap = await db.collection("estoque").get();
    const rankingSnap = await db.collection("ranking_vendas").get();
    const produtosSnap = await db.collection("produtos").get();

    const rankingMap = {};
    const produtosMap = {};

    rankingSnap.forEach((doc) => {
      const item = doc.data();
      rankingMap[String(item.produto_id).trim()] = item;
    });

    produtosSnap.forEach((doc) => {
      const produto = doc.data();

      const possiveisIds = [
        produto.id,
        produto.produto_id,
        produto.codigo,
        produto.cod_produto,
        doc.id,
      ];

      for (const id of possiveisIds) {
        if (id !== undefined && id !== null && id !== "") {
          const idTexto = String(id).trim();
          const idSemZeros = idTexto.replace(/^0+/, "") || "0";

          produtosMap[idTexto] = produto;
          produtosMap[idSemZeros] = produto;
        }
      }
    });

    const antigas = await db.collection("sugestoes_compra").get();

    for (const doc of antigas.docs) {
      await doc.ref.delete();
    }

    for (const doc of estoqueSnap.docs) {
      const estoque = doc.data();

      const produtoId = String(estoque.produto_id).trim();
      const quantidadeEstoque = Number(estoque.quantidade || 0);
      const ranking = rankingMap[produtoId];

      const produto = produtosMap[produtoId];

      if (!ranking) continue;

    
      const quantidadeVendida = Number(ranking.quantidade_vendida || 0);
      const diasPeriodo = 30;
      const diasCoberturaAlvo = 30;
      const diasSeguranca = 7;
      const mediaVendaDia = quantidadeVendida / diasPeriodo;
      const estoqueDesejado = Math.ceil(
        mediaVendaDia * (diasCoberturaAlvo + diasSeguranca)
      );
      const quantidadeSugerida = Math.max(
        0,
        Math.ceil(estoqueDesejado - quantidadeEstoque)
      );

      let prioridade = "baixa";

      if (quantidadeSugerida > 50) {
        prioridade = "alta";
      }
      else if (quantidadeSugerida > 20) {
        prioridade = "media";
      }

      const produtoNome =
        (produto && produto.nome) ||
        (produto && produto.produto_nome) ||
        (produto && produto.descricao) ||
        (produto && produto.descricao_produto) ||
        ranking.produto_nome ||
        "Produto";

      const categoria =
        (produto && produto.categoria) ||
        (produto && produto.grupo) ||
        (produto && produto.departamento) ||
        ranking.categoria ||
        "Sem categoria";

      await db
        .collection("sugestoes_compra")
        .doc(produtoId)
        .set({

          produto_id: produtoId,

          produto_nome: produtoNome,

          categoria: categoria,

          estoque_atual: quantidadeEstoque,

          quantidade_vendida: quantidadeVendida,

          media_venda_dia: Number(mediaVendaDia.toFixed(2)),

          dias_cobertura_alvo: diasCoberturaAlvo,

          dias_seguranca: diasSeguranca,

          estoque_desejado: estoqueDesejado,

          quantidade_sugerida: quantidadeSugerida,

          prioridade: prioridade,

          criado_em: new Date(),

        });
    }

    console.log("âœ… SugestÃµes compra calculadas");

    return null;

  });

  exports.calcularPrevisaoRuptura = functions.pubsub
  .schedule("every 168 hours")
  .onRun(async () => {

    console.log("ðŸ“‰ Calculando previsÃ£o ruptura");

    const db = admin.firestore();

    const estoqueSnap = await db.collection("estoque").get();

    const rankingSnap = await db.collection("ranking_vendas").get();

    // mapa ranking
    const rankingMap = {};

    rankingSnap.forEach((doc) => {

      const item = doc.data();

      rankingMap[item.produto_id] = item;

    });

    // limpa coleÃ§Ã£o antiga
    const antiga = await db.collection("previsao_ruptura").get();

    for (const doc of antiga.docs) {

      await doc.ref.delete();

    }

    // previsÃ£o
    for (const doc of estoqueSnap.docs) {

      const estoque = doc.data();

      const produtoId = estoque.produto_id;

      const quantidadeEstoque =
        estoque.quantidade || 0;

      const ranking = rankingMap[produtoId];

      if (!ranking) continue;

      const quantidadeVendida =
        ranking.quantidade_vendida || 0;

      // mÃ©dia simples
      const mediaVenda = quantidadeVendida / 30;

      // evita divisÃ£o por zero
      if (mediaVenda <= 0) continue;

      const diasCobertura =
        quantidadeEstoque / mediaVenda;

      // risco
      let risco = "baixo";

      if (diasCobertura < 7) {

        risco = "alto";

      }
      else if (diasCobertura < 15) {

        risco = "medio";

      }

      await db
        .collection("previsao_ruptura")
        .doc(String(produtoId))
        .set({

          produto_id: produtoId,

          produto_nome:
          produto?.nome || "Produto",

          categoria:
          produto?.categoria || "Sem categoria",

          estoque_atual: quantidadeEstoque,

          media_venda_dia: Number(mediaVenda.toFixed(2)),

          dias_cobertura:
            Number(diasCobertura.toFixed(1)),

          risco: risco,

          atualizado_em: new Date(),

        });

    }

    console.log("âœ… PrevisÃ£o ruptura calculada");

    return null;

  });

  exports.processarIndicadoresAgora = functions
  .runWith({
    timeoutSeconds: 540,
    memory: "1GB",
  })
  .https.onRequest(async (req, res) => {
    if (req.query.executar !== "sim") {
      res.status(200).send("Processamento pendente. Para executar, use ?executar=sim");
      return;
    }

    const db = admin.firestore();

    try {
      console.log("Processando indicadores agora");

      // =====================================================
      // PRODUTOS MAP
      // =====================================================
      const produtosSnap = await db.collection("produtos").get();
      const produtosMap = {};

      produtosSnap.forEach((doc) => {
        const produto = doc.data();

        const possiveisIds = [
          produto.id,
          produto.produto_id,
          produto.codigo,
          produto.cod_produto,
          doc.id,
        ];

        for (const id of possiveisIds) {
          if (id !== undefined && id !== null && id !== "") {
            const idTexto = String(id).trim();
            const idSemZeros = idTexto.replace(/^0+/, "") || "0";

            produtosMap[idTexto] = produto;
            produtosMap[idSemZeros] = produto;
          }
        }
      });

      // =====================================================
      // RANKING VENDAS
      // =====================================================
      const vendasSnap = await db.collection("vendas").get();
      const rankingMap = {};

      vendasSnap.forEach((doc) => {
        const venda = doc.data();

        const produtoId = String(venda.produto_id).trim();
        const quantidade = obterNumeroVenda(venda, [
          "quantidade",
          "qtd",
          "qtde",
          "quantidade_vendida",
        ]);

        let valor = obterNumeroVenda(venda, [
          "valor_total",
          "total_vendido",
          "valor",
          "total",
          "valor_venda",
          "preco_total",
          "vl_total",
          "vlr_total",
          "valor_liquido",
          "valor_bruto",
          "total_item",
          "subtotal",
        ]);

        if (!valor) {
          const precoUnitario = obterNumeroVenda(venda, [
            "preco_unitario",
            "valor_unitario",
            "preco",
            "valor_produto",
            "preco_venda",
            "vl_unitario",
            "vlr_unitario",
          ]);

          valor = quantidade * precoUnitario;

        if (!valor && quantidade > 0) {
          valor = quantidade;
        }
        }

        if (!rankingMap[produtoId]) {
          rankingMap[produtoId] = {
            produto_id: produtoId,
            quantidade_vendida: 0,
            total_vendido: 0,
          };
        }

        rankingMap[produtoId].quantidade_vendida += quantidade;
        rankingMap[produtoId].total_vendido += valor;
      });

      const rankingArray = Object.values(rankingMap);

      rankingArray.sort((a, b) => {
        return b.quantidade_vendida - a.quantidade_vendida;
      });

      const rankingAntigo = await db.collection("ranking_vendas").get();

      for (const doc of rankingAntigo.docs) {
        await doc.ref.delete();
      }

      for (let i = 0; i < rankingArray.length; i++) {
        const item = rankingArray[i];
        const produto = produtosMap[item.produto_id];

        const produtoNome =
          (produto && produto.nome) ||
          (produto && produto.produto_nome) ||
          (produto && produto.descricao) ||
          (produto && produto.descricao_produto) ||
          "Produto";

        const categoria =
          (produto && produto.categoria) ||
          (produto && produto.grupo) ||
          (produto && produto.departamento) ||
          "Sem categoria";

        item.produto_nome = produtoNome;
        item.categoria = categoria;
        item.ranking = i + 1;
        item.total_vendido = Number(item.total_vendido.toFixed(2));

        await db.collection("ranking_vendas").doc(item.produto_id).set({
          ranking: item.ranking,
          produto_id: item.produto_id,
          produto_nome: item.produto_nome,
          categoria: item.categoria,
          quantidade_vendida: item.quantidade_vendida,
          total_vendido: item.total_vendido,
          atualizado_em: new Date(),
        });
      }

      // =====================================================
      // CURVA ABC
      // =====================================================
      const curvaAntiga = await db.collection("curva_abc").get();

      for (const doc of curvaAntiga.docs) {
        await doc.ref.delete();
      }

      const rankingPorValor = rankingArray.slice().sort((a, b) => {
        return Number(b.total_vendido || 0) - Number(a.total_vendido || 0);
      });

      const totalGeral = rankingPorValor.reduce((total, item) => {
        return total + Number(item.total_vendido || 0);
      }, 0);

      if (rankingPorValor.length === 0) {
        await db.collection("curva_abc").doc("_status").set({
          status: "sem_ranking",
          mensagem: "Ranking de vendas vazio. Curva ABC nao calculada.",
          total_itens: 0,
          total_geral: 0,
          atualizado_em: new Date(),
        });
      }

      let acumulado = 0;

      for (const item of rankingPorValor) {
        const totalVendido = Number(item.total_vendido || 0);
        acumulado += totalVendido;

        const percentual = totalGeral > 0
          ? (acumulado / totalGeral) * 100
          : 0;

        let classe = "C";

        if (percentual <= 80) {
          classe = "A";
        }
        else if (percentual <= 95) {
          classe = "B";
        }

        const percentualAcumulado = Number(percentual.toFixed(2));

        await db.collection("curva_abc").doc(item.produto_id).set({
          produto_id: item.produto_id,
          produto_nome: item.produto_nome || "Produto",
          categoria: item.categoria || "Sem categoria",
          total_vendido: Number(totalVendido.toFixed(2)),
          quantidade_vendida: Number(item.quantidade_vendida || 0),
          percentual_acumulado: `${percentualAcumulado}%`,
          percentual_acumulado_numero: percentualAcumulado,
          classe: classe,
          atualizado_em: new Date(),
        });
      }

      if (rankingPorValor.length > 0) {
        await db.collection("curva_abc").doc("_status").set({
          status: "calculado",
          total_itens: rankingPorValor.length,
          total_geral: Number(totalGeral.toFixed(2)),
          atualizado_em: new Date(),
        });
      }

      // =====================================================
      // SUGESTOES COMPRA
      // =====================================================
      const estoqueSnap = await db.collection("estoque").get();
      const sugestoesAntigas = await db.collection("sugestoes_compra").get();

      for (const doc of sugestoesAntigas.docs) {
        await doc.ref.delete();
      }

      const rankingAtualizadoMap = {};

      for (const item of rankingArray) {
        rankingAtualizadoMap[String(item.produto_id).trim()] = item;
      }

      for (const doc of estoqueSnap.docs) {
        const estoque = doc.data();

        const produtoId = String(estoque.produto_id).trim();
        const quantidadeEstoque = Number(estoque.quantidade || 0);
        const ranking = rankingAtualizadoMap[produtoId];
        const produto = produtosMap[produtoId];

        const quantidadeVendida = ranking
          ? Number(ranking.quantidade_vendida || 0)
          : 0;

        const diasPeriodo = 30;
        const diasCoberturaAlvo = 30;
        const diasSeguranca = 7;
        const mediaVendaDia = quantidadeVendida / diasPeriodo;
        const estoqueDesejado = Math.ceil(
          mediaVendaDia * (diasCoberturaAlvo + diasSeguranca)
        );
        const quantidadeSugerida = Math.max(
          0,
          Math.ceil(estoqueDesejado - quantidadeEstoque)
        );

        let prioridade = "baixa";

        if (quantidadeSugerida > 50) {
          prioridade = "alta";
        }
        else if (quantidadeSugerida > 20) {
          prioridade = "media";
        }

        const produtoNome =
          (produto && produto.nome) ||
          (produto && produto.produto_nome) ||
          (produto && produto.descricao) ||
          (produto && produto.descricao_produto) ||
          (ranking && ranking.produto_nome) ||
          "Produto";

        const categoria =
          (produto && produto.categoria) ||
          (produto && produto.grupo) ||
          (produto && produto.departamento) ||
          (ranking && ranking.categoria) ||
          "Sem categoria";

        await db.collection("sugestoes_compra").doc(produtoId).set({
          produto_id: produtoId,
          produto_nome: produtoNome,
          categoria: categoria,
          estoque_atual: quantidadeEstoque,
          quantidade_vendida: quantidadeVendida,
          media_venda_dia: Number(mediaVendaDia.toFixed(2)),
          dias_cobertura_alvo: diasCoberturaAlvo,
          dias_seguranca: diasSeguranca,
          estoque_desejado: estoqueDesejado,
          quantidade_sugerida: quantidadeSugerida,
          prioridade: prioridade,
          criado_em: new Date(),
        });
      }

      res.status(200).send(
        "Indicadores processados. Ranking: " +
        rankingArray.length +
        ", Curva ABC: " +
        rankingPorValor.length
      );
    }
    catch (error) {
      console.error("Erro ao processar indicadores agora:", error);
      res.status(500).send("Erro ao processar indicadores: " + error.message);
    }
  });
exports.processarDashboardAgora = functions
  .runWith({
    timeoutSeconds: 540,
    memory: "1GB",
  })
  .https.onRequest(async (req, res) => {
    if (req.query.executar !== "sim") {
      res.status(200).send("Processamento pendente. Para executar, use ?executar=sim");
      return;
    }

    const db = admin.firestore();

    try {
      console.log("Processando dashboard agora");

      const dashboard = await calcularDadosDashboard(db);

      await db
        .collection("insights")
        .doc("dashboard")
        .set(dashboard);

      res.status(200).send("Dashboard processado com sucesso");
    }
    catch (error) {
      console.error("Erro ao processar dashboard:", error);
      res.status(500).send("Erro ao processar dashboard: " + error.message);
    }
  });
