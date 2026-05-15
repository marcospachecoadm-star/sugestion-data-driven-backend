const csv = require("csv-parser");
const fs = require("fs");
const os = require("os");
const path = require("path");
const {converterValorCsv, safeDocId} = require("../analytics/utils");
const {
  admin,
  getDb,
  getStorageBucket,
  BatchWriter,
} = require("../repositories/firebaseRepository");
const {normalizeEmpresaId} = require("./tenantService");

async function processarUploadsPendentes(empresaId = null) {
  empresaId = normalizeEmpresaId(empresaId);
  const bucket = getStorageBucket();
  const prefix = empresaId ? `uploads/${empresaId}/` : "uploads/";
  const [files] = await bucket.getFiles({prefix});
  const resultados = [];

  for (const file of files) {
    if (file.name.endsWith("/") || !file.name.toLowerCase().endsWith(".csv")) {
      continue;
    }

    resultados.push(await processarArquivoCsv(bucket, file.name));
  }

  return {
    total: resultados.length,
    resultados,
  };
}

async function processarArquivoCsv(bucket, filePath) {
  if (!filePath || !filePath.startsWith("uploads/")) {
    return {status: "ignorado", filePath};
  }

  const fileName = path.basename(filePath);
  const tempFilePath = path.join(os.tmpdir(), `${Date.now()}_${fileName}`);
  const db = getDb();
  let importId = safeDocId(filePath);
  let jobRef = null;
  let currentFilePath = filePath;
  let destinoErro = `erro/${fileName}`;

  try {
    const dadosArquivo = identificarArquivo(filePath);
    const empresaId = normalizeEmpresaId(dadosArquivo.empresaId);
    const nomeColecao = obterNomeColecao(dadosArquivo.tipoArquivo);
    const fileGeneration = await obterGeracaoArquivo(bucket, filePath);
    importId = safeDocId(`${filePath}_${fileGeneration}`);
    jobRef = db.collection("import_jobs").doc(importId);

    const destinoProcessada = `processada/${empresaId}/${dadosArquivo.fileName}`;
    destinoErro = `erro/${empresaId}/${dadosArquivo.fileName}`;
    const destinoProcessando = `processando/${empresaId}/${importId}_${dadosArquivo.fileName}`;

    const lockResult = await iniciarJobImportacao(jobRef, {
      importId,
      empresaId,
      tipoArquivo: dadosArquivo.tipoArquivo,
      nomeColecao,
      filePath,
      fileName: dadosArquivo.fileName,
    });

    if (!lockResult.locked) {
      return {
        status: lockResult.status === "processed" ? "ja_processado" : "ignorado",
        empresaId,
        filePath,
        importId,
        jobStatus: lockResult.status,
      };
    }

    await moverArquivoSePossivel(bucket, filePath, destinoProcessando);
    currentFilePath = destinoProcessando;

    await bucket.file(currentFilePath).download({destination: tempFilePath});

    const linhas = await lerCsv(tempFilePath, empresaId);
    await salvarLinhasNoFirestore(nomeColecao, linhas);
    await moverArquivoSePossivel(bucket, currentFilePath, destinoProcessada);

    await finalizarJobImportacao(jobRef, {
      status: "processed",
      destino: destinoProcessada,
      linhas: linhas.length,
    });

    return {
      status: "processado",
      importId,
      empresaId,
      filePath,
      destino: destinoProcessada,
      colecao: nomeColecao,
      linhas: linhas.length,
    };
  } catch (error) {
    console.error("Erro ao processar CSV:", filePath, error);

    try {
      await moverArquivoSePossivel(bucket, currentFilePath, destinoErro);
    } catch (moveError) {
      console.error("Erro ao mover arquivo para erro:", moveError);
    }

    if (jobRef) {
      await finalizarJobImportacao(jobRef, {
        status: "failed",
        destino: destinoErro,
        erro: error && error.message ? error.message : String(error),
      });
    }

    return {
      status: "erro",
      importId,
      filePath,
      destino: destinoErro,
      erro: error && error.message ? error.message : String(error),
    };
  } finally {
    if (fs.existsSync(tempFilePath)) {
      fs.unlinkSync(tempFilePath);
    }
  }
}

async function obterGeracaoArquivo(bucket, filePath) {
  const [metadata] = await bucket.file(filePath).getMetadata();
  return metadata.generation || Date.now();
}

async function iniciarJobImportacao(jobRef, data) {
  const db = getDb();

  return db.runTransaction(async (transaction) => {
    const snap = await transaction.get(jobRef);

    if (snap.exists) {
      const job = snap.data();
      if (job.status === "processing" || job.status === "processed") {
        return {
          locked: false,
          status: job.status,
        };
      }
    }

    const payload = {
      import_id: data.importId,
      empresa_id: data.empresaId,
      tipo_arquivo: data.tipoArquivo,
      colecao: data.nomeColecao,
      file_path_original: data.filePath,
      file_path_atual: data.filePath,
      file_name: data.fileName,
      status: "processing",
      tentativas: admin.firestore.FieldValue.increment(1),
      started_at: admin.firestore.FieldValue.serverTimestamp(),
      updated_at: admin.firestore.FieldValue.serverTimestamp(),
      erro: null,
    };

    if (!snap.exists) {
      payload.created_at = admin.firestore.FieldValue.serverTimestamp();
    }

    transaction.set(jobRef, payload, {merge: true});

    return {
      locked: true,
      status: "processing",
    };
  });
}

async function finalizarJobImportacao(jobRef, data) {
  const payload = {
    status: data.status,
    file_path_destino: data.destino,
    updated_at: admin.firestore.FieldValue.serverTimestamp(),
    finished_at: admin.firestore.FieldValue.serverTimestamp(),
  };

  if (data.linhas !== undefined) {
    payload.linhas_processadas = data.linhas;
  }

  if (data.erro) {
    payload.erro = data.erro;
  }

  await jobRef.set(payload, {merge: true});
}

async function moverArquivoSePossivel(bucket, origem, destino) {
  if (origem === destino) {
    return;
  }

  const [exists] = await bucket.file(origem).exists();
  if (!exists) {
    return;
  }

  await bucket.file(origem).move(destino);
}

function identificarArquivo(filePath) {
  const partes = filePath.split("/");
  const fileName = path.basename(filePath);
  const nomeSemExtensao = path.basename(fileName, ".csv");

  const match = nomeSemExtensao.match(
    /^(.+)_(vendas|estoque|produto|produtos)_\d{2}_\d{2}_\d{4}$/i,
  );

  if (match) {
    return {
      empresaId: match[1],
      tipoArquivo: match[2].toLowerCase(),
      fileName,
    };
  }

  if (partes.length >= 3) {
    return {
      empresaId: partes[1],
      tipoArquivo: nomeSemExtensao.toLowerCase(),
      fileName,
    };
  }

  throw new Error(`Nome de arquivo invalido: ${fileName}`);
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

  throw new Error(`Tipo de arquivo invalido: ${tipoArquivo}`);
}

function lerCsv(tempFilePath, empresaId) {
  return new Promise((resolve, reject) => {
    const linhas = [];

    fs.createReadStream(tempFilePath)
      .pipe(csv({
        separator: ",",
        mapHeaders: ({header}) => header.trim().replace(/^\uFEFF/, ""),
        mapValues: ({value}) => typeof value === "string" ? value.trim() : value,
      }))
      .on("data", (data) => {
        const itemTratado = {};

        for (const chaveOriginal in data) {
          const chave = chaveOriginal.trim().replace(/^\uFEFF/, "");
          itemTratado[chave] = converterValorCsv(chave, data[chaveOriginal]);
        }

        itemTratado.empresa_id = empresaId;
        linhas.push(itemTratado);
      })
      .on("end", () => resolve(linhas))
      .on("error", reject);
  });
}

async function salvarLinhasNoFirestore(nomeColecao, linhas) {
  const db = getDb();
  const writer = new BatchWriter(db);

  for (const item of linhas) {
    let documentId = item.id;

    if (nomeColecao === "estoque") {
      documentId = item.produto_id || item.id;
    }

    if (nomeColecao === "produtos") {
      documentId = item.id || item.produto_id || item.codigo || item.cod_produto;
    }

    if (nomeColecao === "vendas") {
      documentId = item.venda_id || item.id || db.collection(nomeColecao).doc().id;
    }

    if (!documentId) {
      documentId = db.collection(nomeColecao).doc().id;
    }

    const tenantDocId = `${safeDocId(item.empresa_id)}_${safeDocId(String(documentId).trim())}`;
    await writer.set(db.collection(nomeColecao).doc(tenantDocId), item);
  }

  await writer.commit();
}

module.exports = {
  processarUploadsPendentes,
  processarArquivoCsv,
  identificarArquivo,
  obterNomeColecao,
};
