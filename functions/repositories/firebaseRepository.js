const admin = require("firebase-admin");
const {safeDocId, cryptoSafeId} = require("../analytics/utils");

const DEFAULT_STORAGE_BUCKET = "datadriven-4816c.firebasestorage.app";

function initializeFirebase() {
  if (admin.apps.length > 0) {
    return;
  }

  const serviceAccountBase64 = process.env.FIREBASE_SERVICE_ACCOUNT_BASE64;
  const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;

  if (serviceAccountBase64) {
    const json = Buffer.from(serviceAccountBase64, "base64").toString("utf8");
    const serviceAccount = JSON.parse(json);
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
  return admin.firestore();
}

function getStorageBucketName(projectId = null) {
  const explicitBucket = (process.env.FIREBASE_STORAGE_BUCKET || "").trim();
  if (explicitBucket) {
    return explicitBucket;
  }

  if (admin.apps.length > 0 && admin.app().options.storageBucket) {
    return admin.app().options.storageBucket;
  }

  if (projectId) {
    return `${projectId}.firebasestorage.app`;
  }

  return DEFAULT_STORAGE_BUCKET;
}

function getStorageBucket() {
  const bucketName = getStorageBucketName();
  if (!bucketName) {
    throw new Error(
      "Bucket do Firebase Storage nao configurado. Defina FIREBASE_STORAGE_BUCKET no Render.",
    );
  }

  return admin.storage().bucket(bucketName);
}

async function getTenantCollection(collectionName, empresaId) {
  const collectionRef = getDb().collection(collectionName);

  if (!empresaId) {
    return collectionRef.get();
  }

  return collectionRef.where("empresa_id", "==", empresaId).get();
}

async function replaceCollection(collectionName, rows, mapper, empresaId = null) {
  const db = getDb();
  const collectionRef = db.collection(collectionName);
  const existing = empresaId ?
    await collectionRef.where("empresa_id", "==", empresaId).get() :
    await collectionRef.get();
  const writer = new BatchWriter(db);

  for (const doc of existing.docs) {
    await writer.delete(doc.ref);
  }

  for (const row of rows) {
    const rawId = row.id || row.produtoId || cryptoSafeId();
    const tenantPrefix = empresaId ? `${safeDocId(empresaId)}_` : "";
    await writer.set(collectionRef.doc(`${tenantPrefix}${safeDocId(rawId)}`), {
      ...mapper(row),
      empresa_id: empresaId || row.empresaId || null,
      atualizado_em: admin.firestore.FieldValue.serverTimestamp(),
    });
  }

  await writer.commit();
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

module.exports = {
  admin,
  initializeFirebase,
  getDb,
  getStorageBucket,
  getTenantCollection,
  replaceCollection,
  BatchWriter,
};
