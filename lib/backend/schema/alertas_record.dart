import 'dart:async';

import 'package:collection/collection.dart';

import '/backend/schema/util/firestore_util.dart';
import '/backend/schema/util/schema_util.dart';

import 'index.dart';
import '/flutter_flow/flutter_flow_util.dart';

class AlertasRecord extends FirestoreRecord {
  AlertasRecord._(
    DocumentReference reference,
    Map<String, dynamic> data,
  ) : super(reference, data) {
    _initializeFields();
  }

  // "empresa_id" field.
  String? _empresaId;
  String get empresaId => _empresaId ?? '';
  bool hasEmpresaId() => _empresaId != null;

  // "titulo" field.
  String? _titulo;
  String get titulo => _titulo ?? '';
  bool hasTitulo() => _titulo != null;

  // "descricao" field.
  String? _descricao;
  String get descricao => _descricao ?? '';
  bool hasDescricao() => _descricao != null;

  // "criado_em" field.
  DateTime? _criadoEm;
  DateTime? get criadoEm => _criadoEm;
  bool hasCriadoEm() => _criadoEm != null;

  // "tipo" field.
  String? _tipo;
  String get tipo => _tipo ?? '';
  bool hasTipo() => _tipo != null;

  // "produto_id" field.
  String? _produtoId;
  String get produtoId => _produtoId ?? '';
  bool hasProdutoId() => _produtoId != null;

  // "produto_nome" field.
  String? _produtoNome;
  String get produtoNome => _produtoNome ?? '';
  bool hasProdutoNome() => _produtoNome != null;

  // "estoque_atual" field.
  int? _estoqueAtual;
  int get estoqueAtual => _estoqueAtual ?? 0;
  bool hasEstoqueAtual() => _estoqueAtual != null;

  // "estoque_minimo" field.
  int? _estoqueMinimo;
  int get estoqueMinimo => _estoqueMinimo ?? 0;
  bool hasEstoqueMinimo() => _estoqueMinimo != null;

  void _initializeFields() {
    _empresaId = snapshotData['empresa_id'] as String?;
    _titulo = snapshotData['titulo'] as String?;
    _descricao = snapshotData['descricao'] as String?;
    _criadoEm = snapshotData['criado_em'] as DateTime?;
    _tipo = snapshotData['tipo'] as String?;
    _produtoId = snapshotData['produto_id'] as String?;
    _produtoNome = snapshotData['produto_nome'] as String?;
    _estoqueAtual = castToType<int>(snapshotData['estoque_atual']);
    _estoqueMinimo = castToType<int>(snapshotData['estoque_minimo']);
  }

  static CollectionReference get collection =>
      FirebaseFirestore.instance.collection('alertas');

  static Stream<AlertasRecord> getDocument(DocumentReference ref) =>
      ref.snapshots().map((s) => AlertasRecord.fromSnapshot(s));

  static Future<AlertasRecord> getDocumentOnce(DocumentReference ref) =>
      ref.get().then((s) => AlertasRecord.fromSnapshot(s));

  static AlertasRecord fromSnapshot(DocumentSnapshot snapshot) =>
      AlertasRecord._(
        snapshot.reference,
        mapFromFirestore(snapshot.data() as Map<String, dynamic>),
      );

  static AlertasRecord getDocumentFromData(
    Map<String, dynamic> data,
    DocumentReference reference,
  ) =>
      AlertasRecord._(reference, mapFromFirestore(data));

  @override
  String toString() =>
      'AlertasRecord(reference: ${reference.path}, data: $snapshotData)';

  @override
  int get hashCode => reference.path.hashCode;

  @override
  bool operator ==(other) =>
      other is AlertasRecord &&
      reference.path.hashCode == other.reference.path.hashCode;
}

Map<String, dynamic> createAlertasRecordData({
  String? empresaId,
  String? titulo,
  String? descricao,
  DateTime? criadoEm,
  String? tipo,
  String? produtoId,
  String? produtoNome,
  int? estoqueAtual,
  int? estoqueMinimo,
}) {
  final firestoreData = mapToFirestore(
    <String, dynamic>{
      'empresa_id': empresaId,
      'titulo': titulo,
      'descricao': descricao,
      'criado_em': criadoEm,
      'tipo': tipo,
      'produto_id': produtoId,
      'produto_nome': produtoNome,
      'estoque_atual': estoqueAtual,
      'estoque_minimo': estoqueMinimo,
    }.withoutNulls,
  );

  return firestoreData;
}

class AlertasRecordDocumentEquality implements Equality<AlertasRecord> {
  const AlertasRecordDocumentEquality();

  @override
  bool equals(AlertasRecord? e1, AlertasRecord? e2) {
    return e1?.empresaId == e2?.empresaId &&
        e1?.titulo == e2?.titulo &&
        e1?.descricao == e2?.descricao &&
        e1?.criadoEm == e2?.criadoEm &&
        e1?.tipo == e2?.tipo &&
        e1?.produtoId == e2?.produtoId &&
        e1?.produtoNome == e2?.produtoNome &&
        e1?.estoqueAtual == e2?.estoqueAtual &&
        e1?.estoqueMinimo == e2?.estoqueMinimo;
  }

  @override
  int hash(AlertasRecord? e) => const ListEquality().hash([
        e?.empresaId,
        e?.titulo,
        e?.descricao,
        e?.criadoEm,
        e?.tipo,
        e?.produtoId,
        e?.produtoNome,
        e?.estoqueAtual,
        e?.estoqueMinimo
      ]);

  @override
  bool isValidKey(Object? o) => o is AlertasRecord;
}
