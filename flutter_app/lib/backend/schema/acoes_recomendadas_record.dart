import 'dart:async';

import 'package:collection/collection.dart';

import '/backend/schema/util/firestore_util.dart';
import '/backend/schema/util/schema_util.dart';

import 'index.dart';
import '/flutter_flow/flutter_flow_util.dart';

class AcoesRecomendadasRecord extends FirestoreRecord {
  AcoesRecomendadasRecord._(
    DocumentReference reference,
    Map<String, dynamic> data,
  ) : super(reference, data) {
    _initializeFields();
  }

  // "titulo" field.
  String? _titulo;
  String get titulo => _titulo ?? '';
  bool hasTitulo() => _titulo != null;

  // "produto_nome" field.
  String? _produtoNome;
  String get produtoNome => _produtoNome ?? '';
  bool hasProdutoNome() => _produtoNome != null;

  // "prioridade" field.
  String? _prioridade;
  String get prioridade => _prioridade ?? '';
  bool hasPrioridade() => _prioridade != null;

  // "impacto" field.
  String? _impacto;
  String get impacto => _impacto ?? '';
  bool hasImpacto() => _impacto != null;

  // "indicador_tipo" field.
  String? _indicadorTipo;
  String get indicadorTipo => _indicadorTipo ?? '';
  bool hasIndicadorTipo() => _indicadorTipo != null;

  // "status" field.
  String? _status;
  String get status => _status ?? '';
  bool hasStatus() => _status != null;

  // "prioridade_score" field.
  double? _prioridadeScore;
  double get prioridadeScore => _prioridadeScore ?? 0.0;
  bool hasPrioridadeScore() => _prioridadeScore != null;

  // "produto_id" field.
  String? _produtoId;
  String get produtoId => _produtoId ?? '';
  bool hasProdutoId() => _produtoId != null;

  // "empresa_id" field.
  String? _empresaId;
  String get empresaId => _empresaId ?? '';
  bool hasEmpresaId() => _empresaId != null;

  // "criado_em" field.
  DateTime? _criadoEm;
  DateTime? get criadoEm => _criadoEm;
  bool hasCriadoEm() => _criadoEm != null;

  // "descricao" field.
  String? _descricao;
  String get descricao => _descricao ?? '';
  bool hasDescricao() => _descricao != null;

  // "valor_impacto_formatado" field.
  String? _valorImpactoFormatado;
  String get valorImpactoFormatado => _valorImpactoFormatado ?? '';
  bool hasValorImpactoFormatado() => _valorImpactoFormatado != null;

  // "estoque_atual" field.
  int? _estoqueAtual;
  int get estoqueAtual => _estoqueAtual ?? 0;
  bool hasEstoqueAtual() => _estoqueAtual != null;

  // "status_giro_label" field.
  String? _statusGiroLabel;
  String get statusGiroLabel => _statusGiroLabel ?? '';
  bool hasStatusGiroLabel() => _statusGiroLabel != null;

  // "itens_estoque_negativo" field.
  int? _itensEstoqueNegativo;
  int get itensEstoqueNegativo => _itensEstoqueNegativo ?? 0;
  bool hasItensEstoqueNegativo() => _itensEstoqueNegativo != null;

  // "itens_ruptura" field.
  int? _itensRuptura;
  int get itensRuptura => _itensRuptura ?? 0;
  bool hasItensRuptura() => _itensRuptura != null;

  // "abc_classe" field.
  String? _abcClasse;
  String get abcClasse => _abcClasse ?? '';
  bool hasAbcClasse() => _abcClasse != null;

  void _initializeFields() {
    _titulo = snapshotData['titulo'] as String?;
    _produtoNome = snapshotData['produto_nome'] as String?;
    _prioridade = snapshotData['prioridade'] as String?;
    _impacto = snapshotData['impacto'] as String?;
    _indicadorTipo = snapshotData['indicador_tipo'] as String?;
    _status = snapshotData['status'] as String?;
    _prioridadeScore = castToType<double>(snapshotData['prioridade_score']);
    _produtoId = snapshotData['produto_id'] as String?;
    _empresaId = snapshotData['empresa_id'] as String?;
    _criadoEm = snapshotData['criado_em'] as DateTime?;
    _descricao = snapshotData['descricao'] as String?;
    _valorImpactoFormatado = snapshotData['valor_impacto_formatado'] as String?;
    _estoqueAtual = castToType<int>(snapshotData['estoque_atual']);
    _statusGiroLabel = snapshotData['status_giro_label'] as String?;
    _itensEstoqueNegativo =
        castToType<int>(snapshotData['itens_estoque_negativo']);
    _itensRuptura = castToType<int>(snapshotData['itens_ruptura']);
    _abcClasse = snapshotData['abc_classe'] as String?;
  }

  static CollectionReference get collection =>
      FirebaseFirestore.instance.collection('acoesRecomendadas');

  static Stream<AcoesRecomendadasRecord> getDocument(DocumentReference ref) =>
      ref.snapshots().map((s) => AcoesRecomendadasRecord.fromSnapshot(s));

  static Future<AcoesRecomendadasRecord> getDocumentOnce(
          DocumentReference ref) =>
      ref.get().then((s) => AcoesRecomendadasRecord.fromSnapshot(s));

  static AcoesRecomendadasRecord fromSnapshot(DocumentSnapshot snapshot) =>
      AcoesRecomendadasRecord._(
        snapshot.reference,
        mapFromFirestore(snapshot.data() as Map<String, dynamic>),
      );

  static AcoesRecomendadasRecord getDocumentFromData(
    Map<String, dynamic> data,
    DocumentReference reference,
  ) =>
      AcoesRecomendadasRecord._(reference, mapFromFirestore(data));

  @override
  String toString() =>
      'AcoesRecomendadasRecord(reference: ${reference.path}, data: $snapshotData)';

  @override
  int get hashCode => reference.path.hashCode;

  @override
  bool operator ==(other) =>
      other is AcoesRecomendadasRecord &&
      reference.path.hashCode == other.reference.path.hashCode;
}

Map<String, dynamic> createAcoesRecomendadasRecordData({
  String? titulo,
  String? produtoNome,
  String? prioridade,
  String? impacto,
  String? indicadorTipo,
  String? status,
  double? prioridadeScore,
  String? produtoId,
  String? empresaId,
  DateTime? criadoEm,
  String? descricao,
  String? valorImpactoFormatado,
  int? estoqueAtual,
  String? statusGiroLabel,
  int? itensEstoqueNegativo,
  int? itensRuptura,
  String? abcClasse,
}) {
  final firestoreData = mapToFirestore(
    <String, dynamic>{
      'titulo': titulo,
      'produto_nome': produtoNome,
      'prioridade': prioridade,
      'impacto': impacto,
      'indicador_tipo': indicadorTipo,
      'status': status,
      'prioridade_score': prioridadeScore,
      'produto_id': produtoId,
      'empresa_id': empresaId,
      'criado_em': criadoEm,
      'descricao': descricao,
      'valor_impacto_formatado': valorImpactoFormatado,
      'estoque_atual': estoqueAtual,
      'status_giro_label': statusGiroLabel,
      'itens_estoque_negativo': itensEstoqueNegativo,
      'itens_ruptura': itensRuptura,
      'abc_classe': abcClasse,
    }.withoutNulls,
  );

  return firestoreData;
}

class AcoesRecomendadasRecordDocumentEquality
    implements Equality<AcoesRecomendadasRecord> {
  const AcoesRecomendadasRecordDocumentEquality();

  @override
  bool equals(AcoesRecomendadasRecord? e1, AcoesRecomendadasRecord? e2) {
    return e1?.titulo == e2?.titulo &&
        e1?.produtoNome == e2?.produtoNome &&
        e1?.prioridade == e2?.prioridade &&
        e1?.impacto == e2?.impacto &&
        e1?.indicadorTipo == e2?.indicadorTipo &&
        e1?.status == e2?.status &&
        e1?.prioridadeScore == e2?.prioridadeScore &&
        e1?.produtoId == e2?.produtoId &&
        e1?.empresaId == e2?.empresaId &&
        e1?.criadoEm == e2?.criadoEm &&
        e1?.descricao == e2?.descricao &&
        e1?.valorImpactoFormatado == e2?.valorImpactoFormatado &&
        e1?.estoqueAtual == e2?.estoqueAtual &&
        e1?.statusGiroLabel == e2?.statusGiroLabel &&
        e1?.itensEstoqueNegativo == e2?.itensEstoqueNegativo &&
        e1?.itensRuptura == e2?.itensRuptura &&
        e1?.abcClasse == e2?.abcClasse;
  }

  @override
  int hash(AcoesRecomendadasRecord? e) => const ListEquality().hash([
        e?.titulo,
        e?.produtoNome,
        e?.prioridade,
        e?.impacto,
        e?.indicadorTipo,
        e?.status,
        e?.prioridadeScore,
        e?.produtoId,
        e?.empresaId,
        e?.criadoEm,
        e?.descricao,
        e?.valorImpactoFormatado,
        e?.estoqueAtual,
        e?.statusGiroLabel,
        e?.itensEstoqueNegativo,
        e?.itensRuptura,
        e?.abcClasse
      ]);

  @override
  bool isValidKey(Object? o) => o is AcoesRecomendadasRecord;
}
