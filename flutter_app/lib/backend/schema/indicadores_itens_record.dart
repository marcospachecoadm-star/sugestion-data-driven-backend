import 'dart:async';

import 'package:collection/collection.dart';

import '/backend/schema/util/firestore_util.dart';
import '/backend/schema/util/schema_util.dart';

import 'index.dart';
import '/flutter_flow/flutter_flow_util.dart';

class IndicadoresItensRecord extends FirestoreRecord {
  IndicadoresItensRecord._(
    DocumentReference reference,
    Map<String, dynamic> data,
  ) : super(reference, data) {
    _initializeFields();
  }

  // "produto_nome" field.
  String? _produtoNome;
  String get produtoNome => _produtoNome ?? '';
  bool hasProdutoNome() => _produtoNome != null;

  // "produto_id" field.
  String? _produtoId;
  String get produtoId => _produtoId ?? '';
  bool hasProdutoId() => _produtoId != null;

  // "giro_diario" field.
  double? _giroDiario;
  double get giroDiario => _giroDiario ?? 0.0;
  bool hasGiroDiario() => _giroDiario != null;

  // "estoque_atual" field.
  int? _estoqueAtual;
  int get estoqueAtual => _estoqueAtual ?? 0;
  bool hasEstoqueAtual() => _estoqueAtual != null;

  // "empresa_id" field.
  String? _empresaId;
  String get empresaId => _empresaId ?? '';
  bool hasEmpresaId() => _empresaId != null;

  // "indicador_tipo" field.
  String? _indicadorTipo;
  String get indicadorTipo => _indicadorTipo ?? '';
  bool hasIndicadorTipo() => _indicadorTipo != null;

  // "status_giro" field.
  String? _statusGiro;
  String get statusGiro => _statusGiro ?? '';
  bool hasStatusGiro() => _statusGiro != null;

  // "status_giro_label" field.
  String? _statusGiroLabel;
  String get statusGiroLabel => _statusGiroLabel ?? '';
  bool hasStatusGiroLabel() => _statusGiroLabel != null;

  // "vendas_45d_formatado" field.
  String? _vendas45dFormatado;
  String get vendas45dFormatado => _vendas45dFormatado ?? '';
  bool hasVendas45dFormatado() => _vendas45dFormatado != null;

  // "cobertura_dias_formatado" field.
  String? _coberturaDiasFormatado;
  String get coberturaDiasFormatado => _coberturaDiasFormatado ?? '';
  bool hasCoberturaDiasFormatado() => _coberturaDiasFormatado != null;

  // "giro_45d_formatado" field.
  String? _giro45dFormatado;
  String get giro45dFormatado => _giro45dFormatado ?? '';
  bool hasGiro45dFormatado() => _giro45dFormatado != null;

  // "cobertura_dias" field.
  double? _coberturaDias;
  double get coberturaDias => _coberturaDias ?? 0.0;
  bool hasCoberturaDias() => _coberturaDias != null;

  // "vendas_45d" field.
  int? _vendas45d;
  int get vendas45d => _vendas45d ?? 0;
  bool hasVendas45d() => _vendas45d != null;

  // "giro_45d" field.
  double? _giro45d;
  double get giro45d => _giro45d ?? 0.0;
  bool hasGiro45d() => _giro45d != null;

  // "estoque_minimo_calculado" field.
  int? _estoqueMinimoCalculado;
  int get estoqueMinimoCalculado => _estoqueMinimoCalculado ?? 0;
  bool hasEstoqueMinimoCalculado() => _estoqueMinimoCalculado != null;

  // "frequencia_venda_formatada" field.
  String? _frequenciaVendaFormatada;
  String get frequenciaVendaFormatada => _frequenciaVendaFormatada ?? '';
  bool hasFrequenciaVendaFormatada() => _frequenciaVendaFormatada != null;

  // "valor_parado_formatado" field.
  String? _valorParadoFormatado;
  String get valorParadoFormatado => _valorParadoFormatado ?? '';
  bool hasValorParadoFormatado() => _valorParadoFormatado != null;

  // "tipo" field.
  String? _tipo;
  String get tipo => _tipo ?? '';
  bool hasTipo() => _tipo != null;

  // "busca_tokens" field.
  String? _buscaTokens;
  String get buscaTokens => _buscaTokens ?? '';
  bool hasBuscaTokens() => _buscaTokens != null;

  void _initializeFields() {
    _produtoNome = snapshotData['produto_nome'] as String?;
    _produtoId = snapshotData['produto_id'] as String?;
    _giroDiario = castToType<double>(snapshotData['giro_diario']);
    _estoqueAtual = castToType<int>(snapshotData['estoque_atual']);
    _empresaId = snapshotData['empresa_id'] as String?;
    _indicadorTipo = snapshotData['indicador_tipo'] as String?;
    _statusGiro = snapshotData['status_giro'] as String?;
    _statusGiroLabel = snapshotData['status_giro_label'] as String?;
    _vendas45dFormatado = snapshotData['vendas_45d_formatado'] as String?;
    _coberturaDiasFormatado =
        snapshotData['cobertura_dias_formatado'] as String?;
    _giro45dFormatado = snapshotData['giro_45d_formatado'] as String?;
    _coberturaDias = castToType<double>(snapshotData['cobertura_dias']);
    _vendas45d = castToType<int>(snapshotData['vendas_45d']);
    _giro45d = castToType<double>(snapshotData['giro_45d']);
    _estoqueMinimoCalculado =
        castToType<int>(snapshotData['estoque_minimo_calculado']);
    _frequenciaVendaFormatada =
        snapshotData['frequencia_venda_formatada'] as String?;
    _valorParadoFormatado = snapshotData['valor_parado_formatado'] as String?;
    _tipo = snapshotData['tipo'] as String?;
    _buscaTokens = snapshotData['busca_tokens'] as String?;
  }

  static CollectionReference get collection =>
      FirebaseFirestore.instance.collection('indicadoresItens');

  static Stream<IndicadoresItensRecord> getDocument(DocumentReference ref) =>
      ref.snapshots().map((s) => IndicadoresItensRecord.fromSnapshot(s));

  static Future<IndicadoresItensRecord> getDocumentOnce(
          DocumentReference ref) =>
      ref.get().then((s) => IndicadoresItensRecord.fromSnapshot(s));

  static IndicadoresItensRecord fromSnapshot(DocumentSnapshot snapshot) =>
      IndicadoresItensRecord._(
        snapshot.reference,
        mapFromFirestore(snapshot.data() as Map<String, dynamic>),
      );

  static IndicadoresItensRecord getDocumentFromData(
    Map<String, dynamic> data,
    DocumentReference reference,
  ) =>
      IndicadoresItensRecord._(reference, mapFromFirestore(data));

  @override
  String toString() =>
      'IndicadoresItensRecord(reference: ${reference.path}, data: $snapshotData)';

  @override
  int get hashCode => reference.path.hashCode;

  @override
  bool operator ==(other) =>
      other is IndicadoresItensRecord &&
      reference.path.hashCode == other.reference.path.hashCode;
}

Map<String, dynamic> createIndicadoresItensRecordData({
  String? produtoNome,
  String? produtoId,
  double? giroDiario,
  int? estoqueAtual,
  String? empresaId,
  String? indicadorTipo,
  String? statusGiro,
  String? statusGiroLabel,
  String? vendas45dFormatado,
  String? coberturaDiasFormatado,
  String? giro45dFormatado,
  double? coberturaDias,
  int? vendas45d,
  double? giro45d,
  int? estoqueMinimoCalculado,
  String? frequenciaVendaFormatada,
  String? valorParadoFormatado,
  String? tipo,
  String? buscaTokens,
}) {
  final firestoreData = mapToFirestore(
    <String, dynamic>{
      'produto_nome': produtoNome,
      'produto_id': produtoId,
      'giro_diario': giroDiario,
      'estoque_atual': estoqueAtual,
      'empresa_id': empresaId,
      'indicador_tipo': indicadorTipo,
      'status_giro': statusGiro,
      'status_giro_label': statusGiroLabel,
      'vendas_45d_formatado': vendas45dFormatado,
      'cobertura_dias_formatado': coberturaDiasFormatado,
      'giro_45d_formatado': giro45dFormatado,
      'cobertura_dias': coberturaDias,
      'vendas_45d': vendas45d,
      'giro_45d': giro45d,
      'estoque_minimo_calculado': estoqueMinimoCalculado,
      'frequencia_venda_formatada': frequenciaVendaFormatada,
      'valor_parado_formatado': valorParadoFormatado,
      'tipo': tipo,
      'busca_tokens': buscaTokens,
    }.withoutNulls,
  );

  return firestoreData;
}

class IndicadoresItensRecordDocumentEquality
    implements Equality<IndicadoresItensRecord> {
  const IndicadoresItensRecordDocumentEquality();

  @override
  bool equals(IndicadoresItensRecord? e1, IndicadoresItensRecord? e2) {
    return e1?.produtoNome == e2?.produtoNome &&
        e1?.produtoId == e2?.produtoId &&
        e1?.giroDiario == e2?.giroDiario &&
        e1?.estoqueAtual == e2?.estoqueAtual &&
        e1?.empresaId == e2?.empresaId &&
        e1?.indicadorTipo == e2?.indicadorTipo &&
        e1?.statusGiro == e2?.statusGiro &&
        e1?.statusGiroLabel == e2?.statusGiroLabel &&
        e1?.vendas45dFormatado == e2?.vendas45dFormatado &&
        e1?.coberturaDiasFormatado == e2?.coberturaDiasFormatado &&
        e1?.giro45dFormatado == e2?.giro45dFormatado &&
        e1?.coberturaDias == e2?.coberturaDias &&
        e1?.vendas45d == e2?.vendas45d &&
        e1?.giro45d == e2?.giro45d &&
        e1?.estoqueMinimoCalculado == e2?.estoqueMinimoCalculado &&
        e1?.frequenciaVendaFormatada == e2?.frequenciaVendaFormatada &&
        e1?.valorParadoFormatado == e2?.valorParadoFormatado &&
        e1?.tipo == e2?.tipo &&
        e1?.buscaTokens == e2?.buscaTokens;
  }

  @override
  int hash(IndicadoresItensRecord? e) => const ListEquality().hash([
        e?.produtoNome,
        e?.produtoId,
        e?.giroDiario,
        e?.estoqueAtual,
        e?.empresaId,
        e?.indicadorTipo,
        e?.statusGiro,
        e?.statusGiroLabel,
        e?.vendas45dFormatado,
        e?.coberturaDiasFormatado,
        e?.giro45dFormatado,
        e?.coberturaDias,
        e?.vendas45d,
        e?.giro45d,
        e?.estoqueMinimoCalculado,
        e?.frequenciaVendaFormatada,
        e?.valorParadoFormatado,
        e?.tipo,
        e?.buscaTokens
      ]);

  @override
  bool isValidKey(Object? o) => o is IndicadoresItensRecord;
}
