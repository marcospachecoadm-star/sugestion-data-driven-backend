import 'dart:async';

import 'package:collection/collection.dart';

import '/backend/schema/util/firestore_util.dart';
import '/backend/schema/util/schema_util.dart';

import 'index.dart';
import '/flutter_flow/flutter_flow_util.dart';

class IndicadoresResumoRecord extends FirestoreRecord {
  IndicadoresResumoRecord._(
    DocumentReference reference,
    Map<String, dynamic> data,
  ) : super(reference, data) {
    _initializeFields();
  }

  // "total_vendas_formatado" field.
  String? _totalVendasFormatado;
  String get totalVendasFormatado => _totalVendasFormatado ?? '';
  bool hasTotalVendasFormatado() => _totalVendasFormatado != null;

  // "disponibilidade_osa_formatada" field.
  String? _disponibilidadeOsaFormatada;
  String get disponibilidadeOsaFormatada => _disponibilidadeOsaFormatada ?? '';
  bool hasDisponibilidadeOsaFormatada() => _disponibilidadeOsaFormatada != null;

  // "itens_criticos" field.
  int? _itensCriticos;
  int get itensCriticos => _itensCriticos ?? 0;
  bool hasItensCriticos() => _itensCriticos != null;

  // "alertas_pendentes" field.
  int? _alertasPendentes;
  int get alertasPendentes => _alertasPendentes ?? 0;
  bool hasAlertasPendentes() => _alertasPendentes != null;

  // "itens_sem_vendas" field.
  int? _itensSemVendas;
  int get itensSemVendas => _itensSemVendas ?? 0;
  bool hasItensSemVendas() => _itensSemVendas != null;

  // "investimento_sugerido_formatado" field.
  String? _investimentoSugeridoFormatado;
  String get investimentoSugeridoFormatado =>
      _investimentoSugeridoFormatado ?? '';
  bool hasInvestimentoSugeridoFormatado() =>
      _investimentoSugeridoFormatado != null;

  // "acoes_recomendadas" field.
  int? _acoesRecomendadas;
  int get acoesRecomendadas => _acoesRecomendadas ?? 0;
  bool hasAcoesRecomendadas() => _acoesRecomendadas != null;

  // "sugestoes_compra" field.
  int? _sugestoesCompra;
  int get sugestoesCompra => _sugestoesCompra ?? 0;
  bool hasSugestoesCompra() => _sugestoesCompra != null;

  // "reposicao_urgente" field.
  int? _reposicaoUrgente;
  int get reposicaoUrgente => _reposicaoUrgente ?? 0;
  bool hasReposicaoUrgente() => _reposicaoUrgente != null;

  // "giro_medio_45d" field.
  double? _giroMedio45d;
  double get giroMedio45d => _giroMedio45d ?? 0.0;
  bool hasGiroMedio45d() => _giroMedio45d != null;

  // "unidades_vendidas_45d" field.
  double? _unidadesVendidas45d;
  double get unidadesVendidas45d => _unidadesVendidas45d ?? 0.0;
  bool hasUnidadesVendidas45d() => _unidadesVendidas45d != null;

  // "giro_medio_status_label" field.
  String? _giroMedioStatusLabel;
  String get giroMedioStatusLabel => _giroMedioStatusLabel ?? '';
  bool hasGiroMedioStatusLabel() => _giroMedioStatusLabel != null;

  // "itens_ruptura" field.
  int? _itensRuptura;
  int get itensRuptura => _itensRuptura ?? 0;
  bool hasItensRuptura() => _itensRuptura != null;

  // "itens_abaixo_minimo" field.
  int? _itensAbaixoMinimo;
  int get itensAbaixoMinimo => _itensAbaixoMinimo ?? 0;
  bool hasItensAbaixoMinimo() => _itensAbaixoMinimo != null;

  // "venda_perdida_estimada_formatada" field.
  String? _vendaPerdidaEstimadaFormatada;
  String get vendaPerdidaEstimadaFormatada =>
      _vendaPerdidaEstimadaFormatada ?? '';
  bool hasVendaPerdidaEstimadaFormatada() =>
      _vendaPerdidaEstimadaFormatada != null;

  // "empresa_id" field.
  String? _empresaId;
  String get empresaId => _empresaId ?? '';
  bool hasEmpresaId() => _empresaId != null;

  // "valor_parado_formatado" field.
  String? _valorParadoFormatado;
  String get valorParadoFormatado => _valorParadoFormatado ?? '';
  bool hasValorParadoFormatado() => _valorParadoFormatado != null;

  // "taxa_ruptura_formatada" field.
  String? _taxaRupturaFormatada;
  String get taxaRupturaFormatada => _taxaRupturaFormatada ?? '';
  bool hasTaxaRupturaFormatada() => _taxaRupturaFormatada != null;

  // "tipo" field.
  String? _tipo;
  String get tipo => _tipo ?? '';
  bool hasTipo() => _tipo != null;

  // "estoque_atual" field.
  int? _estoqueAtual;
  int get estoqueAtual => _estoqueAtual ?? 0;
  bool hasEstoqueAtual() => _estoqueAtual != null;

  // "produto_id" field.
  String? _produtoId;
  String get produtoId => _produtoId ?? '';
  bool hasProdutoId() => _produtoId != null;

  // "produto_nome" field.
  String? _produtoNome;
  String get produtoNome => _produtoNome ?? '';
  bool hasProdutoNome() => _produtoNome != null;

  // "valor_impacto_formatado" field.
  String? _valorImpactoFormatado;
  String get valorImpactoFormatado => _valorImpactoFormatado ?? '';
  bool hasValorImpactoFormatado() => _valorImpactoFormatado != null;

  // "criado_em" field.
  DateTime? _criadoEm;
  DateTime? get criadoEm => _criadoEm;
  bool hasCriadoEm() => _criadoEm != null;

  // "valor_total_itens_sem_venda_formatado" field.
  String? _valorTotalItensSemVendaFormatado;
  String get valorTotalItensSemVendaFormatado =>
      _valorTotalItensSemVendaFormatado ?? '';
  bool hasValorTotalItensSemVendaFormatado() =>
      _valorTotalItensSemVendaFormatado != null;

  // "giro_medio_formatado" field.
  String? _giroMedioFormatado;
  String get giroMedioFormatado => _giroMedioFormatado ?? '';
  bool hasGiroMedioFormatado() => _giroMedioFormatado != null;

  // "itens_estoque_negativo" field.
  int? _itensEstoqueNegativo;
  int get itensEstoqueNegativo => _itensEstoqueNegativo ?? 0;
  bool hasItensEstoqueNegativo() => _itensEstoqueNegativo != null;

  // "valor_total_itens_estoque_negativo_formatado" field.
  String? _valorTotalItensEstoqueNegativoFormatado;
  String get valorTotalItensEstoqueNegativoFormatado =>
      _valorTotalItensEstoqueNegativoFormatado ?? '';
  bool hasValorTotalItensEstoqueNegativoFormatado() =>
      _valorTotalItensEstoqueNegativoFormatado != null;

  // "valor_perda_ruptura_formatado" field.
  String? _valorPerdaRupturaFormatado;
  String get valorPerdaRupturaFormatado => _valorPerdaRupturaFormatado ?? '';
  bool hasValorPerdaRupturaFormatado() => _valorPerdaRupturaFormatado != null;

  void _initializeFields() {
    _totalVendasFormatado = snapshotData['total_vendas_formatado'] as String?;
    _disponibilidadeOsaFormatada =
        snapshotData['disponibilidade_osa_formatada'] as String?;
    _itensCriticos = castToType<int>(snapshotData['itens_criticos']);
    _alertasPendentes = castToType<int>(snapshotData['alertas_pendentes']);
    _itensSemVendas = castToType<int>(snapshotData['itens_sem_vendas']);
    _investimentoSugeridoFormatado =
        snapshotData['investimento_sugerido_formatado'] as String?;
    _acoesRecomendadas = castToType<int>(snapshotData['acoes_recomendadas']);
    _sugestoesCompra = castToType<int>(snapshotData['sugestoes_compra']);
    _reposicaoUrgente = castToType<int>(snapshotData['reposicao_urgente']);
    _giroMedio45d = castToType<double>(snapshotData['giro_medio_45d']);
    _unidadesVendidas45d =
        castToType<double>(snapshotData['unidades_vendidas_45d']);
    _giroMedioStatusLabel = snapshotData['giro_medio_status_label'] as String?;
    _itensRuptura = castToType<int>(snapshotData['itens_ruptura']);
    _itensAbaixoMinimo = castToType<int>(snapshotData['itens_abaixo_minimo']);
    _vendaPerdidaEstimadaFormatada =
        snapshotData['venda_perdida_estimada_formatada'] as String?;
    _empresaId = snapshotData['empresa_id'] as String?;
    _valorParadoFormatado = snapshotData['valor_parado_formatado'] as String?;
    _taxaRupturaFormatada = snapshotData['taxa_ruptura_formatada'] as String?;
    _tipo = snapshotData['tipo'] as String?;
    _estoqueAtual = castToType<int>(snapshotData['estoque_atual']);
    _produtoId = snapshotData['produto_id'] as String?;
    _produtoNome = snapshotData['produto_nome'] as String?;
    _valorImpactoFormatado = snapshotData['valor_impacto_formatado'] as String?;
    _criadoEm = snapshotData['criado_em'] as DateTime?;
    _valorTotalItensSemVendaFormatado =
        snapshotData['valor_total_itens_sem_venda_formatado'] as String?;
    _giroMedioFormatado = snapshotData['giro_medio_formatado'] as String?;
    _itensEstoqueNegativo =
        castToType<int>(snapshotData['itens_estoque_negativo']);
    _valorTotalItensEstoqueNegativoFormatado =
        snapshotData['valor_total_itens_estoque_negativo_formatado'] as String?;
    _valorPerdaRupturaFormatado =
        snapshotData['valor_perda_ruptura_formatado'] as String?;
  }

  static CollectionReference get collection =>
      FirebaseFirestore.instance.collection('indicadoresResumo');

  static Stream<IndicadoresResumoRecord> getDocument(DocumentReference ref) =>
      ref.snapshots().map((s) => IndicadoresResumoRecord.fromSnapshot(s));

  static Future<IndicadoresResumoRecord> getDocumentOnce(
          DocumentReference ref) =>
      ref.get().then((s) => IndicadoresResumoRecord.fromSnapshot(s));

  static IndicadoresResumoRecord fromSnapshot(DocumentSnapshot snapshot) =>
      IndicadoresResumoRecord._(
        snapshot.reference,
        mapFromFirestore(snapshot.data() as Map<String, dynamic>),
      );

  static IndicadoresResumoRecord getDocumentFromData(
    Map<String, dynamic> data,
    DocumentReference reference,
  ) =>
      IndicadoresResumoRecord._(reference, mapFromFirestore(data));

  @override
  String toString() =>
      'IndicadoresResumoRecord(reference: ${reference.path}, data: $snapshotData)';

  @override
  int get hashCode => reference.path.hashCode;

  @override
  bool operator ==(other) =>
      other is IndicadoresResumoRecord &&
      reference.path.hashCode == other.reference.path.hashCode;
}

Map<String, dynamic> createIndicadoresResumoRecordData({
  String? totalVendasFormatado,
  String? disponibilidadeOsaFormatada,
  int? itensCriticos,
  int? alertasPendentes,
  int? itensSemVendas,
  String? investimentoSugeridoFormatado,
  int? acoesRecomendadas,
  int? sugestoesCompra,
  int? reposicaoUrgente,
  double? giroMedio45d,
  double? unidadesVendidas45d,
  String? giroMedioStatusLabel,
  int? itensRuptura,
  int? itensAbaixoMinimo,
  String? vendaPerdidaEstimadaFormatada,
  String? empresaId,
  String? valorParadoFormatado,
  String? taxaRupturaFormatada,
  String? tipo,
  int? estoqueAtual,
  String? produtoId,
  String? produtoNome,
  String? valorImpactoFormatado,
  DateTime? criadoEm,
  String? valorTotalItensSemVendaFormatado,
  String? giroMedioFormatado,
  int? itensEstoqueNegativo,
  String? valorTotalItensEstoqueNegativoFormatado,
  String? valorPerdaRupturaFormatado,
}) {
  final firestoreData = mapToFirestore(
    <String, dynamic>{
      'total_vendas_formatado': totalVendasFormatado,
      'disponibilidade_osa_formatada': disponibilidadeOsaFormatada,
      'itens_criticos': itensCriticos,
      'alertas_pendentes': alertasPendentes,
      'itens_sem_vendas': itensSemVendas,
      'investimento_sugerido_formatado': investimentoSugeridoFormatado,
      'acoes_recomendadas': acoesRecomendadas,
      'sugestoes_compra': sugestoesCompra,
      'reposicao_urgente': reposicaoUrgente,
      'giro_medio_45d': giroMedio45d,
      'unidades_vendidas_45d': unidadesVendidas45d,
      'giro_medio_status_label': giroMedioStatusLabel,
      'itens_ruptura': itensRuptura,
      'itens_abaixo_minimo': itensAbaixoMinimo,
      'venda_perdida_estimada_formatada': vendaPerdidaEstimadaFormatada,
      'empresa_id': empresaId,
      'valor_parado_formatado': valorParadoFormatado,
      'taxa_ruptura_formatada': taxaRupturaFormatada,
      'tipo': tipo,
      'estoque_atual': estoqueAtual,
      'produto_id': produtoId,
      'produto_nome': produtoNome,
      'valor_impacto_formatado': valorImpactoFormatado,
      'criado_em': criadoEm,
      'valor_total_itens_sem_venda_formatado': valorTotalItensSemVendaFormatado,
      'giro_medio_formatado': giroMedioFormatado,
      'itens_estoque_negativo': itensEstoqueNegativo,
      'valor_total_itens_estoque_negativo_formatado':
          valorTotalItensEstoqueNegativoFormatado,
      'valor_perda_ruptura_formatado': valorPerdaRupturaFormatado,
    }.withoutNulls,
  );

  return firestoreData;
}

class IndicadoresResumoRecordDocumentEquality
    implements Equality<IndicadoresResumoRecord> {
  const IndicadoresResumoRecordDocumentEquality();

  @override
  bool equals(IndicadoresResumoRecord? e1, IndicadoresResumoRecord? e2) {
    return e1?.totalVendasFormatado == e2?.totalVendasFormatado &&
        e1?.disponibilidadeOsaFormatada == e2?.disponibilidadeOsaFormatada &&
        e1?.itensCriticos == e2?.itensCriticos &&
        e1?.alertasPendentes == e2?.alertasPendentes &&
        e1?.itensSemVendas == e2?.itensSemVendas &&
        e1?.investimentoSugeridoFormatado ==
            e2?.investimentoSugeridoFormatado &&
        e1?.acoesRecomendadas == e2?.acoesRecomendadas &&
        e1?.sugestoesCompra == e2?.sugestoesCompra &&
        e1?.reposicaoUrgente == e2?.reposicaoUrgente &&
        e1?.giroMedio45d == e2?.giroMedio45d &&
        e1?.unidadesVendidas45d == e2?.unidadesVendidas45d &&
        e1?.giroMedioStatusLabel == e2?.giroMedioStatusLabel &&
        e1?.itensRuptura == e2?.itensRuptura &&
        e1?.itensAbaixoMinimo == e2?.itensAbaixoMinimo &&
        e1?.vendaPerdidaEstimadaFormatada ==
            e2?.vendaPerdidaEstimadaFormatada &&
        e1?.empresaId == e2?.empresaId &&
        e1?.valorParadoFormatado == e2?.valorParadoFormatado &&
        e1?.taxaRupturaFormatada == e2?.taxaRupturaFormatada &&
        e1?.tipo == e2?.tipo &&
        e1?.estoqueAtual == e2?.estoqueAtual &&
        e1?.produtoId == e2?.produtoId &&
        e1?.produtoNome == e2?.produtoNome &&
        e1?.valorImpactoFormatado == e2?.valorImpactoFormatado &&
        e1?.criadoEm == e2?.criadoEm &&
        e1?.valorTotalItensSemVendaFormatado ==
            e2?.valorTotalItensSemVendaFormatado &&
        e1?.giroMedioFormatado == e2?.giroMedioFormatado &&
        e1?.itensEstoqueNegativo == e2?.itensEstoqueNegativo &&
        e1?.valorTotalItensEstoqueNegativoFormatado ==
            e2?.valorTotalItensEstoqueNegativoFormatado &&
        e1?.valorPerdaRupturaFormatado == e2?.valorPerdaRupturaFormatado;
  }

  @override
  int hash(IndicadoresResumoRecord? e) => const ListEquality().hash([
        e?.totalVendasFormatado,
        e?.disponibilidadeOsaFormatada,
        e?.itensCriticos,
        e?.alertasPendentes,
        e?.itensSemVendas,
        e?.investimentoSugeridoFormatado,
        e?.acoesRecomendadas,
        e?.sugestoesCompra,
        e?.reposicaoUrgente,
        e?.giroMedio45d,
        e?.unidadesVendidas45d,
        e?.giroMedioStatusLabel,
        e?.itensRuptura,
        e?.itensAbaixoMinimo,
        e?.vendaPerdidaEstimadaFormatada,
        e?.empresaId,
        e?.valorParadoFormatado,
        e?.taxaRupturaFormatada,
        e?.tipo,
        e?.estoqueAtual,
        e?.produtoId,
        e?.produtoNome,
        e?.valorImpactoFormatado,
        e?.criadoEm,
        e?.valorTotalItensSemVendaFormatado,
        e?.giroMedioFormatado,
        e?.itensEstoqueNegativo,
        e?.valorTotalItensEstoqueNegativoFormatado,
        e?.valorPerdaRupturaFormatado
      ]);

  @override
  bool isValidKey(Object? o) => o is IndicadoresResumoRecord;
}
