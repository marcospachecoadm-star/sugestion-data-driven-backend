import '/backend/api_requests/api_calls.dart';
import '/backend/backend.dart';
import '/auth/firebase_auth/auth_util.dart';
import '/components/card_acoes_recomendadas_oficial_widget.dart';
import '/components/card_acoes_recomendadas_widget.dart';
import '/components/header_indicador_widget.dart';
import '/flutter_flow/flutter_flow_theme.dart';
import '/flutter_flow/flutter_flow_util.dart';
import '/flutter_flow/flutter_flow_widgets.dart';
import 'dart:ui';
import 'package:easy_debounce/easy_debounce.dart';
import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:provider/provider.dart';
import 'e_acoes_recomendadas_oficial_model.dart';
export 'e_acoes_recomendadas_oficial_model.dart';

/// Crie uma tela mobile moderna e profissional para o módulo “Resumo Período”
/// do app Velyon.
///
/// A tela deve ter visual limpo, SaaS, responsivo e focado em leitura rápida
/// de indicadores de estoque e vendas.
///
/// ESTRUTURA OBRIGATÓRIA DA TELA
///
/// Column principal
///   Header fixo
///   Resumo do indicador
///   Busca
///   Expanded
///     ListView
///       ProductCard
///
/// Regras:
/// - Header, Resumo e Busca não devem rolar junto com a lista.
/// - Somente a ListView deve ter scroll vertical.
/// - A ListView precisa estar dentro de Expanded.
/// - Evitar overflow em telas pequenas.
/// - Layout mobile-first.
///
/// COMPONENTE 1: HeaderIndicador
///
/// Criar um header reutilizável com:
/// - Fundo azul escuro.
/// - Altura entre 72 e 88 px.
/// - Respeitar área segura do celular.
/// - Ícone de voltar à esquerda.
/// - Título centralizado.
/// - Ícone de lupa à direita.
/// - Bordas inferiores arredondadas.
/// - Padding horizontal confortável.
/// - O título deve aceitar texto dinâmico, exemplo: “Resumo Período”.
/// - A lupa deve funcionar como toggle para mostrar/recolher a busca.
///
/// Propriedades sugeridas:
/// - `titulo`
/// - `mostrarBusca`
/// - `onVoltar`
/// - `onToggleBusca`
///
/// COMPONENTE 2: IndicatorSummaryCard
///
/// Criar card pequeno de resumo para indicadores do topo.
///
/// Cada card deve ter:
/// - Label/título em fonte pequena.
/// - Valor em fonte maior e negrito.
/// - Fundo branco.
/// - Border radius entre 8 e 12.
/// - Sombra muito leve ou borda sutil.
/// - Alinhamento central.
/// - Altura consistente.
///
/// Usar dois cards lado a lado:
/// - Card 1:
///   - Label: “Giro”
///   - Valor: “2.0”
/// - Card 2:
///   - Label: “Vendas Totais”
///   - Valor: “2,00”
///
/// Propriedades sugeridas:
/// - `label`
/// - `valor`
/// - `valorFormatado`
/// - `corValor`
/// - `icone` opcional
///
/// COMPONENTE 3: SearchField
///
/// Criar campo de busca reutilizável.
///
/// Características:
/// - TextField real, não Placeholder.
/// - Placeholder: “Buscar produto ou SKU”.
/// - Fundo branco.
/// - Border radius 12.
/// - Altura aproximada de 44 a 48 px.
/// - Ícone de busca opcional.
/// - Texto e hint em cinza/azul discreto.
/// - Deve filtrar produtos por nome ou SKU.
/// - Deve aparecer/recolher quando tocar na lupa do header.
/// - Deve ter variável de estado para texto digitado.
///
/// Propriedades sugeridas:
/// - `hintText`
/// - `searchValue`
/// - `onChanged`
/// - `visible`
///
/// COMPONENTE 4: ProductCard
///
/// Criar card reutilizável para item da lista.
///
/// Cada card deve ter:
/// - Fundo branco.
/// - Border radius 12.
/// - Sombra leve.
/// - Padding interno confortável.
/// - Espaçamento vertical consistente.
/// - Altura consistente, sem overflow.
///
/// Conteúdo do card:
/// - Topo:
///   - “Produto:” + valor dinâmico `[produto]`
///   - À direita, badge/status `[coberturaStatus]`
/// - Segunda linha:
///   - “SKU:” + `[sku]`
/// - Divisor sutil.
/// - Linha inferior com quatro colunas:
///   - “Vendas 45d” + `[vendas45d]`
///   - “Giro Médio” + `[giroMedio]`
///   - “Estoque” + `[estoque]`
///   - “Cobertura” + `[cobertura]`
///
/// Badge/status:
/// - Deve ser um container pequeno com bordas arredondadas.
/// - Exemplo de status:
///   - “Saudável”
///   - “Crítico”
///   - “Ruptura”
/// - Cores:
///   - Saudável: verde
///   - Crítico: amarelo/laranja
///   - Ruptura: vermelho
///
/// Propriedades sugeridas:
/// - `produto`
/// - `sku`
/// - `coberturaStatus`
/// - `vendas45d`
/// - `giroMedio`
/// - `estoque`
/// - `cobertura`
/// - `statusColor`
/// - `statusBackgroundColor`
///
/// LAYOUT DA TELA
///
/// 1. Header
/// - Usar o componente HeaderIndicador.
/// - Título: “Resumo Período”.
///
/// 2. Resumo do indicador
/// - Container ou Row com dois IndicatorSummaryCard lado a lado.
/// - Usar Expanded nos cards para dividir igualmente o espaço.
/// - Espaçamento entre cards sutil.
/// - Fundo geral cinza claro.
///
/// 3. Busca
/// - Usar componente SearchField.
/// - Deve ficar abaixo do resumo.
/// - Não deve rolar junto com a lista.
/// - Visibilidade controlada por boolean `mostrarBusca`.
///
/// 4. Lista
/// - Usar Expanded envolvendo uma ListView.
/// - A ListView contém ProductCard.
/// - A lista deve rolar verticalmente.
/// - A lista não pode empurrar o Header, Resumo ou Busca.
/// - Não usar Column com query se a intenção for lista; usar ListView.
///
/// ESTADOS E VARIÁVEIS
///
/// Criar variáveis de Page State:
/// - `mostrarBusca` do tipo Boolean, default false.
/// - `textoBusca` do tipo String, default vazio.
///
/// Comportamento da lupa:
/// - Ao tocar na lupa:
///   - Se `mostrarBusca == false`, definir `mostrarBusca = true`.
///   - Se `mostrarBusca == true`, definir `mostrarBusca = false` e limpar
/// `textoBusca`.
///
/// Comportamento da busca:
/// - Ao digitar no TextField, atualizar `textoBusca`.
/// - Filtrar a lista por produto ou SKU.
/// - Se `textoBusca` estiver vazio, mostrar lista normal.
///
/// ESTILO VISUAL
///
/// - Paleta principal:
///   - Azul escuro para header: similar a `#002B5B` ou `#003B73`.
///   - Fundo geral: cinza muito claro.
///   - Cards: branco.
///   - Texto principal: azul escuro/cinza escuro.
///   - Texto secundário: cinza.
/// - Tipografia:
///   - Título do header em branco, semibold.
///   - Labels pequenos e discretos.
///   - Valores em destaque.
/// - Visual moderno, limpo e profissional.
/// - Interface deve parecer um dashboard operacional SaaS.
/// - Nada de elementos decorativos exagerados.
///
/// IMPORTANTE
///
/// - Criar usando componentes reutilizáveis sempre que possível.
/// - A tela deve servir como padrão para outros indicadores.
/// - Depois de criada, deve ser fácil duplicar e alterar apenas:
///   - título
///   - cards de resumo
///   - query
///   - campos exibidos no ProductCard
/// - Não usar Placeholder no lugar do TextField.
/// - Não deixar a ListView empurrar o TextField.
/// - A ListView deve estar dentro de Expanded.
/// - Header, resumo e busca devem permanecer fixos.
class EAcoesRecomendadasOficialWidget extends StatefulWidget {
  const EAcoesRecomendadasOficialWidget({super.key});

  static String routeName = 'E_AcoesRecomendadas_Oficial';
  static String routePath = '/eAcoesRecomendadasOficial';

  @override
  State<EAcoesRecomendadasOficialWidget> createState() =>
      _EAcoesRecomendadasOficialWidgetState();
}

class _EAcoesRecomendadasOficialWidgetState
    extends State<EAcoesRecomendadasOficialWidget> {
  late EAcoesRecomendadasOficialModel _model;

  final scaffoldKey = GlobalKey<ScaffoldState>();

  @override
  void initState() {
    super.initState();
    _model = createModel(context, () => EAcoesRecomendadasOficialModel());

    _model.textController ??= TextEditingController();
    _model.textFieldFocusNode ??= FocusNode();

    WidgetsBinding.instance.addPostFrameCallback((_) => safeSetState(() {}));
  }

  @override
  void dispose() {
    _model.dispose();

    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: () {
        FocusScope.of(context).unfocus();
        FocusManager.instance.primaryFocus?.unfocus();
      },
      child: Scaffold(
        key: scaffoldKey,
        backgroundColor: FlutterFlowTheme.of(context).primaryBackground,
        body: Column(
          mainAxisSize: MainAxisSize.max,
          mainAxisAlignment: MainAxisAlignment.start,
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            wrapWithModel(
              model: _model.headerIndicadorModel,
              updateCallback: () => safeSetState(() {}),
              child: HeaderIndicadorWidget(
                titulo: 'Ações Recomendadas',
                onSearchTap: () async {
                  if (_model.mostrarBusca) {
                    if (_model.buscaDigitada != null &&
                        _model.buscaDigitada != '') {
                      _model.buscarGiro = _model.buscaDigitada;
                      _model.mostrarBusca = false;
                      safeSetState(() {});
                    } else {
                      _model.buscarGiro = '${'__todos__'}';
                      _model.mostrarBusca = false;
                      safeSetState(() {});
                    }
                  } else {
                    _model.mostrarBusca = true;
                    safeSetState(() {});
                  }
                },
              ),
            ),
            Container(
              decoration: BoxDecoration(
                color: FlutterFlowTheme.of(context).primaryBackground,
                shape: BoxShape.rectangle,
              ),
              child: Padding(
                padding: EdgeInsets.all(10.0),
                child: StreamBuilder<List<IndicadoresResumoRecord>>(
                  stream: queryIndicadoresResumoRecord(
                    queryBuilder: (indicadoresResumoRecord) =>
                        indicadoresResumoRecord.where(
                      'empresa_id',
                      isEqualTo: currentUserEmpresaId,
                    ),
                    singleRecord: true,
                  ),
                  builder: (context, snapshot) {
                    // Customize what your widget looks like when it's loading.
                    if (!snapshot.hasData) {
                      return Center(
                        child: SizedBox(
                          width: 50.0,
                          height: 50.0,
                          child: CircularProgressIndicator(
                            valueColor: AlwaysStoppedAnimation<Color>(
                              FlutterFlowTheme.of(context).primary,
                            ),
                          ),
                        ),
                      );
                    }
                    List<IndicadoresResumoRecord>
                        cardAcoesRecomendadasIndicadoresResumoRecordList =
                        snapshot.data!;
                    // Return an empty Container when the item does not exist.
                    if (snapshot.data!.isEmpty) {
                      return Container();
                    }
                    final cardAcoesRecomendadasIndicadoresResumoRecord =
                        cardAcoesRecomendadasIndicadoresResumoRecordList
                                .isNotEmpty
                            ? cardAcoesRecomendadasIndicadoresResumoRecordList
                                .first
                            : null;

                    return wrapWithModel(
                      model: _model.cardAcoesRecomendadasModel,
                      updateCallback: () => safeSetState(() {}),
                      child: CardAcoesRecomendadasWidget(
                        parameter1: 0,
                      ),
                    );
                  },
                ),
              ),
            ),
            Padding(
              padding: EdgeInsetsDirectional.fromSTEB(3.0, 0.0, 3.0, 0.0),
              child: Row(
                mainAxisSize: MainAxisSize.max,
                children: [
                  if (_model.mostrarBusca)
                    Expanded(
                      child: Container(
                        width: 200.0,
                        child: TextFormField(
                          controller: _model.textController,
                          focusNode: _model.textFieldFocusNode,
                          onChanged: (_) => EasyDebounce.debounce(
                            '_model.textController',
                            Duration(milliseconds: 500),
                            () async {
                              _model.buscaDigitada = _model.textController.text;
                            },
                          ),
                          autofocus: false,
                          enabled: true,
                          obscureText: false,
                          decoration: InputDecoration(
                            isDense: true,
                            labelStyle: FlutterFlowTheme.of(context)
                                .labelMedium
                                .override(
                                  font: GoogleFonts.inter(
                                    fontWeight: FlutterFlowTheme.of(context)
                                        .labelMedium
                                        .fontWeight,
                                    fontStyle: FlutterFlowTheme.of(context)
                                        .labelMedium
                                        .fontStyle,
                                  ),
                                  letterSpacing: 0.0,
                                  fontWeight: FlutterFlowTheme.of(context)
                                      .labelMedium
                                      .fontWeight,
                                  fontStyle: FlutterFlowTheme.of(context)
                                      .labelMedium
                                      .fontStyle,
                                ),
                            hintText: 'Buscar produto ou SKU',
                            hintStyle: FlutterFlowTheme.of(context)
                                .labelMedium
                                .override(
                                  font: GoogleFonts.inter(
                                    fontWeight: FlutterFlowTheme.of(context)
                                        .labelMedium
                                        .fontWeight,
                                    fontStyle: FlutterFlowTheme.of(context)
                                        .labelMedium
                                        .fontStyle,
                                  ),
                                  color: Color(0xFF1A237E),
                                  letterSpacing: 0.0,
                                  fontWeight: FlutterFlowTheme.of(context)
                                      .labelMedium
                                      .fontWeight,
                                  fontStyle: FlutterFlowTheme.of(context)
                                      .labelMedium
                                      .fontStyle,
                                ),
                            enabledBorder: OutlineInputBorder(
                              borderSide: BorderSide(
                                color: Color(0x00000000),
                                width: 1.0,
                              ),
                              borderRadius: BorderRadius.circular(8.0),
                            ),
                            focusedBorder: OutlineInputBorder(
                              borderSide: BorderSide(
                                color: Color(0x00000000),
                                width: 1.0,
                              ),
                              borderRadius: BorderRadius.circular(8.0),
                            ),
                            errorBorder: OutlineInputBorder(
                              borderSide: BorderSide(
                                color: FlutterFlowTheme.of(context).error,
                                width: 1.0,
                              ),
                              borderRadius: BorderRadius.circular(8.0),
                            ),
                            focusedErrorBorder: OutlineInputBorder(
                              borderSide: BorderSide(
                                color: FlutterFlowTheme.of(context).error,
                                width: 1.0,
                              ),
                              borderRadius: BorderRadius.circular(8.0),
                            ),
                            filled: true,
                            fillColor: FlutterFlowTheme.of(context)
                                .secondaryBackground,
                          ),
                          style:
                              FlutterFlowTheme.of(context).bodyMedium.override(
                                    font: GoogleFonts.inter(
                                      fontWeight: FlutterFlowTheme.of(context)
                                          .bodyMedium
                                          .fontWeight,
                                      fontStyle: FlutterFlowTheme.of(context)
                                          .bodyMedium
                                          .fontStyle,
                                    ),
                                    letterSpacing: 0.0,
                                    fontWeight: FlutterFlowTheme.of(context)
                                        .bodyMedium
                                        .fontWeight,
                                    fontStyle: FlutterFlowTheme.of(context)
                                        .bodyMedium
                                        .fontStyle,
                                  ),
                          cursorColor: FlutterFlowTheme.of(context).primaryText,
                          enableInteractiveSelection: true,
                          validator: _model.textControllerValidator
                              .asValidator(context),
                        ),
                      ),
                    ),
                ],
              ),
            ),
            Expanded(
              flex: 1,
              child: Container(
                decoration: BoxDecoration(),
                child: Visibility(
                  visible: _model.buscarGiro == null || _model.buscarGiro == ''
                      ? false
                      : true,
                  child: Padding(
                    padding: EdgeInsets.all(24.0),
                    child: FutureBuilder<ApiCallResponse>(
                      future: BuscarIndicadoresItensCall.call(
                        indicadorTipo: 'acao_recomendada',
                        q: _model.buscarGiro,
                      ),
                      builder: (context, snapshot) {
                        // Customize what your widget looks like when it's loading.
                        if (!snapshot.hasData) {
                          return Center(
                            child: SizedBox(
                              width: 50.0,
                              height: 50.0,
                              child: CircularProgressIndicator(
                                valueColor: AlwaysStoppedAnimation<Color>(
                                  FlutterFlowTheme.of(context).primary,
                                ),
                              ),
                            ),
                          );
                        }
                        final listViewBuscarIndicadoresItensResponse =
                            snapshot.data!;

                        return Builder(
                          builder: (context) {
                            final itemIndicador =
                                BuscarIndicadoresItensCall.items(
                                      listViewBuscarIndicadoresItensResponse
                                          .jsonBody,
                                    )?.toList() ??
                                    [];

                            return ListView.separated(
                              padding: EdgeInsets.zero,
                              primary: false,
                              shrinkWrap: true,
                              scrollDirection: Axis.vertical,
                              itemCount: itemIndicador.length,
                              separatorBuilder: (_, __) =>
                                  SizedBox(height: 0.0),
                              itemBuilder: (context, itemIndicadorIndex) {
                                final itemIndicadorItem =
                                    itemIndicador[itemIndicadorIndex];
                                return CardAcoesRecomendadasOficialWidget(
                                  key: Key(
                                      'Key5h3_${itemIndicadorIndex}_of_${itemIndicador.length}'),
                                  produto: getJsonField(
                                    itemIndicadorItem,
                                    r'''$.produto_nome''',
                                  ).toString(),
                                  coberturaStatus: getJsonField(
                                    itemIndicadorItem,
                                    r'''$.abc_classe''',
                                  ).toString(),
                                  sku: getJsonField(
                                    itemIndicadorItem,
                                    r'''$.produto_id''',
                                  ).toString(),
                                  estoqueatual: getJsonField(
                                    itemIndicadorItem,
                                    r'''$.estoque_atual''',
                                  ).toString(),
                                  valorparado: getJsonField(
                                    itemIndicadorItem,
                                    r'''$.valor_impacto_formatado''',
                                  ).toString(),
                                  acao: getJsonField(
                                    itemIndicadorItem,
                                    r'''$.descricao''',
                                  ).toString(),
                                );
                              },
                            );
                          },
                        );
                      },
                    ),
                  ),
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }
}
