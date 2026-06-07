import '/backend/backend.dart';
import '/auth/firebase_auth/auth_util.dart';
import '/components/dashboard_metric_row4_widget.dart';
import '/components/dashboard_shortcut_card3_widget.dart';
import '/flutter_flow/flutter_flow_icon_button.dart';
import '/flutter_flow/flutter_flow_theme.dart';
import '/flutter_flow/flutter_flow_util.dart';
import '/flutter_flow/flutter_flow_widgets.dart';
import 'dart:ui';
import '/index.dart';
import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:provider/provider.dart';
import 'b_pagina_initial_oficial_model.dart';
export 'b_pagina_initial_oficial_model.dart';

/// Crie uma nova tela Dashboard para o app Velyon, do zero.
///
/// A tela deve ser MUITO parecida com a imagem de referência:
/// - Header azul/roxo escuro completo no topo.
/// - Título “Velyon”.
/// - Ícone X.
/// - Quatro atalhos coloridos no header.
/// - Fundo cinza claro.
/// - Seção “Resumo”.
/// - Card branco de resumo.
/// - Seção “Ações”.
/// - Card branco de ações.
/// - Ícones reais nas linhas, não quadrados.
/// - Valores coloridos à direita.
/// - Todos os campos separados para Binding.
///
/// NÃO reaproveite o layout anterior se ele estiver errado.
/// Não use quadrados coloridos no lugar de ícones.
/// Não corte o header.
/// Não crie placeholders.
///
/// ESTRUTURA OBRIGATÓRIA
///
/// Page
///   SafeArea
///     Column
///       HeaderDashboard
///       Expanded
///         ListView
///           AtualizadoEm
///           TituloResumo
///           CardResumo
///           TituloAcoes
///           CardAcoes
///
/// A tela deve caber perfeitamente em celular de 393px de largura.
///
/// HEADERDASHBOARD
///
/// Criar um container no topo com:
/// - Largura 100%.
/// - Altura suficiente para título e atalhos.
/// - Fundo azul/roxo escuro.
/// - Bordas inferiores arredondadas.
/// - Padding interno.
/// - Não pode ficar cortado.
/// - Deve aparecer inteiro.
///
/// Dentro do header:
///
/// Linha superior:
/// - À esquerda: Text “Velyon”.
/// - À direita: Icon X.
/// - Texto branco.
/// - Ícone branco.
///
/// Abaixo da linha superior:
/// - Row com quatro atalhos.
/// - Os quatro atalhos devem caber na largura do celular.
/// - Todos com o mesmo tamanho.
/// - Espaçamento uniforme entre eles.
///
/// ATALHOS DO HEADER
///
/// Criar quatro atalhos como cards separados:
///
/// 1. Giro Médio
/// 2. Estoque Negativo
/// 3. Alertas
/// 4. Sugestão Ações
///
/// Cada atalho deve ter:
/// - Card azul/roxo um pouco mais claro que o header.
/// - Border radius arredondado.
/// - Ícone circular no topo.
/// - Label abaixo do ícone.
/// - Texto branco.
/// - Label com no máximo duas linhas.
/// - Sem cortar texto.
/// - Sem ultrapassar a tela.
///
/// Cores dos ícones dos atalhos:
/// - Giro Médio: ícone azul.
/// - Estoque Negativo: ícone vermelho.
/// - Alertas: ícone amarelo.
/// - Sugestão Ações: ícone verde.
///
/// Todos os ícones devem ter o mesmo tamanho.
/// Todos os círculos dos ícones devem ter o mesmo tamanho.
/// Não usar a mesma cor em todos.
/// Logo abaixo do header, dentro da ListView:
/// - Criar uma linha centralizada.
/// - Ícone pequeno de atualização em cinza.
/// - Text separado: “Atualizado em:”
/// - Text separado: “[M/d H:mm]”
/// - Cor cinza discreta.
/// - Não usar barra grande.
/// - Não cortar.
///
/// SEÇÃO RESUMO
///
/// Criar Text separado:
/// - “Resumo”
/// - Alinhado à esquerda.
/// - Fonte semibold.
/// - Cor escura.
///
/// Criar um Card branco com:
/// - Border radius arredondado.
/// - Sombra leve.
/// - Padding interno.
/// - Divisores sutis entre linhas.
///
/// Dentro do CardResumo, criar três itens separados.
/// Cada item deve ser uma linha editável individualmente.
///
/// Item 1:
/// - Ícone real à esquerda: ícone de menos/círculo/alerta discreto.
/// - Label separado: “Estoque Negativo”
/// - Valor separado: “16”
/// - Cor do valor: vermelho.
///
/// Item 2:
/// - Ícone real à esquerda: ícone de exclamação/alerta.
/// - Label separado: “Ruptura”
/// - Valor separado: “16”
/// - Cor do valor: amarelo.
///
/// Item 3:
/// - Ícone real à esquerda: ícone de caixa/arquivo/cartão.
/// - Label separado: “Qtd Itens sem Vendas ”
/// - Valor separado: “R$ 84.572,02”
/// - Cor do valor: cinza escuro.
///
/// Item 4:
/// - Ícone real à esquerda: ícone de caixa/arquivo/cartão.
/// - Label separado: “Itens sem Vendas $”
/// - Valor separado: “R$ 84.572,02”
/// - Cor do valor: cinza escuro.
///
/// IMPORTANTE PARA AS LINHAS:
/// - Usar Icon real, não Container colorido.
/// - Não usar quadrados coloridos.
/// - Ícones das linhas devem ser cinza escuro.
/// - Label e valor devem ser widgets Text separados.
/// - O valor fica alinhado à direita.
/// - A linha deve ter altura confortável.
/// - Divisor sutil entre linhas.
///
/// SEÇÃO AÇÕES
///
/// Criar Text separado:
/// - “Ações”
/// - Alinhado à esquerda.
/// - Fonte semibold.
/// - Cor escura.
///
/// Criar Card branco com:
/// - Border radius arredondado.
/// - Sombra leve.
/// - Padding interno.
/// - Divisores sutis entre linhas.
///
/// Dentro do CardAcoes, criar três itens separados.
///
/// Item 1:
/// - Ícone real à esquerda: check/lista/estrela.
/// - Label separado: “Ações Recomendadas”
/// - Valor separado: “47”
/// - Cor do valor: azul.
///
/// Item 2:
/// - Ícone real à esquerda: lâmpada.
/// - Label separado: “Sugestões e Ações”
/// - Valor separado: “11”
/// - Cor do valor: verde.
///
/// Item 3:
/// - Ícone real à esquerda: carrinho/reposição.
/// - Label separado: “Itens para Reposição”
/// - Valor separado: “36”
/// - Cor do valor: vermelho.
///
/// IMPORTANTE PARA BINDING
///
/// Todos os campos devem ser separados e editáveis:
/// - Título “Velyon”
/// - Ícone X
/// - Label de cada atalho
/// - Ícone de cada atalho
/// - Texto “Atualizado em:”
/// - Valor da data/hora
/// - Título “Resumo”
/// - Label de cada linha do Resumo
/// - Valor de cada linha do Resumo
/// - Título “Ações”
/// - Label de cada linha de Ações
/// - Valor de cada linha de Ações
///
/// Não criar textos combinados.
/// Errado: “Alertas 16”
/// Certo:
/// - Text “Alertas”
/// - Text “16”
///
/// COMPONENTES
///
/// Pode criar componentes, mas seguindo estas regras:
///
/// 1. DashboardShortcutCard
/// - Um componente para cada atalho superior.
/// - Editável para label, ícone, cor e ação.
/// - Não criar parâmetros de layout.
///
/// 2. DashboardMetricRow
/// - Um componente para cada linha de Resumo e Ações.
/// - Deve conter Icon real + Text label + Text value.
/// - Editável para ícone, label, valor, cor do valor e ação.
/// - Não usar quadrado colorido como ícone.
/// - Não criar parâmetros de layout.
///
/// REGRAS VISUAIS
///
/// - Fundo geral cinza muito claro.
/// - Header azul/roxo escuro.
/// - Cards do header em azul/roxo mais claro.
/// - Cards de conteúdo brancos.
/// - Ícones das linhas em cinza escuro.
/// - Valores coloridos conforme indicador.
/// - Bordas arredondadas.
/// - Sombra leve.
/// - Espaçamento consistente.
/// - Visual moderno e profissional.
///
/// REGRAS DE RESPONSIVIDADE
///
/// - Caber em celular 393px.
/// - Não cortar header.
/// - Não cortar atalhos.
/// - Não cortar textos.
/// - Não cortar valores.
/// - Usar SafeArea.
/// - Usar Expanded com ListView para conteúdo rolável.
/// - Header fica fixo no topo.
/// - O conteúdo abaixo rola.
///
/// PROIBIDO
///
/// - Proibido usar quadrados coloridos no lugar dos ícones.
/// - Proibido cortar o header.
/// - Proibido criar placeholders.
/// - Proibido agrupar label e valor no mesmo texto.
/// - Proibido criar um card gigante com tudo difícil de editar.
/// - Proibido usar largura maior que a tela.
/// - Proibido deixar qualquer widget estourar a tela.
///
/// RESULTADO ESPERADO
///
/// Uma tela igual à referência original, moderna e limpa, com:
/// - Header completo.
/// - Atalhos coloridos.
/// - Cards brancos.
/// - Ícones reais nas linhas.
/// - Campos separados para Binding.
/// - Tudo editável individualmente.
/// - Tudo cabendo perfeitamente no celular.
class BPaginaInitialOficialWidget extends StatefulWidget {
  const BPaginaInitialOficialWidget({super.key});

  static String routeName = 'B_PaginaInitial_Oficial';
  static String routePath = '/bPaginaInitialOficial';

  @override
  State<BPaginaInitialOficialWidget> createState() =>
      _BPaginaInitialOficialWidgetState();
}

class _BPaginaInitialOficialWidgetState
    extends State<BPaginaInitialOficialWidget> {
  late BPaginaInitialOficialModel _model;

  final scaffoldKey = GlobalKey<ScaffoldState>();

  @override
  void initState() {
    super.initState();
    _model = createModel(context, () => BPaginaInitialOficialModel());

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
        backgroundColor: Color(0xFFF5F6F8),
        body: Column(
          mainAxisSize: MainAxisSize.max,
          mainAxisAlignment: MainAxisAlignment.start,
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Container(
              decoration: BoxDecoration(
                color: Color(0xFF1A237E),
                borderRadius: BorderRadius.only(
                  bottomLeft: Radius.circular(24.0),
                  bottomRight: Radius.circular(24.0),
                ),
                shape: BoxShape.rectangle,
              ),
              child: Padding(
                padding: EdgeInsetsDirectional.fromSTEB(24.0, 24.0, 24.0, 32.0),
                child: Container(
                  child: Column(
                    mainAxisSize: MainAxisSize.min,
                    mainAxisAlignment: MainAxisAlignment.start,
                    crossAxisAlignment: CrossAxisAlignment.stretch,
                    children: [
                      Row(
                        mainAxisSize: MainAxisSize.max,
                        mainAxisAlignment: MainAxisAlignment.spaceBetween,
                        crossAxisAlignment: CrossAxisAlignment.center,
                        children: [
                          Container(
                            width: 300.0,
                            height: 90.0,
                            decoration: BoxDecoration(
                              image: DecorationImage(
                                fit: BoxFit.cover,
                                image: Image.asset(
                                  'assets/images/Logo1_Velyon.png',
                                ).image,
                              ),
                              shape: BoxShape.rectangle,
                            ),
                          ),
                          FlutterFlowIconButton(
                            borderRadius: 8.0,
                            buttonSize: 40.0,
                            fillColor: Colors.transparent,
                            icon: Icon(
                              Icons.close_rounded,
                              color: Colors.white,
                              size: 24.0,
                            ),
                            onPressed: () async {
                              context.pushNamed(ALoginWidget.routeName);
                            },
                          ),
                        ],
                      ),
                      Row(
                        mainAxisSize: MainAxisSize.max,
                        mainAxisAlignment: MainAxisAlignment.spaceBetween,
                        crossAxisAlignment: CrossAxisAlignment.center,
                        children: [
                          Expanded(
                            flex: 1,
                            child: InkWell(
                              splashColor: Colors.transparent,
                              focusColor: Colors.transparent,
                              hoverColor: Colors.transparent,
                              highlightColor: Colors.transparent,
                              onTap: () async {
                                context.pushNamed(
                                    CResumoGiroOficialWidget.routeName);
                              },
                              child: wrapWithModel(
                                model: _model.dashboardShortcutCardModel1,
                                updateCallback: () => safeSetState(() {}),
                                child: DashboardShortcutCard3Widget(
                                  icon: Icon(
                                    Icons.trending_up_rounded,
                                    color: Color(0xFF42A5F5),
                                    size: 24.0,
                                  ),
                                  iconColor: Color(0xFF42A5F5),
                                  label: 'Giro Médio',
                                ),
                              ),
                            ),
                          ),
                          Expanded(
                            flex: 1,
                            child: InkWell(
                              splashColor: Colors.transparent,
                              focusColor: Colors.transparent,
                              hoverColor: Colors.transparent,
                              highlightColor: Colors.transparent,
                              onTap: () async {
                                context.pushNamed(
                                    DEstoqueNegativoOficialWidget.routeName);
                              },
                              child: wrapWithModel(
                                model: _model.dashboardShortcutCardModel2,
                                updateCallback: () => safeSetState(() {}),
                                child: DashboardShortcutCard3Widget(
                                  icon: Icon(
                                    Icons.remove_circle_outline_rounded,
                                    color: Color(0xFFEF5350),
                                    size: 24.0,
                                  ),
                                  iconColor: Color(0xFFEF5350),
                                  label: 'Estoque Negativo',
                                ),
                              ),
                            ),
                          ),
                          Expanded(
                            flex: 1,
                            child: InkWell(
                              splashColor: Colors.transparent,
                              focusColor: Colors.transparent,
                              hoverColor: Colors.transparent,
                              highlightColor: Colors.transparent,
                              onTap: () async {
                                context.pushNamed(
                                    FItensEmRupturaOficialWidget.routeName);
                              },
                              child: wrapWithModel(
                                model: _model.dashboardShortcutCardModel3,
                                updateCallback: () => safeSetState(() {}),
                                child: DashboardShortcutCard3Widget(
                                  icon: Icon(
                                    Icons.hide_source,
                                    color: FlutterFlowTheme.of(context).error,
                                    size: 24.0,
                                  ),
                                  iconColor: Color(0xFFFFCA28),
                                  label: 'Ruptura',
                                ),
                              ),
                            ),
                          ),
                          Expanded(
                            flex: 1,
                            child: InkWell(
                              splashColor: Colors.transparent,
                              focusColor: Colors.transparent,
                              hoverColor: Colors.transparent,
                              highlightColor: Colors.transparent,
                              onTap: () async {
                                context.pushNamed(
                                    GItensSemVendasOficialWidget.routeName);
                              },
                              child: wrapWithModel(
                                model: _model.dashboardShortcutCardModel4,
                                updateCallback: () => safeSetState(() {}),
                                child: DashboardShortcutCard3Widget(
                                  icon: Icon(
                                    Icons.notifications_active_outlined,
                                    color: FlutterFlowTheme.of(context).warning,
                                    size: 24.0,
                                  ),
                                  iconColor:
                                      FlutterFlowTheme.of(context).warning,
                                  label: 'Sem Vendas',
                                ),
                              ),
                            ),
                          ),
                        ].divide(SizedBox(width: 8.0)),
                      ),
                    ].divide(SizedBox(height: 24.0)),
                  ),
                ),
              ),
            ),
            Expanded(
              flex: 1,
              child: Container(
                child: SingleChildScrollView(
                  primary: false,
                  child: Column(
                    mainAxisSize: MainAxisSize.min,
                    mainAxisAlignment: MainAxisAlignment.start,
                    crossAxisAlignment: CrossAxisAlignment.stretch,
                    children: [
                      Padding(
                        padding: EdgeInsets.all(24.0),
                        child: Container(
                          child: Column(
                            mainAxisSize: MainAxisSize.min,
                            mainAxisAlignment: MainAxisAlignment.start,
                            crossAxisAlignment: CrossAxisAlignment.stretch,
                            children: [
                              Column(
                                mainAxisSize: MainAxisSize.min,
                                mainAxisAlignment: MainAxisAlignment.start,
                                crossAxisAlignment: CrossAxisAlignment.stretch,
                                children: [
                                  Text(
                                    'Resumo',
                                    style: FlutterFlowTheme.of(context)
                                        .titleMedium
                                        .override(
                                          font: GoogleFonts.plusJakartaSans(
                                            fontWeight: FontWeight.bold,
                                            fontStyle:
                                                FlutterFlowTheme.of(context)
                                                    .titleMedium
                                                    .fontStyle,
                                          ),
                                          color: Color(0xFF1A237E),
                                          letterSpacing: 0.0,
                                          fontWeight: FontWeight.bold,
                                          fontStyle:
                                              FlutterFlowTheme.of(context)
                                                  .titleMedium
                                                  .fontStyle,
                                          lineHeight: 1.4,
                                        ),
                                  ),
                                  StreamBuilder<List<IndicadoresResumoRecord>>(
                                    stream: queryIndicadoresResumoRecord(
                                      queryBuilder: (indicadoresResumoRecord) =>
                                          indicadoresResumoRecord.where(
                                        'empresa_id',
                                        isEqualTo: currentUserEmpresaId,
                                      ),
                                      limit: 4,
                                    ),
                                    builder: (context, snapshot) {
                                      // Customize what your widget looks like when it's loading.
                                      if (!snapshot.hasData) {
                                        return Center(
                                          child: SizedBox(
                                            width: 50.0,
                                            height: 50.0,
                                            child: CircularProgressIndicator(
                                              valueColor:
                                                  AlwaysStoppedAnimation<Color>(
                                                FlutterFlowTheme.of(context)
                                                    .primary,
                                              ),
                                            ),
                                          ),
                                        );
                                      }
                                      List<IndicadoresResumoRecord>
                                          containerIndicadoresResumoRecordList =
                                          snapshot.data!;

                                      return Container(
                                        decoration: BoxDecoration(
                                          color: FlutterFlowTheme.of(context)
                                              .secondaryBackground,
                                          borderRadius:
                                              BorderRadius.circular(24.0),
                                          shape: BoxShape.rectangle,
                                        ),
                                        child: Padding(
                                          padding:
                                              EdgeInsetsDirectional.fromSTEB(
                                                  24.0, 16.0, 24.0, 16.0),
                                          child: Container(
                                            child: Column(
                                              mainAxisSize: MainAxisSize.min,
                                              mainAxisAlignment:
                                                  MainAxisAlignment.start,
                                              crossAxisAlignment:
                                                  CrossAxisAlignment.center,
                                              children: [
                                                wrapWithModel(
                                                  model: _model
                                                      .dashboardMetricRowModel1,
                                                  updateCallback: () =>
                                                      safeSetState(() {}),
                                                  child:
                                                      DashboardMetricRow4Widget(
                                                    icon: Icon(
                                                      Icons
                                                          .remove_circle_outline_rounded,
                                                      color: Color(0xFF636366),
                                                      size: 22.0,
                                                    ),
                                                    label: 'Estoque Negativo',
                                                    value:
                                                        containerIndicadoresResumoRecordList
                                                            .firstOrNull
                                                            ?.itensEstoqueNegativo
                                                            ?.toString(),
                                                    valueColor:
                                                        FlutterFlowTheme.of(
                                                                context)
                                                            .error,
                                                    last: false,
                                                  ),
                                                ),
                                                wrapWithModel(
                                                  model: _model
                                                      .dashboardMetricRowModel2,
                                                  updateCallback: () =>
                                                      safeSetState(() {}),
                                                  child:
                                                      DashboardMetricRow4Widget(
                                                    icon: Icon(
                                                      Icons.hide_source,
                                                      color: Color(0xFF636366),
                                                      size: 22.0,
                                                    ),
                                                    label: 'Ruptura',
                                                    value:
                                                        containerIndicadoresResumoRecordList
                                                            .firstOrNull
                                                            ?.itensRuptura
                                                            ?.toString(),
                                                    valueColor:
                                                        FlutterFlowTheme.of(
                                                                context)
                                                            .error,
                                                    last: false,
                                                  ),
                                                ),
                                                wrapWithModel(
                                                  model: _model
                                                      .dashboardMetricRowModel3,
                                                  updateCallback: () =>
                                                      safeSetState(() {}),
                                                  child:
                                                      DashboardMetricRow4Widget(
                                                    icon: Icon(
                                                      Icons
                                                          .trending_down_outlined,
                                                      color: Color(0xFF636366),
                                                      size: 22.0,
                                                    ),
                                                    label:
                                                        'Qtd Itens sem Vendas',
                                                    value:
                                                        containerIndicadoresResumoRecordList
                                                            .firstOrNull
                                                            ?.itensSemVendas
                                                            ?.toString(),
                                                    valueColor:
                                                        FlutterFlowTheme.of(
                                                                context)
                                                            .warning,
                                                    last: false,
                                                  ),
                                                ),
                                                wrapWithModel(
                                                  model: _model
                                                      .dashboardMetricRowModel4,
                                                  updateCallback: () =>
                                                      safeSetState(() {}),
                                                  child:
                                                      DashboardMetricRow4Widget(
                                                    icon: Icon(
                                                      Icons.payments_rounded,
                                                      color: Color(0xFF636366),
                                                      size: 22.0,
                                                    ),
                                                    label:
                                                        'Itens sem Vendas \$',
                                                    value: containerIndicadoresResumoRecordList
                                                        .firstOrNull
                                                        ?.valorTotalItensSemVendaFormatado,
                                                    valueColor:
                                                        FlutterFlowTheme.of(
                                                                context)
                                                            .warning,
                                                    last: true,
                                                  ),
                                                ),
                                              ],
                                            ),
                                          ),
                                        ),
                                      );
                                    },
                                  ),
                                ].divide(SizedBox(height: 16.0)),
                              ),
                              Column(
                                mainAxisSize: MainAxisSize.min,
                                mainAxisAlignment: MainAxisAlignment.start,
                                crossAxisAlignment: CrossAxisAlignment.stretch,
                                children: [
                                  Text(
                                    'Ações',
                                    style: FlutterFlowTheme.of(context)
                                        .titleMedium
                                        .override(
                                          font: GoogleFonts.plusJakartaSans(
                                            fontWeight: FontWeight.bold,
                                            fontStyle:
                                                FlutterFlowTheme.of(context)
                                                    .titleMedium
                                                    .fontStyle,
                                          ),
                                          color: Color(0xFF1A237E),
                                          letterSpacing: 0.0,
                                          fontWeight: FontWeight.bold,
                                          fontStyle:
                                              FlutterFlowTheme.of(context)
                                                  .titleMedium
                                                  .fontStyle,
                                          lineHeight: 1.4,
                                        ),
                                  ),
                                  StreamBuilder<List<IndicadoresResumoRecord>>(
                                    stream: queryIndicadoresResumoRecord(
                                      queryBuilder: (indicadoresResumoRecord) =>
                                          indicadoresResumoRecord.where(
                                        'empresa_id',
                                        isEqualTo: currentUserEmpresaId,
                                      ),
                                      limit: 3,
                                    ),
                                    builder: (context, snapshot) {
                                      // Customize what your widget looks like when it's loading.
                                      if (!snapshot.hasData) {
                                        return Center(
                                          child: SizedBox(
                                            width: 50.0,
                                            height: 50.0,
                                            child: CircularProgressIndicator(
                                              valueColor:
                                                  AlwaysStoppedAnimation<Color>(
                                                FlutterFlowTheme.of(context)
                                                    .primary,
                                              ),
                                            ),
                                          ),
                                        );
                                      }
                                      List<IndicadoresResumoRecord>
                                          containerIndicadoresResumoRecordList =
                                          snapshot.data!;

                                      return Container(
                                        decoration: BoxDecoration(
                                          color: FlutterFlowTheme.of(context)
                                              .secondaryBackground,
                                          borderRadius:
                                              BorderRadius.circular(24.0),
                                          shape: BoxShape.rectangle,
                                        ),
                                        child: Padding(
                                          padding:
                                              EdgeInsetsDirectional.fromSTEB(
                                                  24.0, 16.0, 24.0, 16.0),
                                          child: Container(
                                            child: Column(
                                              mainAxisSize: MainAxisSize.min,
                                              mainAxisAlignment:
                                                  MainAxisAlignment.start,
                                              crossAxisAlignment:
                                                  CrossAxisAlignment.center,
                                              children: [
                                                InkWell(
                                                  splashColor:
                                                      Colors.transparent,
                                                  focusColor:
                                                      Colors.transparent,
                                                  hoverColor:
                                                      Colors.transparent,
                                                  highlightColor:
                                                      Colors.transparent,
                                                  onTap: () async {
                                                    context.pushNamed(
                                                        EAcoesRecomendadasOficialWidget
                                                            .routeName);
                                                  },
                                                  child: wrapWithModel(
                                                    model: _model
                                                        .dashboardMetricRowModel5,
                                                    updateCallback: () =>
                                                        safeSetState(() {}),
                                                    child:
                                                        DashboardMetricRow4Widget(
                                                      icon: Icon(
                                                        Icons
                                                            .fact_check_rounded,
                                                        color:
                                                            Color(0xFF636366),
                                                        size: 22.0,
                                                      ),
                                                      label:
                                                          'Ações Recomendadas',
                                                      value:
                                                          containerIndicadoresResumoRecordList
                                                              .firstOrNull
                                                              ?.acoesRecomendadas
                                                              ?.toString(),
                                                      valueColor:
                                                          FlutterFlowTheme.of(
                                                                  context)
                                                              .primary,
                                                      last: false,
                                                    ),
                                                  ),
                                                ),
                                              ],
                                            ),
                                          ),
                                        ),
                                      );
                                    },
                                  ),
                                ].divide(SizedBox(height: 16.0)),
                              ),
                              Container(
                                height: 24.0,
                              ),
                            ].divide(SizedBox(height: 24.0)),
                          ),
                        ),
                      ),
                    ],
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
