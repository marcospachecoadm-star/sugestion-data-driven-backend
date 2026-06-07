import '/backend/api_requests/api_calls.dart';
import '/backend/backend.dart';
import '/components/card_acoes_recomendadas_oficial_widget.dart';
import '/components/card_acoes_recomendadas_widget.dart';
import '/components/header_indicador_widget.dart';
import '/flutter_flow/flutter_flow_theme.dart';
import '/flutter_flow/flutter_flow_util.dart';
import '/flutter_flow/flutter_flow_widgets.dart';
import 'dart:ui';
import 'e_acoes_recomendadas_oficial_widget.dart'
    show EAcoesRecomendadasOficialWidget;
import 'package:easy_debounce/easy_debounce.dart';
import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:provider/provider.dart';

class EAcoesRecomendadasOficialModel
    extends FlutterFlowModel<EAcoesRecomendadasOficialWidget> {
  ///  Local state fields for this page.

  bool mostrarBusca = false;

  String? buscarGiro = ' __todos__';

  String? buscaDigitada;

  ///  State fields for stateful widgets in this page.

  // Model for HeaderIndicador.
  late HeaderIndicadorModel headerIndicadorModel;
  // Model for CardAcoesRecomendadas component.
  late CardAcoesRecomendadasModel cardAcoesRecomendadasModel;
  // State field(s) for TextField widget.
  FocusNode? textFieldFocusNode;
  TextEditingController? textController;
  String? Function(BuildContext, String?)? textControllerValidator;

  @override
  void initState(BuildContext context) {
    headerIndicadorModel = createModel(context, () => HeaderIndicadorModel());
    cardAcoesRecomendadasModel =
        createModel(context, () => CardAcoesRecomendadasModel());
  }

  @override
  void dispose() {
    headerIndicadorModel.dispose();
    cardAcoesRecomendadasModel.dispose();
    textFieldFocusNode?.dispose();
    textController?.dispose();
  }
}
