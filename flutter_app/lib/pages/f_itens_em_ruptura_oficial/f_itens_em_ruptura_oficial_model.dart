import '/backend/api_requests/api_calls.dart';
import '/backend/backend.dart';
import '/components/card_em_ruptura_oficial_widget.dart';
import '/components/header_indicador_widget.dart';
import '/components/indicator_summary_card2_widget.dart';
import '/flutter_flow/flutter_flow_theme.dart';
import '/flutter_flow/flutter_flow_util.dart';
import '/flutter_flow/flutter_flow_widgets.dart';
import 'dart:ui';
import 'f_itens_em_ruptura_oficial_widget.dart'
    show FItensEmRupturaOficialWidget;
import 'package:easy_debounce/easy_debounce.dart';
import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:provider/provider.dart';

class FItensEmRupturaOficialModel
    extends FlutterFlowModel<FItensEmRupturaOficialWidget> {
  ///  Local state fields for this page.

  bool mostrarBusca = false;

  String? buscarGiro = ' __todos__';

  String? buscaDigitada;

  ///  State fields for stateful widgets in this page.

  // Model for HeaderIndicador.
  late HeaderIndicadorModel headerIndicadorModel;
  // Model for IndicatorSummaryCard.
  late IndicatorSummaryCard2Model indicatorSummaryCardModel1;
  // Model for IndicatorSummaryCard.
  late IndicatorSummaryCard2Model indicatorSummaryCardModel2;
  // State field(s) for TextField widget.
  FocusNode? textFieldFocusNode;
  TextEditingController? textController;
  String? Function(BuildContext, String?)? textControllerValidator;

  @override
  void initState(BuildContext context) {
    headerIndicadorModel = createModel(context, () => HeaderIndicadorModel());
    indicatorSummaryCardModel1 =
        createModel(context, () => IndicatorSummaryCard2Model());
    indicatorSummaryCardModel2 =
        createModel(context, () => IndicatorSummaryCard2Model());
  }

  @override
  void dispose() {
    headerIndicadorModel.dispose();
    indicatorSummaryCardModel1.dispose();
    indicatorSummaryCardModel2.dispose();
    textFieldFocusNode?.dispose();
    textController?.dispose();
  }
}
