import '/components/status_badge/status_badge_widget.dart';
import '/flutter_flow/flutter_flow_theme.dart';
import '/flutter_flow/flutter_flow_util.dart';
import '/flutter_flow/flutter_flow_widgets.dart';
import 'dart:ui';
import 'item_sem_venda_card_widget.dart' show ItemSemVendaCardWidget;
import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:provider/provider.dart';

class ItemSemVendaCardModel extends FlutterFlowModel<ItemSemVendaCardWidget> {
  ///  State fields for stateful widgets in this component.

  // Model for StatusBadge.
  late StatusBadgeModel statusBadgeModel;

  @override
  void initState(BuildContext context) {
    statusBadgeModel = createModel(context, () => StatusBadgeModel());
  }

  @override
  void dispose() {
    statusBadgeModel.dispose();
  }
}
