import '/backend/backend.dart';
import '/components/dashboard_metric_row4_widget.dart';
import '/components/dashboard_shortcut_card3_widget.dart';
import '/flutter_flow/flutter_flow_icon_button.dart';
import '/flutter_flow/flutter_flow_theme.dart';
import '/flutter_flow/flutter_flow_util.dart';
import '/flutter_flow/flutter_flow_widgets.dart';
import 'dart:ui';
import '/index.dart';
import 'b_pagina_initial_oficial_widget.dart' show BPaginaInitialOficialWidget;
import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:provider/provider.dart';

class BPaginaInitialOficialModel
    extends FlutterFlowModel<BPaginaInitialOficialWidget> {
  ///  State fields for stateful widgets in this page.

  // Model for DashboardShortcutCard.
  late DashboardShortcutCard3Model dashboardShortcutCardModel1;
  // Model for DashboardShortcutCard.
  late DashboardShortcutCard3Model dashboardShortcutCardModel2;
  // Model for DashboardShortcutCard.
  late DashboardShortcutCard3Model dashboardShortcutCardModel3;
  // Model for DashboardShortcutCard.
  late DashboardShortcutCard3Model dashboardShortcutCardModel4;
  // Model for DashboardMetricRow.
  late DashboardMetricRow4Model dashboardMetricRowModel1;
  // Model for DashboardMetricRow.
  late DashboardMetricRow4Model dashboardMetricRowModel2;
  // Model for DashboardMetricRow.
  late DashboardMetricRow4Model dashboardMetricRowModel3;
  // Model for DashboardMetricRow.
  late DashboardMetricRow4Model dashboardMetricRowModel4;
  // Model for DashboardMetricRow.
  late DashboardMetricRow4Model dashboardMetricRowModel5;

  @override
  void initState(BuildContext context) {
    dashboardShortcutCardModel1 =
        createModel(context, () => DashboardShortcutCard3Model());
    dashboardShortcutCardModel2 =
        createModel(context, () => DashboardShortcutCard3Model());
    dashboardShortcutCardModel3 =
        createModel(context, () => DashboardShortcutCard3Model());
    dashboardShortcutCardModel4 =
        createModel(context, () => DashboardShortcutCard3Model());
    dashboardMetricRowModel1 =
        createModel(context, () => DashboardMetricRow4Model());
    dashboardMetricRowModel2 =
        createModel(context, () => DashboardMetricRow4Model());
    dashboardMetricRowModel3 =
        createModel(context, () => DashboardMetricRow4Model());
    dashboardMetricRowModel4 =
        createModel(context, () => DashboardMetricRow4Model());
    dashboardMetricRowModel5 =
        createModel(context, () => DashboardMetricRow4Model());
  }

  @override
  void dispose() {
    dashboardShortcutCardModel1.dispose();
    dashboardShortcutCardModel2.dispose();
    dashboardShortcutCardModel3.dispose();
    dashboardShortcutCardModel4.dispose();
    dashboardMetricRowModel1.dispose();
    dashboardMetricRowModel2.dispose();
    dashboardMetricRowModel3.dispose();
    dashboardMetricRowModel4.dispose();
    dashboardMetricRowModel5.dispose();
  }
}
