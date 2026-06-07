import '/auth/firebase_auth/auth_util.dart';
import '/backend/backend.dart';
import '/components/menu_item_widget.dart';
import '/components/status_badge2_widget.dart';
import '/flutter_flow/flutter_flow_drop_down.dart';
import '/flutter_flow/flutter_flow_theme.dart';
import '/flutter_flow/flutter_flow_util.dart';
import '/flutter_flow/flutter_flow_widgets.dart';
import '/flutter_flow/form_field_controller.dart';
import 'dart:ui';
import '/index.dart';
import 'a_gestao_painel_admin_widget.dart' show AGestaoPainelAdminWidget;
import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:provider/provider.dart';

class AGestaoPainelAdminModel
    extends FlutterFlowModel<AGestaoPainelAdminWidget> {
  ///  State fields for stateful widgets in this page.

  // Model for MenuItem.
  late MenuItemModel menuItemModel1;
  // Model for MenuItem.
  late MenuItemModel menuItemModel2;
  // Model for MenuItem.
  late MenuItemModel menuItemModel3;
  // Model for MenuItem.
  late MenuItemModel menuItemModel4;
  // State field(s) for Dropdown widget.
  String? dropdownValue1;
  FormFieldController<String>? dropdownValueController1;
  // State field(s) for Dropdown widget.
  String? dropdownValue2;
  FormFieldController<String>? dropdownValueController2;
  // Model for StatusBadge.
  late StatusBadge2Model statusBadgeModel;

  @override
  void initState(BuildContext context) {
    menuItemModel1 = createModel(context, () => MenuItemModel());
    menuItemModel2 = createModel(context, () => MenuItemModel());
    menuItemModel3 = createModel(context, () => MenuItemModel());
    menuItemModel4 = createModel(context, () => MenuItemModel());
    statusBadgeModel = createModel(context, () => StatusBadge2Model());
  }

  @override
  void dispose() {
    menuItemModel1.dispose();
    menuItemModel2.dispose();
    menuItemModel3.dispose();
    menuItemModel4.dispose();
    statusBadgeModel.dispose();
  }
}
