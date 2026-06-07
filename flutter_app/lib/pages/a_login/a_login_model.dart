import '/auth/firebase_auth/auth_util.dart';
import '/components/button/button_widget.dart';
import '/flutter_flow/flutter_flow_theme.dart';
import '/flutter_flow/flutter_flow_util.dart';
import '/flutter_flow/flutter_flow_widgets.dart';
import 'dart:ui';
import '/index.dart';
import 'a_login_widget.dart' show ALoginWidget;
import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:provider/provider.dart';

class ALoginModel extends FlutterFlowModel<ALoginWidget> {
  ///  State fields for stateful widgets in this page.

  // State field(s) for EMAILCERTO widget.
  FocusNode? emailcertoFocusNode;
  TextEditingController? emailcertoTextController;
  String? Function(BuildContext, String?)? emailcertoTextControllerValidator;
  // State field(s) for SENHACERTA widget.
  FocusNode? senhacertaFocusNode;
  TextEditingController? senhacertaTextController;
  late bool senhacertaVisibility;
  String? Function(BuildContext, String?)? senhacertaTextControllerValidator;
  // Model for Button.
  late ButtonModel buttonModel1;
  // Model for Button.
  late ButtonModel buttonModel2;

  @override
  void initState(BuildContext context) {
    senhacertaVisibility = false;
    buttonModel1 = createModel(context, () => ButtonModel());
    buttonModel2 = createModel(context, () => ButtonModel());
  }

  @override
  void dispose() {
    emailcertoFocusNode?.dispose();
    emailcertoTextController?.dispose();

    senhacertaFocusNode?.dispose();
    senhacertaTextController?.dispose();

    buttonModel1.dispose();
    buttonModel2.dispose();
  }
}
