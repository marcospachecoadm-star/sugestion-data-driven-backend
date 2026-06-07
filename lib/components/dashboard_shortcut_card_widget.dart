import '/flutter_flow/flutter_flow_theme.dart';
import '/flutter_flow/flutter_flow_util.dart';
import '/flutter_flow/flutter_flow_widgets.dart';
import 'dart:ui';
import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:provider/provider.dart';
import 'dashboard_shortcut_card_model.dart';
export 'dashboard_shortcut_card_model.dart';

class DashboardShortcutCardWidget extends StatefulWidget {
  const DashboardShortcutCardWidget({
    super.key,
    Color? iconBg,
    this.iconName,
    String? label,
  })  : this.iconBg = iconBg ?? const Color(0xFF4361EE),
        this.label = label ?? 'Giro Médio';

  final Color iconBg;
  final Widget? iconName;
  final String label;

  @override
  State<DashboardShortcutCardWidget> createState() =>
      _DashboardShortcutCardWidgetState();
}

class _DashboardShortcutCardWidgetState
    extends State<DashboardShortcutCardWidget> {
  late DashboardShortcutCardModel _model;

  @override
  void setState(VoidCallback callback) {
    super.setState(callback);
    _model.onUpdate();
  }

  @override
  void initState() {
    super.initState();
    _model = createModel(context, () => DashboardShortcutCardModel());

    WidgetsBinding.instance.addPostFrameCallback((_) => safeSetState(() {}));
  }

  @override
  void dispose() {
    _model.maybeDispose();

    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Container(
      height: 104.0,
      decoration: BoxDecoration(
        color: Color(0xFF283593),
        borderRadius: BorderRadius.circular(16.0),
        shape: BoxShape.rectangle,
      ),
      child: Padding(
        padding: EdgeInsetsDirectional.fromSTEB(8.0, 16.0, 8.0, 16.0),
        child: Container(
          child: Container(
            alignment: AlignmentDirectional(0.0, 0.0),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              mainAxisAlignment: MainAxisAlignment.center,
              crossAxisAlignment: CrossAxisAlignment.center,
              children: [
                Container(
                  width: 40.0,
                  height: 40.0,
                  decoration: BoxDecoration(
                    color: valueOrDefault<Color>(
                      widget!.iconBg,
                      Color(0xFF4361EE),
                    ),
                    borderRadius: BorderRadius.circular(9999.0),
                    shape: BoxShape.rectangle,
                  ),
                  alignment: AlignmentDirectional(0.0, 0.0),
                  child: widget!.iconName!,
                ),
                Text(
                  valueOrDefault<String>(
                    widget!.label,
                    'Giro Médio',
                  ),
                  textAlign: TextAlign.center,
                  maxLines: 2,
                  style: FlutterFlowTheme.of(context).labelSmall.override(
                        font: GoogleFonts.inter(
                          fontWeight: FlutterFlowTheme.of(context)
                              .labelSmall
                              .fontWeight,
                          fontStyle:
                              FlutterFlowTheme.of(context).labelSmall.fontStyle,
                        ),
                        color: Colors.white,
                        fontSize: 9.0,
                        letterSpacing: 0.0,
                        fontWeight:
                            FlutterFlowTheme.of(context).labelSmall.fontWeight,
                        fontStyle:
                            FlutterFlowTheme.of(context).labelSmall.fontStyle,
                        lineHeight: 1.0,
                      ),
                  overflow: TextOverflow.ellipsis,
                ),
              ].divide(SizedBox(height: 8.0)),
            ),
          ),
        ),
      ),
    );
  }
}
