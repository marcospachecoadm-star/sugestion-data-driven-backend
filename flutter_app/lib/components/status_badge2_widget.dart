import '/flutter_flow/flutter_flow_theme.dart';
import '/flutter_flow/flutter_flow_util.dart';
import '/flutter_flow/flutter_flow_widgets.dart';
import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:provider/provider.dart';
import 'status_badge2_model.dart';
export 'status_badge2_model.dart';

class StatusBadge2Widget extends StatefulWidget {
  const StatusBadge2Widget({
    super.key,
    Color? bgTone,
    String? label,
    Color? textTone,
  })  : this.bgTone = bgTone ?? const Color(0x2619C463),
        this.label = label ?? 'Sucesso',
        this.textTone = textTone ?? const Color(0xFF19C463);

  final Color bgTone;
  final String label;
  final Color textTone;

  @override
  State<StatusBadge2Widget> createState() => _StatusBadge2WidgetState();
}

class _StatusBadge2WidgetState extends State<StatusBadge2Widget> {
  late StatusBadge2Model _model;

  @override
  void setState(VoidCallback callback) {
    super.setState(callback);
    _model.onUpdate();
  }

  @override
  void initState() {
    super.initState();
    _model = createModel(context, () => StatusBadge2Model());

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
      decoration: BoxDecoration(
        color: valueOrDefault<Color>(
          widget!.bgTone,
          Color(0x2619C463),
        ),
        borderRadius: BorderRadius.circular(8.0),
        shape: BoxShape.rectangle,
      ),
      child: Padding(
        padding: EdgeInsetsDirectional.fromSTEB(16.0, 4.0, 16.0, 4.0),
        child: Container(
          child: Text(
            valueOrDefault<String>(
              widget!.label,
              'Sucesso',
            ),
            style: FlutterFlowTheme.of(context).labelSmall.override(
                  font: GoogleFonts.inter(
                    fontWeight: FontWeight.bold,
                    fontStyle:
                        FlutterFlowTheme.of(context).labelSmall.fontStyle,
                  ),
                  color: valueOrDefault<Color>(
                    widget!.textTone,
                    Color(0xFF19C463),
                  ),
                  letterSpacing: 0.0,
                  fontWeight: FontWeight.bold,
                  fontStyle: FlutterFlowTheme.of(context).labelSmall.fontStyle,
                  lineHeight: 1.4,
                ),
          ),
        ),
      ),
    );
  }
}
