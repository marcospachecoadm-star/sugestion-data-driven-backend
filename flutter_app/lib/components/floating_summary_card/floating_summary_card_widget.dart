import '/flutter_flow/flutter_flow_theme.dart';
import '/flutter_flow/flutter_flow_util.dart';
import '/flutter_flow/flutter_flow_widgets.dart';
import 'dart:ui';
import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:provider/provider.dart';
import 'floating_summary_card_model.dart';
export 'floating_summary_card_model.dart';

class FloatingSummaryCardWidget extends StatefulWidget {
  const FloatingSummaryCardWidget({
    super.key,
    Color? bgIcon,
    Color? colorIcon,
    this.icon,
    String? label,
    String? onTap,
  })  : this.bgIcon = bgIcon ?? const Color(0xFFE3F2FD),
        this.colorIcon = colorIcon ?? const Color(0xFF1976D2),
        this.label = label ?? 'Giro Médio',
        this.onTap = onTap ?? 'navigate:GiroMDio';

  final Color bgIcon;
  final Color colorIcon;
  final Widget? icon;
  final String label;
  final String onTap;

  @override
  State<FloatingSummaryCardWidget> createState() =>
      _FloatingSummaryCardWidgetState();
}

class _FloatingSummaryCardWidgetState extends State<FloatingSummaryCardWidget> {
  late FloatingSummaryCardModel _model;

  @override
  void setState(VoidCallback callback) {
    super.setState(callback);
    _model.onUpdate();
  }

  @override
  void initState() {
    super.initState();
    _model = createModel(context, () => FloatingSummaryCardModel());

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
      width: 80.0,
      height: 101.1,
      decoration: BoxDecoration(
        color: FlutterFlowTheme.of(context).secondaryBackground,
        borderRadius: BorderRadius.circular(16.0),
        shape: BoxShape.rectangle,
      ),
      child: Padding(
        padding: EdgeInsets.all(16.0),
        child: Container(
          child: Container(
            height: 68.0,
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
                      widget!.bgIcon,
                      Color(0xFFE3F2FD),
                    ),
                    borderRadius: BorderRadius.circular(9999.0),
                    shape: BoxShape.rectangle,
                  ),
                  alignment: AlignmentDirectional(0.0, 0.0),
                  child: widget!.icon!,
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
                        color: FlutterFlowTheme.of(context).primaryText,
                        letterSpacing: 0.0,
                        fontWeight:
                            FlutterFlowTheme.of(context).labelSmall.fontWeight,
                        fontStyle:
                            FlutterFlowTheme.of(context).labelSmall.fontStyle,
                        lineHeight: 1.27,
                      ),
                  overflow: TextOverflow.ellipsis,
                ),
              ].divide(SizedBox(height: 1.0)),
            ),
          ),
        ),
      ),
    );
  }
}
