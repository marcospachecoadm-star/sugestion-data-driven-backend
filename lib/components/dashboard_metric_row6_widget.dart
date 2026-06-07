import '/flutter_flow/flutter_flow_theme.dart';
import '/flutter_flow/flutter_flow_util.dart';
import '/flutter_flow/flutter_flow_widgets.dart';
import 'dart:ui';
import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:provider/provider.dart';
import 'dashboard_metric_row6_model.dart';
export 'dashboard_metric_row6_model.dart';

class DashboardMetricRow6Widget extends StatefulWidget {
  const DashboardMetricRow6Widget({
    super.key,
    this.icon,
    String? label,
    String? value,
    Color? valueColor,
    bool? isLast,
  })  : this.label = label ?? 'Total de Vendas',
        this.value = value ?? 'R\$ 289.494,35',
        this.valueColor = valueColor ?? const Color(0x00000000),
        this.isLast = isLast ?? false;

  final Widget? icon;
  final String label;
  final String value;
  final Color valueColor;
  final bool isLast;

  @override
  State<DashboardMetricRow6Widget> createState() =>
      _DashboardMetricRow6WidgetState();
}

class _DashboardMetricRow6WidgetState extends State<DashboardMetricRow6Widget> {
  late DashboardMetricRow6Model _model;

  @override
  void setState(VoidCallback callback) {
    super.setState(callback);
    _model.onUpdate();
  }

  @override
  void initState() {
    super.initState();
    _model = createModel(context, () => DashboardMetricRow6Model());

    WidgetsBinding.instance.addPostFrameCallback((_) => safeSetState(() {}));
  }

  @override
  void dispose() {
    _model.maybeDispose();

    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      mainAxisSize: MainAxisSize.min,
      mainAxisAlignment: MainAxisAlignment.start,
      crossAxisAlignment: CrossAxisAlignment.center,
      children: [
        Container(
          child: Padding(
            padding: EdgeInsetsDirectional.fromSTEB(0.0, 16.0, 0.0, 16.0),
            child: Container(
              child: Row(
                mainAxisSize: MainAxisSize.max,
                mainAxisAlignment: MainAxisAlignment.start,
                crossAxisAlignment: CrossAxisAlignment.center,
                children: [
                  Container(
                    width: 36.0,
                    height: 36.0,
                    decoration: BoxDecoration(
                      borderRadius: BorderRadius.circular(12.0),
                      shape: BoxShape.rectangle,
                    ),
                    alignment: AlignmentDirectional(0.0, 0.0),
                    child: widget!.icon!,
                  ),
                  Expanded(
                    flex: 1,
                    child: Text(
                      valueOrDefault<String>(
                        widget!.label,
                        'Total de Vendas',
                      ),
                      maxLines: 1,
                      style: FlutterFlowTheme.of(context).bodyMedium.override(
                            font: GoogleFonts.inter(
                              fontWeight: FlutterFlowTheme.of(context)
                                  .bodyMedium
                                  .fontWeight,
                              fontStyle: FlutterFlowTheme.of(context)
                                  .bodyMedium
                                  .fontStyle,
                            ),
                            color: FlutterFlowTheme.of(context).primaryText,
                            letterSpacing: 0.0,
                            fontWeight: FlutterFlowTheme.of(context)
                                .bodyMedium
                                .fontWeight,
                            fontStyle: FlutterFlowTheme.of(context)
                                .bodyMedium
                                .fontStyle,
                            lineHeight: 1.4,
                          ),
                      overflow: TextOverflow.ellipsis,
                    ),
                  ),
                  Text(
                    valueOrDefault<String>(
                      widget!.value,
                      'R\$ 289.494,35',
                    ),
                    style: FlutterFlowTheme.of(context).titleSmall.override(
                          font: GoogleFonts.plusJakartaSans(
                            fontWeight: FontWeight.bold,
                            fontStyle: FlutterFlowTheme.of(context)
                                .titleSmall
                                .fontStyle,
                          ),
                          color: valueOrDefault<Color>(
                            widget!.valueColor,
                            FlutterFlowTheme.of(context).primary,
                          ),
                          letterSpacing: 0.0,
                          fontWeight: FontWeight.bold,
                          fontStyle:
                              FlutterFlowTheme.of(context).titleSmall.fontStyle,
                        ),
                  ),
                ].divide(SizedBox(width: 16.0)),
              ),
            ),
          ),
        ),
        if (widget!.isLast ? false : true)
          Padding(
            padding: EdgeInsetsDirectional.fromSTEB(52.0, 0.0, 0.0, 0.0),
            child: Container(
              child: Container(
                height: 1.0,
                decoration: BoxDecoration(
                  color: FlutterFlowTheme.of(context).alternate,
                  shape: BoxShape.rectangle,
                ),
              ),
            ),
          ),
      ],
    );
  }
}
