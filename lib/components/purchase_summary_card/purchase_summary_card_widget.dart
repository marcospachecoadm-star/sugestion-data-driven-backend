import '/flutter_flow/flutter_flow_theme.dart';
import '/flutter_flow/flutter_flow_util.dart';
import '/flutter_flow/flutter_flow_widgets.dart';
import 'dart:ui';
import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:percent_indicator/percent_indicator.dart';
import 'package:provider/provider.dart';
import 'purchase_summary_card_model.dart';
export 'purchase_summary_card_model.dart';

class PurchaseSummaryCardWidget extends StatefulWidget {
  const PurchaseSummaryCardWidget({
    super.key,
    Color? bgIcon,
    Color? colorIcon,
    this.icon,
    String? label,
    double? progress,
    String? value,
  })  : this.bgIcon = bgIcon ?? const Color(0x00000000),
        this.colorIcon = colorIcon ?? const Color(0x00000000),
        this.label = label ?? 'Investimento',
        this.progress = progress ?? 0.7,
        this.value = value ?? 'R\$ 42,5k';

  final Color bgIcon;
  final Color colorIcon;
  final Widget? icon;
  final String label;
  final double progress;
  final String value;

  @override
  State<PurchaseSummaryCardWidget> createState() =>
      _PurchaseSummaryCardWidgetState();
}

class _PurchaseSummaryCardWidgetState extends State<PurchaseSummaryCardWidget> {
  late PurchaseSummaryCardModel _model;

  @override
  void setState(VoidCallback callback) {
    super.setState(callback);
    _model.onUpdate();
  }

  @override
  void initState() {
    super.initState();
    _model = createModel(context, () => PurchaseSummaryCardModel());

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
        color: FlutterFlowTheme.of(context).secondaryBackground,
        borderRadius: BorderRadius.circular(16.0),
        shape: BoxShape.rectangle,
        border: Border.all(
          color: FlutterFlowTheme.of(context).alternate,
          width: 1.0,
        ),
      ),
      child: Padding(
        padding: EdgeInsets.all(24.0),
        child: Container(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            mainAxisAlignment: MainAxisAlignment.start,
            crossAxisAlignment: CrossAxisAlignment.center,
            children: [
              Row(
                mainAxisSize: MainAxisSize.max,
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                crossAxisAlignment: CrossAxisAlignment.center,
                children: [
                  Column(
                    mainAxisSize: MainAxisSize.min,
                    mainAxisAlignment: MainAxisAlignment.start,
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        valueOrDefault<String>(
                          widget!.label,
                          'Investimento',
                        ),
                        style: FlutterFlowTheme.of(context).bodySmall.override(
                              font: GoogleFonts.inter(
                                fontWeight: FlutterFlowTheme.of(context)
                                    .bodySmall
                                    .fontWeight,
                                fontStyle: FlutterFlowTheme.of(context)
                                    .bodySmall
                                    .fontStyle,
                              ),
                              color: FlutterFlowTheme.of(context).secondaryText,
                              letterSpacing: 0.0,
                              fontWeight: FlutterFlowTheme.of(context)
                                  .bodySmall
                                  .fontWeight,
                              fontStyle: FlutterFlowTheme.of(context)
                                  .bodySmall
                                  .fontStyle,
                              lineHeight: 1.38,
                            ),
                      ),
                      Text(
                        valueOrDefault<String>(
                          widget!.value,
                          'R\$ 42,5k',
                        ),
                        style: FlutterFlowTheme.of(context)
                            .headlineMedium
                            .override(
                              font: GoogleFonts.plusJakartaSans(
                                fontWeight: FontWeight.bold,
                                fontStyle: FlutterFlowTheme.of(context)
                                    .headlineMedium
                                    .fontStyle,
                              ),
                              color: FlutterFlowTheme.of(context).primaryText,
                              letterSpacing: 0.0,
                              fontWeight: FontWeight.bold,
                              fontStyle: FlutterFlowTheme.of(context)
                                  .headlineMedium
                                  .fontStyle,
                              lineHeight: 1.25,
                            ),
                      ),
                    ].divide(SizedBox(height: 4.0)),
                  ),
                  Container(
                    width: 48.0,
                    height: 48.0,
                    decoration: BoxDecoration(
                      color: valueOrDefault<Color>(
                        widget!.bgIcon,
                        Color(0x00000000),
                      ),
                      borderRadius: BorderRadius.circular(9999.0),
                      shape: BoxShape.rectangle,
                    ),
                    alignment: AlignmentDirectional(0.0, 0.0),
                    child: widget!.icon!,
                  ),
                ],
              ),
              LinearPercentIndicator(
                percent: valueOrDefault<double>(
                  widget!.progress,
                  0.7,
                ),
                lineHeight: 4.0,
                animation: true,
                animateFromLastPercent: true,
                progressColor: valueOrDefault<Color>(
                  widget!.colorIcon,
                  FlutterFlowTheme.of(context).info,
                ),
                backgroundColor: FlutterFlowTheme.of(context).alternate,
                barRadius: Radius.circular(2.0),
                padding: EdgeInsets.zero,
              ),
            ].divide(SizedBox(height: 16.0)),
          ),
        ),
      ),
    );
  }
}
