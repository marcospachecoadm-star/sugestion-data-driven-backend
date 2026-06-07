import 'dart:convert';
import 'dart:math' as math;

import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:intl/intl.dart';
import 'package:timeago/timeago.dart' as timeago;
import 'lat_lng.dart';
import 'place.dart';
import 'uploaded_file.dart';
import '/backend/backend.dart';
import 'package:cloud_firestore/cloud_firestore.dart';
import '/auth/firebase_auth/auth_util.dart';

int calcularAtencao(
  int totalCriticos,
  int ruptura,
) {
  final resultado = totalCriticos - ruptura;
  return resultado < 0 ? 0 : resultado;
}

int somarCriticos(
  int ruptura,
  int abaixoMinimo,
) {
  return ruptura + abaixoMinimo;
}

bool filtrarBusca(
  String? busca,
  String? texto,
) {
  if (busca == null || busca.trim().isEmpty) {
    return true;
  }

  final termo = busca.toLowerCase().trim();
  final base = (texto ?? '').toLowerCase();

  return base.contains(termo);
}
