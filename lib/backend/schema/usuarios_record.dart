import 'dart:async';

import 'package:collection/collection.dart';

import '/backend/schema/util/firestore_util.dart';
import '/backend/schema/util/schema_util.dart';

import 'index.dart';
import '/flutter_flow/flutter_flow_util.dart';

class UsuariosRecord extends FirestoreRecord {
  UsuariosRecord._(
    DocumentReference reference,
    Map<String, dynamic> data,
  ) : super(reference, data) {
    _initializeFields();
  }

  // "atualizado_em" field.
  DateTime? _atualizadoEm;
  DateTime? get atualizadoEm => _atualizadoEm;
  bool hasAtualizadoEm() => _atualizadoEm != null;

  // "criado_em" field.
  DateTime? _criadoEm;
  DateTime? get criadoEm => _criadoEm;
  bool hasCriadoEm() => _criadoEm != null;

  // "email" field.
  String? _email;
  String get email => _email ?? '';
  bool hasEmail() => _email != null;

  // "empresa_id" field.
  String? _empresaId;
  String get empresaId => _empresaId ?? '';
  bool hasEmpresaId() => _empresaId != null;

  // "nome" field.
  String? _nome;
  String get nome => _nome ?? '';
  bool hasNome() => _nome != null;

  // "perfil" field.
  String? _perfil;
  String get perfil => _perfil ?? '';
  bool hasPerfil() => _perfil != null;

  // "role" field.
  String? _role;
  String get role => _role ?? '';
  bool hasRole() => _role != null;

  // "uid" field.
  String? _uid;
  String get uid => _uid ?? '';
  bool hasUid() => _uid != null;

  // "ativo" field.
  bool? _ativo;
  bool get ativo => _ativo ?? false;
  bool hasAtivo() => _ativo != null;

  // "categorias_ids" field.
  String? _categoriasIds;
  String get categoriasIds => _categoriasIds ?? '';
  bool hasCategoriasIds() => _categoriasIds != null;

  // "lojas_ids" field.
  String? _lojasIds;
  String get lojasIds => _lojasIds ?? '';
  bool hasLojasIds() => _lojasIds != null;

  // "perfil_label" field.
  String? _perfilLabel;
  String get perfilLabel => _perfilLabel ?? '';
  bool hasPerfilLabel() => _perfilLabel != null;

  // "empresa_nome" field.
  String? _empresaNome;
  String get empresaNome => _empresaNome ?? '';
  bool hasEmpresaNome() => _empresaNome != null;

  // "lojas_texto" field.
  String? _lojasTexto;
  String get lojasTexto => _lojasTexto ?? '';
  bool hasLojasTexto() => _lojasTexto != null;

  // "status_texto" field.
  String? _statusTexto;
  String get statusTexto => _statusTexto ?? '';
  bool hasStatusTexto() => _statusTexto != null;

  // "display_name" field.
  String? _displayName;
  String get displayName => _displayName ?? '';
  bool hasDisplayName() => _displayName != null;

  // "photo_url" field.
  String? _photoUrl;
  String get photoUrl => _photoUrl ?? '';
  bool hasPhotoUrl() => _photoUrl != null;

  // "created_time" field.
  DateTime? _createdTime;
  DateTime? get createdTime => _createdTime;
  bool hasCreatedTime() => _createdTime != null;

  // "phone_number" field.
  String? _phoneNumber;
  String get phoneNumber => _phoneNumber ?? '';
  bool hasPhoneNumber() => _phoneNumber != null;

  // "status_color" field.
  String? _statusColor;
  String get statusColor => _statusColor ?? '';
  bool hasStatusColor() => _statusColor != null;

  // "status_bg_color" field.
  String? _statusBgColor;
  String get statusBgColor => _statusBgColor ?? '';
  bool hasStatusBgColor() => _statusBgColor != null;

  void _initializeFields() {
    _atualizadoEm = snapshotData['atualizado_em'] as DateTime?;
    _criadoEm = snapshotData['criado_em'] as DateTime?;
    _email = snapshotData['email'] as String?;
    _empresaId = snapshotData['empresa_id'] as String?;
    _nome = snapshotData['nome'] as String?;
    _perfil = snapshotData['perfil'] as String?;
    _role = snapshotData['role'] as String?;
    _uid = snapshotData['uid'] as String?;
    _ativo = snapshotData['ativo'] as bool?;
    _categoriasIds = snapshotData['categorias_ids'] as String?;
    _lojasIds = snapshotData['lojas_ids'] as String?;
    _perfilLabel = snapshotData['perfil_label'] as String?;
    _empresaNome = snapshotData['empresa_nome'] as String?;
    _lojasTexto = snapshotData['lojas_texto'] as String?;
    _statusTexto = snapshotData['status_texto'] as String?;
    _displayName = snapshotData['display_name'] as String?;
    _photoUrl = snapshotData['photo_url'] as String?;
    _createdTime = snapshotData['created_time'] as DateTime?;
    _phoneNumber = snapshotData['phone_number'] as String?;
    _statusColor = snapshotData['status_color'] as String?;
    _statusBgColor = snapshotData['status_bg_color'] as String?;
  }

  static CollectionReference get collection =>
      FirebaseFirestore.instance.collection('usuarios');

  static Stream<UsuariosRecord> getDocument(DocumentReference ref) =>
      ref.snapshots().map((s) => UsuariosRecord.fromSnapshot(s));

  static Future<UsuariosRecord> getDocumentOnce(DocumentReference ref) =>
      ref.get().then((s) => UsuariosRecord.fromSnapshot(s));

  static UsuariosRecord fromSnapshot(DocumentSnapshot snapshot) =>
      UsuariosRecord._(
        snapshot.reference,
        mapFromFirestore(snapshot.data() as Map<String, dynamic>),
      );

  static UsuariosRecord getDocumentFromData(
    Map<String, dynamic> data,
    DocumentReference reference,
  ) =>
      UsuariosRecord._(reference, mapFromFirestore(data));

  @override
  String toString() =>
      'UsuariosRecord(reference: ${reference.path}, data: $snapshotData)';

  @override
  int get hashCode => reference.path.hashCode;

  @override
  bool operator ==(other) =>
      other is UsuariosRecord &&
      reference.path.hashCode == other.reference.path.hashCode;
}

Map<String, dynamic> createUsuariosRecordData({
  DateTime? atualizadoEm,
  DateTime? criadoEm,
  String? email,
  String? empresaId,
  String? nome,
  String? perfil,
  String? role,
  String? uid,
  bool? ativo,
  String? categoriasIds,
  String? lojasIds,
  String? perfilLabel,
  String? empresaNome,
  String? lojasTexto,
  String? statusTexto,
  String? displayName,
  String? photoUrl,
  DateTime? createdTime,
  String? phoneNumber,
  String? statusColor,
  String? statusBgColor,
}) {
  final firestoreData = mapToFirestore(
    <String, dynamic>{
      'atualizado_em': atualizadoEm,
      'criado_em': criadoEm,
      'email': email,
      'empresa_id': empresaId,
      'nome': nome,
      'perfil': perfil,
      'role': role,
      'uid': uid,
      'ativo': ativo,
      'categorias_ids': categoriasIds,
      'lojas_ids': lojasIds,
      'perfil_label': perfilLabel,
      'empresa_nome': empresaNome,
      'lojas_texto': lojasTexto,
      'status_texto': statusTexto,
      'display_name': displayName,
      'photo_url': photoUrl,
      'created_time': createdTime,
      'phone_number': phoneNumber,
      'status_color': statusColor,
      'status_bg_color': statusBgColor,
    }.withoutNulls,
  );

  return firestoreData;
}

class UsuariosRecordDocumentEquality implements Equality<UsuariosRecord> {
  const UsuariosRecordDocumentEquality();

  @override
  bool equals(UsuariosRecord? e1, UsuariosRecord? e2) {
    return e1?.atualizadoEm == e2?.atualizadoEm &&
        e1?.criadoEm == e2?.criadoEm &&
        e1?.email == e2?.email &&
        e1?.empresaId == e2?.empresaId &&
        e1?.nome == e2?.nome &&
        e1?.perfil == e2?.perfil &&
        e1?.role == e2?.role &&
        e1?.uid == e2?.uid &&
        e1?.ativo == e2?.ativo &&
        e1?.categoriasIds == e2?.categoriasIds &&
        e1?.lojasIds == e2?.lojasIds &&
        e1?.perfilLabel == e2?.perfilLabel &&
        e1?.empresaNome == e2?.empresaNome &&
        e1?.lojasTexto == e2?.lojasTexto &&
        e1?.statusTexto == e2?.statusTexto &&
        e1?.displayName == e2?.displayName &&
        e1?.photoUrl == e2?.photoUrl &&
        e1?.createdTime == e2?.createdTime &&
        e1?.phoneNumber == e2?.phoneNumber &&
        e1?.statusColor == e2?.statusColor &&
        e1?.statusBgColor == e2?.statusBgColor;
  }

  @override
  int hash(UsuariosRecord? e) => const ListEquality().hash([
        e?.atualizadoEm,
        e?.criadoEm,
        e?.email,
        e?.empresaId,
        e?.nome,
        e?.perfil,
        e?.role,
        e?.uid,
        e?.ativo,
        e?.categoriasIds,
        e?.lojasIds,
        e?.perfilLabel,
        e?.empresaNome,
        e?.lojasTexto,
        e?.statusTexto,
        e?.displayName,
        e?.photoUrl,
        e?.createdTime,
        e?.phoneNumber,
        e?.statusColor,
        e?.statusBgColor
      ]);

  @override
  bool isValidKey(Object? o) => o is UsuariosRecord;
}
