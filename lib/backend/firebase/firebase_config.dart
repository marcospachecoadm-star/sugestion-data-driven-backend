import 'package:firebase_core/firebase_core.dart';
import 'package:flutter/foundation.dart';

Future initFirebase() async {
  if (kIsWeb) {
    await Firebase.initializeApp(
        options: FirebaseOptions(
            apiKey: "AIzaSyB6oi0cIXXAIcaBANUttTdUmb71__7lVM0",
            authDomain: "datadriven-4816c.firebaseapp.com",
            projectId: "datadriven-4816c",
            storageBucket: "datadriven-4816c.firebasestorage.app",
            messagingSenderId: "387321907004",
            appId: "1:387321907004:web:da65d48b0899d2dfa80039"));
  } else {
    await Firebase.initializeApp();
  }
}
