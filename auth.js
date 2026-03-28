import {
  onAuthStateChanged,
  signInWithEmailAndPassword,
  signOut
} from "firebase/auth";
import { doc, getDoc, serverTimestamp, setDoc } from "firebase/firestore";
import { auth, db } from "./firebase-config.js";

export async function signInWithEmail(email, password) {
  const result = await signInWithEmailAndPassword(auth, email, password);
  await ensureUserProfile(result.user);
  return result.user;
}

export async function logout() {
  await signOut(auth);
}

export function onAuthChange(callback) {
  return onAuthStateChanged(auth, callback);
}

async function ensureUserProfile(user) {
  if (!user?.uid) {
    return;
  }

  const userRef = doc(db, "users", user.uid);
  const snap = await getDoc(userRef);

  if (snap.exists()) {
    return;
  }

  await setDoc(userRef, {
    uid: user.uid,
    displayName: user.displayName || user.email || "Aura User",
    email: user.email || "",
    photoURL: user.photoURL || "",
    profileColor: "#86b8ff",
    role: "participant",
    auraPoints: 0,
    auraCoins: 0,
    location: { lat: null, lng: null },
    locationUpdatedAt: null,
    lastLoginAt: serverTimestamp(),
    createdAt: serverTimestamp()
  });
}
