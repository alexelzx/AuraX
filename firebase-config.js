import { initializeApp } from "firebase/app";
import { getAnalytics } from "firebase/analytics";
import { getFirestore } from "firebase/firestore";
import { getAuth } from "firebase/auth";
import { getStorage } from "firebase/storage";

const firebaseConfig = {
  apiKey: "AIzaSyDoX-RjasDb-Vcdbik--tKC4AhOG0dceg4",
  authDomain: "aurax2026gr.firebaseapp.com",
  projectId: "aurax2026gr",
  storageBucket: "aurax2026gr.firebasestorage.app",
  messagingSenderId: "965089476571",
  appId: "1:965089476571:web:f3239fdc0fcb4ad20371fe",
  measurementId: "G-4BVQZE9N6T"
};

const app = initializeApp(firebaseConfig);
const analytics = getAnalytics(app);
export const db = getFirestore(app);
export const auth = getAuth(app);
export const storage = getStorage(app);
export { analytics };
