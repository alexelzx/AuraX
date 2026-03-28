# AuraX 🌟

AuraX is a mobile-first, highly gamified social web application designed to track, manage, and visualize "Aura". Built with a modern, dark-mode "Material You" UI, it features a real-time leaderboard, a custom mock economy, and a real-life compass to locate other participants.

## 🚀 Core Functionalities

### 🛡️ Admin Mode
- **Aura Management:** Admins can add or deduct Aura points from users. Entries include a title, description, and photo support.
- **Full Logs:** A comprehensive, real-time feed of all Aura transactions and system events.
- **Approval Workflow:** Admins can review, approve, or reject user-submitted requests for Aura adjustments.

### 👥 Participant Mode
- **Personal Dashboard:** View your current Aura balance, Mastery Level, and AuraCoin wallet.
- **The Podium:** A real-time leaderboard ranking all participants by their total Aura.
- **Voting System:** Upvote or downvote actions from other participants.
- **Aura Requests:** Submit requests to the Admin to grant or remove Aura for yourself or others.

### 🪙 Economy & Progression
- **AuraCoins:** Every 10,000 Aura generates 1 AuraCoin.
- **Transactions:** Send AuraCoins to other participants. Sent coins are subject to a 50% transaction fee, meaning the receiver gets an instant deposit of +5,000 Aura.
- **Leveling & Mastery:** Every 1,000 Aura equals 1 Level. Reaching Level 100 resets the level to 0 and grants a new "Mastery" tier (Mastery I up to Mastery X).

### 🧭 Compass Feature
- Real-time navigation tool utilizing HTML5 Geolocation and DeviceOrientation APIs.
- Select a participant from the dropdown, and the UI will display a dynamic compass arrow pointing directly toward their last known location.

## 🛠️ Tech Stack
- **Frontend:** Plain HTML5, Vanilla JavaScript (`app.js`), and modern CSS variables (`styles.css`).
- **Design Language:** Material You (Strict Dark Mode, Glassmorphism, Fluid Animations).
- **Backend & Database:** Firebase V9 (Authentication, Firestore, Storage).

## 🔒 License
This project is proprietary and confidential. All rights reserved. See the `LICENSE` file for details.
