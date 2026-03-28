import {
  addDoc,
  collection,
  doc,
  getDoc,
  limit,
  onSnapshot,
  orderBy,
  query,
  runTransaction,
  serverTimestamp,
  updateDoc,
  where
} from "firebase/firestore";
import { getDownloadURL, ref, uploadBytes } from "firebase/storage";
import { auth, db, storage } from "./firebase-config.js";
import {
  logout,
  onAuthChange,
  signInWithEmail
} from "./auth.js";

const shell = {
  authShell: document.getElementById("auth-shell"),
  appShell: document.getElementById("app-shell"),
  tabs: document.getElementById("tabs"),
  views: Array.from(document.querySelectorAll("#app-shell > section")),
  toast: document.getElementById("toast"),

  logoutBtn: document.getElementById("logout-btn"),
  emailLoginBtn: document.getElementById("email-login-btn"),
  downloadAppBtn: document.getElementById("download-app-btn"),
  authEmail: document.getElementById("auth-email"),
  authPassword: document.getElementById("auth-password"),

  fabRefresh: document.getElementById("fab-refresh"),
  installAppBtn: document.getElementById("install-app-btn")
};

let els = {
  myAura: null,
  myLevel: null,
  myMastery: null,
  myCoins: null,
  bankAura: null,
  bankCoins: null,
  auraHistoryList: null,
  coinHistoryList: null,
  tradeAuraForm: null,
  tradeAuraAmount: null,
  tradeAuraRaw: null,
  tradeAuraNet: null,
  tradeCoinsForm: null,
  tradeCoinsAmount: null,
  tradeCoinsRaw: null,
  tradeCoinsNet: null,
  groupParticipantsList: null,
  statTotalAura: null,
  statAvgDailyWon: null,
  statAvgDailyLost: null,
  statTopLoser: null,
  statTopWinner: null,
  leaderboardList: null,
  auraMovementsList: null,
  contestPollsList: null,
  requestForm: null,
  requestTarget: null,
  requestAmount: null,
  requestReason: null,
  voteSessionForm: null,
  voteSessionTarget: null,
  voteSessionReason: null,
  voteSessionAmount: null,
  auraVoteSessionsList: null,
  adminVoteSessionsList: null,
  coinForm: null,
  coinTarget: null,
  adminView: null,
  adminAuraForm: null,
  adminTarget: null,
  adminAmount: null,
  adminTitle: null,
  adminDescription: null,
  adminPhoto: null,
  voteApprovalsList: null,
  pendingRequestsList: null,
  logsList: null,
  profileForm: null,
  profileDisplayName: null,
  profileEmail: null,
  profilePhotoUrl: null,
  profileColor: null,
  profileSaveBtn: null,
  profileInspectorCard: null,
  inspectorPhoto: null,
  inspectorName: null,
  inspectorEmail: null,
  inspectorGps: null,
  inspectorGpsUpdated: null,
  inspectorLastLogin: null,
  inspectorLevel: null,
  inspectorMastery: null,
  inspectorPerformance: null
};

const state = {
  me: null,
  users: [],
  usersById: new Map(),
  personalLogs: [],
  adminLogs: [],
  publicMovements: [],
  voteSessions: [],
  mySessionVotes: new Map(),
  contestPolls: [],
  myContestVotes: new Map(),
  contestPollByLogId: new Map(),
  selectedProfileUserId: null,
  unsubscribers: [],
  deferredInstallPrompt: null,
  lastLocationUpdate: 0,
  lastOrientationUpdate: 0,
  pendingRenderUpdates: new Set(),
  renderScheduled: false,
  voteTickerId: null
};

init();

async function init() {
  initPwaSupport();
  await loadViewPartials();
  cacheDynamicElements();
  wireUiEvents();

  onAuthChange(async (user) => {
    clearSubscriptions();

    if (!user) {
      state.me = null;
      toggleApp(false);
      return;
    }

    const meSnap = await getDoc(doc(db, "users", user.uid));
    state.me = meSnap.data() || null;
    state.selectedProfileUserId = user.uid;

    try {
      await updateDoc(doc(db, "users", user.uid), {
        lastLoginAt: serverTimestamp(),
        email: user.email || state.me?.email || ""
      });
    } catch {
      // Keep app usable even when write rules prevent profile timestamp updates.
    }

    toggleApp(true);
    setupRoleUI();
    subscribeCoreData();
  });
}

function initPwaSupport() {
  if ("serviceWorker" in navigator) {
    window.addEventListener("load", () => {
      navigator.serviceWorker.register("./service-worker.js").catch(() => {
        // App remains fully usable without offline support.
      });
    });
  }

  if (isStandaloneMode()) {
    shell.installAppBtn?.classList.add("hidden");
    return;
  }

  window.addEventListener("beforeinstallprompt", (event) => {
    event.preventDefault();
    state.deferredInstallPrompt = event;
    shell.installAppBtn?.classList.remove("hidden");
  });

  window.addEventListener("appinstalled", () => {
    state.deferredInstallPrompt = null;
    shell.installAppBtn?.classList.add("hidden");
    toast("AuraX installed.");
  });

  if (isIosSafari()) {
    shell.installAppBtn?.classList.remove("hidden");
  }
}

async function handleInstallAppClick() {
  if (isStandaloneMode()) {
    toast("AuraX is already installed.");
    return;
  }

  if (state.deferredInstallPrompt) {
    const promptEvent = state.deferredInstallPrompt;
    state.deferredInstallPrompt = null;

    try {
      await promptEvent.prompt();
      await promptEvent.userChoice;
    } catch {
      // User dismissed prompt or browser canceled.
    }
    return;
  }

  if (isIosSafari()) {
    toast("On iPhone: open Share, then tap Add to Home Screen.");
    return;
  }

  if (!window.isSecureContext) {
    toast("Install works only on HTTPS or localhost.");
    return;
  }

  toast("Open your browser menu and tap Add to Home screen.");
}

function isStandaloneMode() {
  return window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;
}

function isIosSafari() {
  const ua = window.navigator.userAgent || "";
  const isIos = /iPad|iPhone|iPod/i.test(ua);
  const isSafari = /Safari/i.test(ua) && !/CriOS|FxiOS|EdgiOS/i.test(ua);
  return isIos && isSafari;
}

function wireUiEvents() {
  shell.emailLoginBtn.addEventListener("click", async () => {
    try {
      await signInWithEmail(shell.authEmail.value.trim(), shell.authPassword.value);
      toast("Logged in.");
    } catch (error) {
      toast(error.message || "Email login failed.");
    }
  });

  shell.downloadAppBtn?.addEventListener("click", handleInstallAppClick);

  shell.logoutBtn.addEventListener("click", async () => {
    await logout();
    toast("Logged out.");
  });

  setTimeout(() => {
    validateListeners();
  }, 1000);

  if (!state.voteTickerId) {
    state.voteTickerId = window.setInterval(() => {
      if (auth.currentUser) {
        maybeAutoCloseVoteSessions();
      }
    }, 1000);
  }

  shell.tabs.addEventListener("click", (event) => {
    const tab = event.target.closest(".tab");
    if (!tab) {
      return;
    }

    const viewId = tab.dataset.view;
    switchView(viewId, tab);
  });

  els.requestForm?.addEventListener("submit", handleRequestAura);
  els.tradeAuraForm?.addEventListener("submit", handleTradeAuraToCoins);
  els.tradeCoinsForm?.addEventListener("submit", handleTradeCoinsToAura);
  els.tradeAuraAmount?.addEventListener("input", renderTradeEstimates);
  els.tradeCoinsAmount?.addEventListener("input", renderTradeEstimates);
  els.coinForm?.addEventListener("submit", handleSendCoin);
  els.adminAuraForm?.addEventListener("submit", handleAdminAuraSubmit);
  els.profileForm?.addEventListener("submit", handleProfileSubmit);
  els.voteSessionForm?.addEventListener("submit", handleStartAuraVoteSession);
  els.auraVoteSessionsList?.addEventListener("click", handleAuraVoteSessionsClick);
  els.adminVoteSessionsList?.addEventListener("click", handleAdminVoteSessionsClick);
  els.voteApprovalsList?.addEventListener("click", handleVoteApprovalsClick);
  els.contestPollsList?.addEventListener("click", handleContestPollsClick);
  els.leaderboardList?.addEventListener("click", handleProfileListClick);
  els.groupParticipantsList?.addEventListener("click", handleProfileListClick);
  els.auraHistoryList?.addEventListener("click", handleAuraHistoryClick);

  shell.fabRefresh.addEventListener("click", () => {
    clearSubscriptions();
    subscribeCoreData();
    toast("Refreshed listeners.");
  });

  shell.installAppBtn?.addEventListener("click", handleInstallAppClick);
}

function validateListeners() {
  if (!auth.currentUser) {
    return;
  }

  if (!Array.isArray(state.unsubscribers) || state.unsubscribers.length === 0) {
    clearSubscriptions();
    subscribeCoreData();
  }
}

function toggleApp(isSignedIn) {
  shell.authShell.classList.toggle("hidden", isSignedIn);
  shell.appShell.classList.toggle("hidden", !isSignedIn);
  shell.logoutBtn.classList.toggle("hidden", !isSignedIn);
}

function switchView(viewId, clickedTab) {
  shell.views.forEach((view) => {
    view.classList.toggle("hidden", view.id !== viewId);
  });

  const allTabs = Array.from(document.querySelectorAll(".tab"));
  allTabs.forEach((tab) => tab.classList.remove("active"));
  clickedTab?.classList.add("active");
}

function setupRoleUI() {
  const isAdmin = state.me?.role === "admin";
  const adminTab = document.querySelector('.tab[data-view="admin-view"]');

  adminTab?.classList.toggle("hidden", !isAdmin);
  els.adminView?.classList.toggle("hidden", !isAdmin);
  document.querySelectorAll(".admin-only").forEach((node) => {
    node.classList.toggle("hidden", !isAdmin);
  });

  if (!isAdmin) {
    const dashboardTab = document.querySelector('.tab[data-view="dashboard-view"]');
    switchView("dashboard-view", dashboardTab);
  }
}

function subscribeCoreData() {
  const uid = auth.currentUser?.uid;
  const usersQ = query(collection(db, "users"));
  const usersUnsub = onSnapshot(usersQ, (snapshot) => {
    state.users = snapshot.docs
      .map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }))
      .sort((a, b) => Number(b.auraPoints || 0) - Number(a.auraPoints || 0));
    state.usersById = new Map(state.users.map((user) => [user.id, user]));

    state.me = state.usersById.get(auth.currentUser.uid) || state.me;
    renderDashboard();
    renderLeaderboard();
    renderGroupParticipants();
    renderSelectOptions();
    syncProfileForm();
    renderProfileInspector();
  });

  const logsQ = query(collection(db, "logs"), orderBy("timestamp", "desc"), limit(150));
  const logsUnsub = onSnapshot(logsQ, (snapshot) => {
    const logs = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
    state.adminLogs = logs;
    state.publicMovements = logs;
    state.personalLogs = filterPersonalLogs(logs, uid);
    renderLogs(logs);
    renderAuraMovements();
    renderPersonalHistories();
    renderAuraStats();
  });

  const voteSessionsQ = query(collection(db, "auraVoteSessions"), orderBy("timestamp", "desc"), limit(40));
  const voteSessionsUnsub = onSnapshot(voteSessionsQ, (snapshot) => {
    state.voteSessions = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
    maybeAutoCloseVoteSessions();
    renderAuraVoteSessions();
    renderAdminVoteSessions();
  });

  const myBallotsQ = query(collection(db, "auraVoteBallots"), where("voterId", "==", uid));
  const myBallotsUnsub = onSnapshot(myBallotsQ, (snapshot) => {
    state.mySessionVotes = new Map(
      snapshot.docs.map((docSnap) => [docSnap.data().sessionId, docSnap.data().voteType])
    );
    renderAuraVoteSessions();
  });

  const contestPollsQ = query(collection(db, "auraContestPolls"), orderBy("timestamp", "desc"), limit(60));
  const contestPollsUnsub = onSnapshot(contestPollsQ, (snapshot) => {
    state.contestPolls = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
    state.contestPollByLogId = new Map(
      state.contestPolls
        .filter((poll) => poll.logId)
        .map((poll) => [poll.logId, poll])
    );
    renderContestPolls();
    renderPersonalHistories();
  });

  const myContestVotesQ = query(collection(db, "auraContestVotes"), where("voterId", "==", uid));
  const myContestVotesUnsub = onSnapshot(myContestVotesQ, (snapshot) => {
    state.myContestVotes = new Map(
      snapshot.docs.map((docSnap) => [docSnap.data().pollId, docSnap.data().voteType])
    );
    renderContestPolls();
  });

  const extraUnsubs = [
    logsUnsub,
    voteSessionsUnsub,
    myBallotsUnsub,
    contestPollsUnsub,
    myContestVotesUnsub
  ];

  if (state.me?.role === "admin") {
    const requestsQ = query(
      collection(db, "requests"),
      where("status", "==", "pending"),
      orderBy("timestamp", "desc"),
      limit(40)
    );
    extraUnsubs.push(
      onSnapshot(requestsQ, (snapshot) => {
        const requests = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
        renderPendingRequests(requests);
      })
    );

    const pendingVoteApprovalsQ = query(
      collection(db, "auraVoteSessions"),
      where("status", "==", "pending_approval"),
      limit(40)
    );

    extraUnsubs.push(
      onSnapshot(pendingVoteApprovalsQ, (snapshot) => {
        const rows = snapshot.docs
          .map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }))
          .sort((a, b) => toMillis(b.timestamp) - toMillis(a.timestamp));
        renderVoteApprovals(rows);
      })
    );
  } else {
    renderPendingRequests([]);
    renderVoteApprovals([]);
  }

  state.unsubscribers.push(usersUnsub, ...extraUnsubs);
}

function clearSubscriptions() {
  while (state.unsubscribers.length > 0) {
    const unsub = state.unsubscribers.pop();
    if (typeof unsub === "function") {
      unsub();
    }
  }
}

function renderDashboard() {
  if (!state.me) {
    return;
  }

  const aura = Number(state.me.auraPoints || 0);
  const coins = Number(state.me.auraCoins || 0);
  const levelInfo = calculateLevelAndMastery(aura);

  if (els.myAura) {
    els.myAura.textContent = formatNumber(aura);
  }
  if (els.myLevel) {
    els.myLevel.textContent = String(levelInfo.level);
  }
  if (els.myMastery) {
    els.myMastery.textContent = levelInfo.masteryLabel;
  }
  if (els.myCoins) {
    els.myCoins.textContent = formatNumber(coins);
  }
  if (els.bankAura) {
    els.bankAura.textContent = formatNumber(aura);
  }
  if (els.bankCoins) {
    els.bankCoins.textContent = formatNumber(coins);
  }

  renderTradeEstimates();

  renderAuraStats();
}

function renderLeaderboard() {
  if (!els.leaderboardList) {
    return;
  }

  els.leaderboardList.innerHTML = "";

  state.users.forEach((user, idx) => {
    const color = safeProfileColor(user.profileColor);
    const li = document.createElement("li");
    li.className = "list-item clickable";
    li.dataset.userId = user.id;
    li.innerHTML = `
      <div class="list-item-head">
        <strong style="color:${color}">#${idx + 1} ${escapeHtml(user.displayName || "Aura User")}</strong>
        <span>${formatNumber(user.auraPoints || 0)} Aura</span>
      </div>
      <p class="muted">${user.role || "participant"} • Tap to inspect profile</p>
    `;
    els.leaderboardList.appendChild(li);
  });
}

function renderGroupParticipants() {
  if (!els.groupParticipantsList) {
    return;
  }

  els.groupParticipantsList.innerHTML = "";
  const participants = state.users.filter((user) => user.role !== "admin");

  if (participants.length === 0) {
    const li = document.createElement("li");
    li.className = "list-item";
    li.textContent = "No participants found.";
    els.groupParticipantsList.appendChild(li);
    return;
  }

  participants.forEach((user) => {
    const color = safeProfileColor(user.profileColor);
    const li = document.createElement("li");
    li.className = "list-item clickable";
    li.dataset.userId = user.id;
    li.innerHTML = `
      <div class="list-item-head">
        <strong style="color:${color}">${escapeHtml(user.displayName || "Aura User")}</strong>
        <span>${formatNumber(user.auraPoints || 0)} Aura</span>
      </div>
      <p class="muted">Tap to inspect profile</p>
    `;
    els.groupParticipantsList.appendChild(li);
  });
}

function renderAuraMovements() {
  if (!els.auraMovementsList) {
    return;
  }

  els.auraMovementsList.innerHTML = "";
  const rows = state.publicMovements.slice(0, 60);

  if (rows.length === 0) {
    const li = document.createElement("li");
    li.className = "list-item";
    li.textContent = "No Aura movements yet.";
    els.auraMovementsList.appendChild(li);
    return;
  }

  rows.forEach((entry) => {
    const actor = state.usersById.get(entry.adminId)?.displayName || "Unknown";
    const targetUser = state.usersById.get(entry.targetUserId);
    const target = targetUser?.displayName || "Unknown";
    const targetColor = safeProfileColor(targetUser?.profileColor);

    const li = document.createElement("li");
    li.className = "list-item";
    li.innerHTML = `
      <div class="list-item-head">
        <strong style="color:${targetColor}">${escapeHtml(target)}</strong>
        <span class="${amountClass(entry.amount)}">${formatSigned(entry.amount || 0)} Aura</span>
      </div>
      <p class="muted">${escapeHtml(entry.title || "Aura Update")} • by ${escapeHtml(actor)}</p>
      <p class="muted">${formatTimestamp(entry.timestamp)}</p>
    `;
    els.auraMovementsList.appendChild(li);
  });
}

function handleProfileListClick(event) {
  const row = event.target.closest(".list-item[data-user-id]");
  if (!row) {
    return;
  }

  openUserProfile(row.dataset.userId);
}

function openUserProfile(userId) {
  if (!userId) {
    return;
  }

  state.selectedProfileUserId = userId;
  renderProfileInspector();

  const profileTab = document.querySelector('.tab[data-view="profile-view"]');
  switchView("profile-view", profileTab);
}

function renderProfileInspector() {
  if (!els.inspectorName) {
    return;
  }

  const activeId = state.selectedProfileUserId || auth.currentUser?.uid;
  const user = state.usersById.get(activeId) || state.me;
  const isOwner = activeId === auth.currentUser?.uid;

  if (!user) {
    els.inspectorName.textContent = "Participant";
    els.inspectorEmail.textContent = "Unknown";
    els.inspectorGps.textContent = "Unknown";
    els.inspectorGpsUpdated.textContent = "Unknown";
    els.inspectorLastLogin.textContent = "Unknown";
    if (els.inspectorLevel) {
      els.inspectorLevel.textContent = "Unknown";
    }
    if (els.inspectorMastery) {
      els.inspectorMastery.textContent = "Unknown";
    }
    if (els.inspectorPerformance) {
      els.inspectorPerformance.textContent = "Unknown";
      els.inspectorPerformance.className = "status-pill";
    }
    setProfileEditability(false, null);
    return;
  }

  const lat = user.location?.lat;
  const lng = user.location?.lng;
  const gps = lat != null && lng != null ? `${Number(lat).toFixed(5)}, ${Number(lng).toFixed(5)}` : "Unknown";
  const levelInfo = calculateLevelAndMastery(Number(user.auraPoints || 0));

  els.inspectorName.textContent = user.displayName || "Aura User";
  els.inspectorName.style.color = safeProfileColor(user.profileColor);
  els.inspectorEmail.textContent = user.email || "No email";
  els.inspectorGps.textContent = gps;
  els.inspectorGpsUpdated.textContent = formatTimestamp(user.locationUpdatedAt);
  els.inspectorLastLogin.textContent = formatTimestamp(user.lastLoginAt);
  if (els.inspectorLevel) {
    els.inspectorLevel.textContent = String(levelInfo.level);
  }
  if (els.inspectorMastery) {
    els.inspectorMastery.textContent = levelInfo.masteryLabel;
  }

  const perf = calculatePerformanceStatus(Number(user.auraPoints || 0));
  if (els.inspectorPerformance) {
    els.inspectorPerformance.textContent = perf.label;
    els.inspectorPerformance.className = `status-pill ${perf.className}`;
  }

  if (els.inspectorPhoto) {
    els.inspectorPhoto.src = user.photoURL || "https://placehold.co/112x112/png";
  }

  if (els.profileInspectorCard) {
    els.profileInspectorCard.style.borderColor = safeProfileColor(user.profileColor);
  }

  setProfileEditability(isOwner, user);
}

function renderAuraStats() {
  if (!els.statTotalAura) {
    return;
  }

  const totalAura = state.users.reduce((sum, user) => sum + Number(user.auraPoints || 0), 0);
  const movements = state.publicMovements || [];
  const dailyMap = new Map();
  const winnerMap = new Map();
  const loserMap = new Map();

  movements.forEach((entry) => {
    const ms = toMillis(entry.timestamp);
    if (!ms) {
      return;
    }

    const amount = Number(entry.amount || 0);
    const dayKey = new Date(ms).toISOString().slice(0, 10);
    if (!dailyMap.has(dayKey)) {
      dailyMap.set(dayKey, { won: 0, lost: 0 });
    }

    const day = dailyMap.get(dayKey);
    if (amount > 0) {
      day.won += amount;
      winnerMap.set(entry.targetUserId, (winnerMap.get(entry.targetUserId) || 0) + amount);
    } else if (amount < 0) {
      day.lost += Math.abs(amount);
      loserMap.set(entry.targetUserId, (loserMap.get(entry.targetUserId) || 0) + Math.abs(amount));
    }
  });

  const dayCount = Math.max(dailyMap.size, 1);
  let sumWon = 0;
  let sumLost = 0;
  dailyMap.forEach((value) => {
    sumWon += value.won;
    sumLost += value.lost;
  });

  const topWinner = pickTopParticipant(winnerMap);
  const topLoser = pickTopParticipant(loserMap);

  els.statTotalAura.textContent = formatNumber(totalAura);
  els.statAvgDailyWon.textContent = formatNumber(Math.round(sumWon / dayCount));
  els.statAvgDailyLost.textContent = formatNumber(Math.round(sumLost / dayCount));
  els.statTopWinner.textContent = topWinner;
  els.statTopLoser.textContent = topLoser;
}

function pickTopParticipant(scoreMap) {
  let topId = null;
  let topValue = 0;

  scoreMap.forEach((value, key) => {
    if (value > topValue && key) {
      topId = key;
      topValue = value;
    }
  });

  if (!topId) {
    return "None";
  }

  const name = state.usersById.get(topId)?.displayName || "Unknown";
  return `${name} (${formatNumber(topValue)})`;
}

function renderSelectOptions() {
  const meProfile = state.me && auth.currentUser?.uid
    ? { id: auth.currentUser.uid, ...state.me }
    : null;
  const everyone = meProfile && !state.users.some((user) => user.id === meProfile.id)
    ? [...state.users, meProfile]
    : state.users;

  const allSelects = [
    els.requestTarget,
    els.voteSessionTarget,
    els.coinTarget,
    els.adminTarget
  ].filter(Boolean);

  const admins = everyone.filter((user) => user.role === "admin");
  const participants = everyone.filter((user) => user.role !== "admin");

  fillSelect(els.requestTarget, admins);
  fillSelect(els.voteSessionTarget, everyone);
  fillSelect(els.coinTarget, participants);
  fillSelect(els.adminTarget, everyone);

  ensureCurrentUserOption(els.voteSessionTarget);
  ensureCurrentUserOption(els.adminTarget);

  allSelects.forEach((select) => {
    select.disabled = select.options.length === 0;
  });
}

function fillSelect(selectEl, users) {
  if (!selectEl) {
    return;
  }

  const previous = selectEl.value;
  selectEl.innerHTML = "";

  users.forEach((user) => {
    const option = document.createElement("option");
    option.value = user.id;
    option.textContent = user.displayName || user.email || user.uid;
    selectEl.appendChild(option);
  });

  if (previous && users.some((u) => u.id === previous)) {
    selectEl.value = previous;
  }
}

function ensureCurrentUserOption(selectEl) {
  if (!selectEl || !auth.currentUser?.uid) {
    return;
  }

  const myUid = auth.currentUser.uid;
  const hasOption = Array.from(selectEl.options).some((option) => option.value === myUid);
  if (hasOption) {
    return;
  }

  const option = document.createElement("option");
  option.value = myUid;
  option.textContent = state.me?.displayName || auth.currentUser.email || "You";
  selectEl.appendChild(option);
}

function renderLogs(logs) {
  if (state.me?.role !== "admin") {
    if (els.logsList) {
      els.logsList.innerHTML = "";
    }
    return;
  }

  if (!els.logsList) {
    return;
  }

  els.logsList.innerHTML = "";

  logs.forEach((entry) => {
    const actorUser = state.usersById.get(entry.adminId);
    const actor = actorUser?.displayName || "Unknown";
    const targetUser = state.usersById.get(entry.targetUserId);
    const target = targetUser?.displayName || "Unknown";
    const ts = entry.timestamp?.toDate ? entry.timestamp.toDate().toLocaleString() : "Just now";
    const actorColor = safeProfileColor(actorUser?.profileColor);
    const targetColor = safeProfileColor(targetUser?.profileColor);

    const li = document.createElement("li");
    li.className = "list-item";
    li.innerHTML = `
      <div class="list-item-head">
        <strong>${escapeHtml(entry.title || "Aura Update")}</strong>
        <span class="${amountClass(entry.amount)}">${formatSigned(entry.amount || 0)} Aura</span>
      </div>
      <p class="muted">${escapeHtml(entry.description || "")}</p>
      <p class="muted">By <span style="color:${actorColor}">${escapeHtml(actor)}</span> • For <span style="color:${targetColor}">${escapeHtml(target)}</span> • ${escapeHtml(ts)}</p>
      ${entry.photoUrl ? `<a href="${entry.photoUrl}" target="_blank" rel="noreferrer">View photo</a>` : ""}
    `;
    els.logsList.appendChild(li);
  });
}

function renderPendingRequests(requests) {
  if (state.me?.role !== "admin") {
    if (els.pendingRequestsList) {
      els.pendingRequestsList.innerHTML = "";
    }
    return;
  }

  if (!els.pendingRequestsList) {
    return;
  }

  els.pendingRequestsList.innerHTML = "";

  if (requests.length === 0) {
    const li = document.createElement("li");
    li.className = "list-item";
    li.textContent = "No pending requests.";
    els.pendingRequestsList.appendChild(li);
    return;
  }

  requests.forEach((request) => {
    const requester = state.usersById.get(request.requesterId)?.displayName || "Unknown";

    const li = document.createElement("li");
    li.className = "list-item";
    li.innerHTML = `
      <div class="list-item-head">
        <strong>${escapeHtml(requester)}</strong>
        <span>${formatNumber(request.requestedAmount || 0)} Aura</span>
      </div>
      <p class="muted">${escapeHtml(request.reason || "")}</p>
      <div class="row">
        <button class="btn btn-primary" data-action="approve" data-id="${request.id}">Approve</button>
        <button class="btn btn-danger" data-action="reject" data-id="${request.id}">Reject</button>
      </div>
    `;
    els.pendingRequestsList.appendChild(li);
  });

  els.pendingRequestsList.querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", () => {
      const requestId = button.dataset.id;
      const action = button.dataset.action;
      handleRequestDecision(requestId, action);
    });
  });
}

async function handleRequestAura(event) {
  event.preventDefault();

  if (!state.me) {
    return;
  }

  if (!els.requestTarget?.value) {
    toast("Choose an admin target first.");
    return;
  }

  try {
    await addDoc(collection(db, "requests"), {
      timestamp: serverTimestamp(),
      requesterId: auth.currentUser.uid,
      targetUserId: els.requestTarget.value,
      requestedAmount: Number(els.requestAmount?.value),
      reason: els.requestReason?.value.trim(),
      status: "pending"
    });

    els.requestForm?.reset();
    toast("Aura request submitted.");
  } catch (error) {
    toast(error.message || "Failed to submit request.");
  }
}

async function handleStartAuraVoteSession(event) {
  event.preventDefault();

  const targetUserId = els.voteSessionTarget?.value;
  const reason = els.voteSessionReason?.value.trim();
  const proposedAmount = Number(els.voteSessionAmount?.value);

  if (!targetUserId || !reason || Number.isNaN(proposedAmount) || proposedAmount === 0) {
    toast("Provide target, reason, and a non-zero amount.");
    return;
  }

  try {
    await addDoc(collection(db, "auraVoteSessions"), {
      timestamp: serverTimestamp(),
      createdByUserId: auth.currentUser.uid,
      targetUserId,
      reason,
      proposedAmount,
      proposalSum: proposedAmount,
      proposalCount: 1,
      status: "pending_approval",
      upVotes: 0,
      downVotes: 0,
      expectedVoters: 0,
      votingEndsAtMs: 0,
      votingDurationSeconds: 60
    });

    els.voteSessionForm?.reset();
    toast("Vote proposal submitted. Waiting for admin approval.");
  } catch (error) {
    toast(error.message || "Could not start voting session.");
  }
}

function handleAuraVoteSessionsClick(event) {
  const voteBtn = event.target.closest("button[data-session-id][data-vote]");
  if (voteBtn) {
    const row = voteBtn.closest(".list-item");
    const proposalInput = row?.querySelector(
      `input[data-proposal-input-for="${voteBtn.dataset.sessionId}"]`
    );
    handleCastAuraVote(voteBtn.dataset.sessionId, voteBtn.dataset.vote, proposalInput?.value);
  }
}

function handleVoteApprovalsClick(event) {
  const btn = event.target.closest("button[data-session-id][data-session-action]");
  if (!btn) {
    return;
  }

  handleAuraVoteSessionAdminAction(btn.dataset.sessionId, btn.dataset.sessionAction);
}

function handleAdminVoteSessionsClick(event) {
  const adminActionBtn = event.target.closest("button[data-session-id][data-session-action]");
  if (!adminActionBtn) {
    return;
  }

  handleAuraVoteSessionAdminAction(
    adminActionBtn.dataset.sessionId,
    adminActionBtn.dataset.sessionAction
  );
}

async function handleCastAuraVote(sessionId, voteType, proposalValueInput) {
  if (!sessionId || !["up", "down"].includes(voteType)) {
    return;
  }

  if (state.mySessionVotes.has(sessionId)) {
    toast("You already voted on this session.");
    return;
  }

  try {
    await runTransaction(db, async (transaction) => {
      const sessionRef = doc(db, "auraVoteSessions", sessionId);
      const ballotRef = doc(db, "auraVoteBallots", `${sessionId}_${auth.currentUser.uid}`);

      const [sessionSnap, ballotSnap] = await Promise.all([
        transaction.get(sessionRef),
        transaction.get(ballotRef)
      ]);

      if (!sessionSnap.exists()) {
        throw new Error("Voting session not found.");
      }
      const data = sessionSnap.data();
      if (data.status !== "open") {
        throw new Error("Voting is closed for this session.");
      }
      if (ballotSnap.exists()) {
        throw new Error("You already voted on this session.");
      }
      if (Number(data.votingEndsAtMs || 0) > 0 && Date.now() >= Number(data.votingEndsAtMs || 0)) {
        transaction.update(sessionRef, {
          status: "closed",
          closedReason: "timeout",
          closedAt: serverTimestamp()
        });
        throw new Error("Voting time is over for this session.");
      }

      const nextUp = Number(data.upVotes || 0) + (voteType === "up" ? 1 : 0);
      const nextDown = Number(data.downVotes || 0) + (voteType === "down" ? 1 : 0);
      const typedCounter = Number(proposalValueInput);
      const hasCounter = Number.isFinite(typedCounter) && typedCounter !== 0;
      const offeredAmount = hasCounter ? typedCounter : Number(data.proposedAmount || 0);
      const nextProposalSum = Number(data.proposalSum || data.proposedAmount || 0) + offeredAmount;
      const nextProposalCount = Number(data.proposalCount || 1) + 1;
      const expectedVoters = Math.max(1, Number(data.expectedVoters || 0));
      const totalVotes = nextUp + nextDown;
      const shouldCloseNow = totalVotes >= expectedVoters;

      transaction.update(sessionRef, {
        upVotes: nextUp,
        downVotes: nextDown,
        proposalSum: nextProposalSum,
        proposalCount: nextProposalCount,
        ...(shouldCloseNow
          ? {
              status: "closed",
              closedReason: "all_votes",
              closedAt: serverTimestamp()
            }
          : {})
      });

      transaction.set(ballotRef, {
        timestamp: serverTimestamp(),
        sessionId,
        voterId: auth.currentUser.uid,
        voteType,
        proposedAmount: offeredAmount,
        hasCounter
      });
    });

    toast("Vote submitted.");
  } catch (error) {
    toast(error.message || "Vote submission failed.");
  }
}

async function handleAuraVoteSessionAdminAction(sessionId, action) {
  if (state.me?.role !== "admin") {
    toast("Admin access required.");
    return;
  }

  try {
    await runTransaction(db, async (transaction) => {
      const sessionRef = doc(db, "auraVoteSessions", sessionId);
      const sessionSnap = await transaction.get(sessionRef);

      if (!sessionSnap.exists()) {
        throw new Error("Voting session not found.");
      }

      const session = sessionSnap.data();

      if (action === "approve") {
        if (session.status !== "pending_approval") {
          throw new Error("Only pending vote proposals can be approved.");
        }

        transaction.update(sessionRef, {
          status: "open",
          approvedAt: serverTimestamp(),
          approvedBy: auth.currentUser.uid,
          expectedVoters: getVoteExpectedCount(),
          votingEndsAtMs: Date.now() + 60000,
          votingDurationSeconds: 60
        });
        return;
      }

      if (action === "reject") {
        if (session.status !== "pending_approval") {
          throw new Error("Only pending vote proposals can be rejected.");
        }

        transaction.update(sessionRef, {
          status: "rejected",
          rejectedAt: serverTimestamp(),
          rejectedBy: auth.currentUser.uid
        });
        return;
      }

      if (action === "close") {
        if (session.status !== "open") {
          throw new Error("Session is already closed.");
        }

        transaction.update(sessionRef, {
          status: "closed",
          closedAt: serverTimestamp(),
          closedBy: auth.currentUser.uid
        });
        return;
      }

      if (action !== "apply") {
        throw new Error("Unknown session action.");
      }

      if (session.status !== "closed") {
        throw new Error("Close the vote before applying results.");
      }
      if (Number(session.proposalCount || 0) <= 0) {
        throw new Error("Session has no valid proposals.");
      }

      const targetRef = doc(db, "users", session.targetUserId);
      const targetSnap = await transaction.get(targetRef);
      if (!targetSnap.exists()) {
        throw new Error("Target participant missing.");
      }

      const amount = computeVoteResultAmount(session);
      const prevAura = Number(targetSnap.data().auraPoints || 0);
      const prevCoins = Number(targetSnap.data().auraCoins || 0);
      const gainedCoins = amount > 0 ? Math.floor(amount / 10000) : 0;

      transaction.update(targetRef, {
        auraPoints: prevAura + amount,
        auraCoins: prevCoins + gainedCoins
      });

      transaction.update(sessionRef, {
        status: "applied",
        finalAmount: amount,
        appliedAt: serverTimestamp(),
        appliedBy: auth.currentUser.uid
      });

      const logRef = doc(collection(db, "logs"));
      transaction.set(logRef, {
        timestamp: serverTimestamp(),
        adminId: auth.currentUser.uid,
        targetUserId: session.targetUserId,
        amount,
        title: "Vote Result Applied",
        description:
          `Final average from ${Math.max(Number(session.proposalCount || 1), 1)} proposals. ` +
          (session.reason || "Awarded through AuraVote session."),
        photoUrl: ""
      });
    });

    if (action === "approve") {
      toast("Vote proposal approved. Voting is now open for 1 minute.");
    } else if (action === "reject") {
      toast("Vote proposal rejected.");
    } else if (action === "close") {
      toast("Voting session closed.");
    } else {
      toast("Vote result applied.");
    }
  } catch (error) {
    toast(error.message || "Session action failed.");
  }
}

function renderAuraVoteSessions() {
  if (!els.auraVoteSessionsList) {
    return;
  }

  els.auraVoteSessionsList.innerHTML = "";

  if (state.voteSessions.length === 0) {
    const li = document.createElement("li");
    li.className = "list-item";
    li.textContent = "No vote sessions yet.";
    els.auraVoteSessionsList.appendChild(li);
    return;
  }

  state.voteSessions.forEach((session) => {
    const targetName = state.usersById.get(session.targetUserId)?.displayName || "Unknown";
    const creatorName = state.usersById.get(session.createdByUserId)?.displayName || "Unknown";
    const myVote = state.mySessionVotes.get(session.id);
    const status = session.status || "pending_approval";
    const isOpen = status === "open";
    const remainingLabel = getTimeRemainingLabel(session.votingEndsAtMs);
    const canVote = isOpen && !myVote && remainingLabel !== "Ended";
    const proposalCount = Math.max(Number(session.proposalCount || 1), 1);
    const averageAmount = computeVoteResultAmount(session);

    const li = document.createElement("li");
    li.className = "list-item";
    li.innerHTML = `
      <div class="list-item-head">
        <strong>${escapeHtml(targetName)} • Proposed ${formatSigned(session.proposedAmount || 0)} Aura</strong>
        <span>${escapeHtml(String(status).toUpperCase())}</span>
      </div>
      <p class="muted">Reason: ${escapeHtml(session.reason || "No reason")}</p>
      <p class="muted">Created by: ${escapeHtml(creatorName)}</p>
      <p class="muted">Responses used for result: ${formatNumber(proposalCount)}</p>
      <p class="muted">Votes: UP ${formatNumber(session.upVotes || 0)} • DOWN ${formatNumber(session.downVotes || 0)}</p>
      <p class="muted">Result if applied now: ${formatSigned(averageAmount)} Aura</p>
      <p class="muted">Voting window: ${isOpen ? remainingLabel : "Not open"}</p>
      <label class="muted">Counter-offer amount (optional)
        <input type="number" step="1" placeholder="Leave empty to keep proposed amount" data-proposal-input-for="${session.id}" ${canVote ? "" : "disabled"} />
      </label>
      <div class="row">
        <button class="btn btn-tonal" type="button" data-session-id="${session.id}" data-vote="up" ${canVote ? "" : "disabled"}>Upvote</button>
        <button class="btn btn-danger" type="button" data-session-id="${session.id}" data-vote="down" ${canVote ? "" : "disabled"}>Downvote</button>
      </div>
      ${myVote ? `<p class="muted">Your vote: ${escapeHtml(myVote)}</p>` : ""}
    `;
    els.auraVoteSessionsList.appendChild(li);
  });
}

function renderAdminVoteSessions() {
  if (!els.adminVoteSessionsList) {
    return;
  }

  if (state.me?.role !== "admin") {
    els.adminVoteSessionsList.innerHTML = "";
    return;
  }

  els.adminVoteSessionsList.innerHTML = "";

  const manageableSessions = state.voteSessions.filter((session) => session.status !== "pending_approval");

  if (manageableSessions.length === 0) {
    const li = document.createElement("li");
    li.className = "list-item";
    li.textContent = "No vote sessions to manage.";
    els.adminVoteSessionsList.appendChild(li);
    return;
  }

  manageableSessions.forEach((session) => {
    const targetName = state.usersById.get(session.targetUserId)?.displayName || "Unknown";
    const status = session.status || "closed";
    const canClose = status === "open";
    const canApply = status === "closed" && Number(session.proposalCount || 0) > 0;
    const proposalCount = Math.max(Number(session.proposalCount || 1), 1);
    const averageAmount = computeVoteResultAmount(session);

    const li = document.createElement("li");
    li.className = "list-item";
    li.innerHTML = `
      <div class="list-item-head">
        <strong>${escapeHtml(targetName)} • Avg ${formatSigned(averageAmount)} Aura</strong>
        <span>${escapeHtml(String(status).toUpperCase())}</span>
      </div>
      <p class="muted">Reason: ${escapeHtml(session.reason || "No reason")}</p>
      <p class="muted">Proposed: ${formatSigned(session.proposedAmount || 0)} Aura</p>
      <p class="muted">Proposal count: ${formatNumber(proposalCount)}</p>
      <p class="muted">Votes: UP ${formatNumber(session.upVotes || 0)} • DOWN ${formatNumber(session.downVotes || 0)}</p>
      <div class="row">
        ${canClose ? `<button class="btn btn-ghost" type="button" data-session-id="${session.id}" data-session-action="close">Close</button>` : ""}
        ${canApply ? `<button class="btn btn-primary" type="button" data-session-id="${session.id}" data-session-action="apply">Apply Aura</button>` : ""}
      </div>
    `;
    els.adminVoteSessionsList.appendChild(li);
  });
}

function renderVoteApprovals(rows) {
  if (!els.voteApprovalsList) {
    return;
  }

  els.voteApprovalsList.innerHTML = "";

  if (!rows || rows.length === 0) {
    const li = document.createElement("li");
    li.className = "list-item";
    li.textContent = "No vote proposals awaiting approval.";
    els.voteApprovalsList.appendChild(li);
    return;
  }

  rows.forEach((session) => {
    const targetName = state.usersById.get(session.targetUserId)?.displayName || "Unknown";
    const creatorName = state.usersById.get(session.createdByUserId)?.displayName || "Unknown";

    const li = document.createElement("li");
    li.className = "list-item";
    li.innerHTML = `
      <div class="list-item-head">
        <strong>${escapeHtml(targetName)} • ${formatSigned(session.proposedAmount || 0)} Aura</strong>
        <span>PENDING</span>
      </div>
      <p class="muted">Created by: ${escapeHtml(creatorName)}</p>
      <p class="muted">Reason: ${escapeHtml(session.reason || "No reason")}</p>
      <div class="row">
        <button class="btn btn-primary" type="button" data-session-id="${session.id}" data-session-action="approve">Approve</button>
        <button class="btn btn-danger" type="button" data-session-id="${session.id}" data-session-action="reject">Reject</button>
      </div>
    `;
    els.voteApprovalsList.appendChild(li);
  });
}

function maybeAutoCloseVoteSessions() {
  state.voteSessions.forEach((session) => {
    if (session.status !== "open") {
      return;
    }

    const voteCount = Number(session.upVotes || 0) + Number(session.downVotes || 0);
    const expectedVoters = Math.max(1, Number(session.expectedVoters || 0));
    const endedByCount = voteCount >= expectedVoters;
    const endedByTime = Number(session.votingEndsAtMs || 0) > 0 && Date.now() >= Number(session.votingEndsAtMs || 0);

    if (!endedByCount && !endedByTime) {
      return;
    }

    const reason = endedByCount ? "all_votes" : "timeout";
    void closeVoteSessionIfNeeded(session.id, reason);
  });
}

async function closeVoteSessionIfNeeded(sessionId, reason) {
  try {
    await runTransaction(db, async (transaction) => {
      const sessionRef = doc(db, "auraVoteSessions", sessionId);
      const sessionSnap = await transaction.get(sessionRef);
      if (!sessionSnap.exists() || sessionSnap.data().status !== "open") {
        return;
      }

      transaction.update(sessionRef, {
        status: "closed",
        closedReason: reason,
        closedAt: serverTimestamp()
      });
    });
  } catch {
    // Non-fatal race with other clients auto-closing at the same time.
  }
}

function getVoteExpectedCount() {
  return Math.max(1, state.users.length || 0);
}

function computeVoteResultAmount(session) {
  const count = Math.max(Number(session.proposalCount || 1), 1);
  const sum = Number(session.proposalSum || session.proposedAmount || 0);
  return Math.round(sum / count);
}

function getTimeRemainingLabel(votingEndsAtMs) {
  const endMs = Number(votingEndsAtMs || 0);
  if (!endMs) {
    return "Not started";
  }

  const remaining = endMs - Date.now();
  if (remaining <= 0) {
    return "Ended";
  }

  const sec = Math.ceil(remaining / 1000);
  return `${sec}s remaining`;
}

async function handleSendCoin(event) {
  event.preventDefault();

  const senderId = auth.currentUser?.uid;
  const receiverId = els.coinTarget?.value;

  if (!senderId || !receiverId) {
    toast("Select a valid recipient.");
    return;
  }

  try {
    await runTransaction(db, async (transaction) => {
      const senderRef = doc(db, "users", senderId);
      const receiverRef = doc(db, "users", receiverId);
      const senderSnap = await transaction.get(senderRef);
      const receiverSnap = await transaction.get(receiverRef);

      if (!senderSnap.exists() || !receiverSnap.exists()) {
        throw new Error("User profile not found.");
      }

      const senderCoins = Number(senderSnap.data().auraCoins || 0);
      if (senderCoins < 1) {
        throw new Error("Not enough AuraCoins.");
      }

      const receiverAura = Number(receiverSnap.data().auraPoints || 0) + 5000;

      transaction.update(senderRef, { auraCoins: senderCoins - 1 });
      transaction.update(receiverRef, { auraPoints: receiverAura });

      const logRef = doc(collection(db, "logs"));
      transaction.set(logRef, {
        timestamp: serverTimestamp(),
        adminId: senderId,
        targetUserId: receiverId,
        amount: 5000,
        title: "AuraCoin Transfer",
        description: "1 AuraCoin consumed by sender; receiver gained +5,000 Aura.",
        photoUrl: ""
      });
    });

    els.coinForm?.reset();
    toast("AuraCoin sent successfully.");
  } catch (error) {
    toast(error.message || "Coin transfer failed.");
  }
}

function renderTradeEstimates() {
  const auraInput = Number(els.tradeAuraAmount?.value || 0);
  const rawCoins = auraInput > 0 ? Math.floor(auraInput / 10000) : 0;

  if (els.tradeAuraRaw) {
    els.tradeAuraRaw.textContent = `${formatNumber(rawCoins)} AuraCoins`;
  }
  if (els.tradeAuraNet) {
    els.tradeAuraNet.textContent = `${formatNumber(rawCoins)} AuraCoins`;
  }

  const coinsInput = Number(els.tradeCoinsAmount?.value || 0);
  const rawAura = coinsInput > 0 ? coinsInput * 10000 : 0;
  const netAura = Math.floor(rawAura * 0.5);

  if (els.tradeCoinsRaw) {
    els.tradeCoinsRaw.textContent = `${formatNumber(rawAura)} Aura`;
  }
  if (els.tradeCoinsNet) {
    els.tradeCoinsNet.textContent = `${formatNumber(netAura)} Aura`;
  }
}

async function handleTradeAuraToCoins(event) {
  event.preventDefault();

  const uid = auth.currentUser?.uid;
  const auraAmount = Number(els.tradeAuraAmount?.value || 0);

  if (!uid || !Number.isFinite(auraAmount) || auraAmount < 10000 || auraAmount % 10000 !== 0) {
    toast("Trade amount must be a multiple of 10,000 Aura.");
    return;
  }

  const mintedCoins = Math.floor(auraAmount / 10000);

  try {
    await runTransaction(db, async (transaction) => {
      const userRef = doc(db, "users", uid);
      const userSnap = await transaction.get(userRef);

      if (!userSnap.exists()) {
        throw new Error("User profile not found.");
      }

      const currentAura = Number(userSnap.data().auraPoints || 0);
      const currentCoins = Number(userSnap.data().auraCoins || 0);
      if (currentAura < auraAmount) {
        throw new Error("Not enough Aura to trade.");
      }

      transaction.update(userRef, {
        auraPoints: currentAura - auraAmount,
        auraCoins: currentCoins + mintedCoins
      });

      const logRef = doc(collection(db, "logs"));
      transaction.set(logRef, {
        timestamp: serverTimestamp(),
        adminId: uid,
        targetUserId: uid,
        amount: -auraAmount,
        title: "AuraBank Trade In",
        description: `${formatNumber(auraAmount)} Aura traded for ${formatNumber(mintedCoins)} AuraCoins.`,
        photoUrl: ""
      });
    });

    if (els.tradeAuraForm) {
      els.tradeAuraForm.reset();
    }
    renderTradeEstimates();
    toast("Aura traded for AuraCoins.");
  } catch (error) {
    toast(error.message || "Trade failed.");
  }
}

async function handleTradeCoinsToAura(event) {
  event.preventDefault();

  const uid = auth.currentUser?.uid;
  const coinAmount = Number(els.tradeCoinsAmount?.value || 0);

  if (!uid || !Number.isInteger(coinAmount) || coinAmount < 1) {
    toast("Enter a valid AuraCoin amount.");
    return;
  }

  const rawAura = coinAmount * 10000;
  const netAura = Math.floor(rawAura * 0.5);

  try {
    await runTransaction(db, async (transaction) => {
      const userRef = doc(db, "users", uid);
      const userSnap = await transaction.get(userRef);

      if (!userSnap.exists()) {
        throw new Error("User profile not found.");
      }

      const currentAura = Number(userSnap.data().auraPoints || 0);
      const currentCoins = Number(userSnap.data().auraCoins || 0);
      if (currentCoins < coinAmount) {
        throw new Error("Not enough AuraCoins to redeem.");
      }

      transaction.update(userRef, {
        auraPoints: currentAura + netAura,
        auraCoins: currentCoins - coinAmount
      });

      const logRef = doc(collection(db, "logs"));
      transaction.set(logRef, {
        timestamp: serverTimestamp(),
        adminId: uid,
        targetUserId: uid,
        amount: netAura,
        title: "AuraBank Redeem",
        description:
          `${formatNumber(coinAmount)} AuraCoins redeemed. Raw: ${formatNumber(rawAura)} Aura; Net after 50% tax: ${formatNumber(netAura)} Aura.`,
        photoUrl: ""
      });
    });

    if (els.tradeCoinsForm) {
      els.tradeCoinsForm.reset();
    }
    renderTradeEstimates();
    toast("AuraCoins redeemed to Aura.");
  } catch (error) {
    toast(error.message || "Redeem failed.");
  }
}

async function handleAdminAuraSubmit(event) {
  event.preventDefault();

  if (state.me?.role !== "admin") {
    toast("Admin access required.");
    return;
  }

  const targetUserId = els.adminTarget?.value;
  const amount = Number(els.adminAmount?.value);
  const title = els.adminTitle?.value.trim();
  const description = els.adminDescription?.value.trim();

  if (!targetUserId || Number.isNaN(amount)) {
    toast("Provide a valid target and amount.");
    return;
  }

  try {
    let photoUrl = "";
    const file = els.adminPhoto?.files?.[0] || null;

    if (file) {
      const photoRef = ref(storage, `logs/${Date.now()}-${file.name}`);
      await uploadBytes(photoRef, file);
      photoUrl = await getDownloadURL(photoRef);
    }

    await runTransaction(db, async (transaction) => {
      const targetRef = doc(db, "users", targetUserId);
      const targetSnap = await transaction.get(targetRef);

      if (!targetSnap.exists()) {
        throw new Error("Target user does not exist.");
      }

      const previousAura = Number(targetSnap.data().auraPoints || 0);
      const previousCoins = Number(targetSnap.data().auraCoins || 0);
      const gainedCoins = amount > 0 ? Math.floor(amount / 10000) : 0;

      transaction.update(targetRef, {
        auraPoints: Math.max(0, previousAura + amount),
        auraCoins: previousCoins + gainedCoins
      });

      const logRef = doc(collection(db, "logs"));
      transaction.set(logRef, {
        timestamp: serverTimestamp(),
        adminId: auth.currentUser.uid,
        targetUserId,
        amount,
        title,
        description,
        photoUrl
      });
    });

    els.adminAuraForm?.reset();
    toast("Aura transaction saved.");
  } catch (error) {
    toast(error.message || "Aura update failed.");
  }
}

async function handleRequestDecision(requestId, action) {
  if (state.me?.role !== "admin") {
    toast("Admin access required.");
    return;
  }

  try {
    const requestRef = doc(db, "requests", requestId);

    if (action === "reject") {
      await updateDoc(requestRef, { status: "rejected" });
      toast("Request rejected.");
      return;
    }

    await runTransaction(db, async (transaction) => {
      const requestSnap = await transaction.get(requestRef);
      if (!requestSnap.exists()) {
        throw new Error("Request not found.");
      }

      const request = requestSnap.data();
      if (request.status !== "pending") {
        throw new Error("Request already processed.");
      }

      const targetRef = doc(db, "users", request.requesterId);
      const targetSnap = await transaction.get(targetRef);
      if (!targetSnap.exists()) {
        throw new Error("Requester profile missing.");
      }

      const prevAura = Number(targetSnap.data().auraPoints || 0);
      const prevCoins = Number(targetSnap.data().auraCoins || 0);
      const amount = Number(request.requestedAmount || 0);
      const gainedCoins = amount > 0 ? Math.floor(amount / 10000) : 0;

      transaction.update(targetRef, {
        auraPoints: prevAura + amount,
        auraCoins: prevCoins + gainedCoins
      });

      transaction.update(requestRef, { status: "approved" });

      const logRef = doc(collection(db, "logs"));
      transaction.set(logRef, {
        timestamp: serverTimestamp(),
        adminId: auth.currentUser.uid,
        targetUserId: request.requesterId,
        amount,
        title: "Request Approved",
        description: request.reason || "Participant request approved by admin.",
        photoUrl: ""
      });
    });

    toast("Request approved.");
  } catch (error) {
    toast(error.message || "Failed to process request.");
  }
}

function renderPersonalHistories() {
  renderAuraHistory();
  renderCoinHistory();
}

function renderAuraHistory() {
  if (!els.auraHistoryList) {
    return;
  }

  const rows = state.personalLogs.filter((entry) => entry.title !== "AuraCoin Transfer").slice(0, 40);
  els.auraHistoryList.innerHTML = "";

  if (rows.length === 0) {
    const li = document.createElement("li");
    li.className = "list-item";
    li.textContent = "No Aura history yet.";
    els.auraHistoryList.appendChild(li);
    return;
  }

  rows.forEach((entry) => {
    const actorUser = state.usersById.get(entry.adminId);
    const actor = actorUser?.displayName || "System";
    const actorColor = safeProfileColor(actorUser?.profileColor);
    const contestMeta = getContestMeta(entry);
    const li = document.createElement("li");
    li.className = "list-item";
    li.innerHTML = `
      <div class="list-item-head">
        <strong>${escapeHtml(entry.title || "Aura Update")}</strong>
        <span class="${amountClass(entry.amount)}">${formatSigned(entry.amount || 0)} Aura</span>
      </div>
      <p class="muted">${escapeHtml(entry.description || "No description")}</p>
      <p class="muted">By <span style="color:${actorColor}">${escapeHtml(actor)}</span> • ${formatTimestamp(entry.timestamp)}</p>
      ${contestMeta.message ? `<p class="muted">${escapeHtml(contestMeta.message)}</p>` : ""}
      ${contestMeta.canContest ? `<div class="list-item-actions"><button class="btn btn-tonal" type="button" data-contest-log-id="${entry.id}">Contest</button></div>` : ""}
    `;
    els.auraHistoryList.appendChild(li);
  });
}

function renderCoinHistory() {
  if (!els.coinHistoryList) {
    return;
  }

  const uid = auth.currentUser?.uid;
  const rows = state.personalLogs.filter((entry) => entry.title === "AuraCoin Transfer").slice(0, 40);
  els.coinHistoryList.innerHTML = "";

  if (rows.length === 0) {
    const li = document.createElement("li");
    li.className = "list-item";
    li.textContent = "No AuraCoin history yet.";
    els.coinHistoryList.appendChild(li);
    return;
  }

  rows.forEach((entry) => {
    const isSender = entry.adminId === uid;
    const counterpartyId = isSender ? entry.targetUserId : entry.adminId;
    const counterpartyName = state.usersById.get(counterpartyId)?.displayName || "Unknown";

    const li = document.createElement("li");
    li.className = "list-item";
    li.innerHTML = `
      <div class="list-item-head">
        <strong>${isSender ? "Sent 1 AuraCoin" : "Received Aura from coin"}</strong>
        <span class="${isSender ? "amount-negative" : "amount-positive"}">${isSender ? "-1 Coin" : "+5,000 Aura"}</span>
      </div>
      <p class="muted">Counterparty: ${escapeHtml(counterpartyName)}</p>
      <p class="muted">${formatTimestamp(entry.timestamp)}</p>
    `;
    els.coinHistoryList.appendChild(li);
  });
}

function handleAuraHistoryClick(event) {
  const btn = event.target.closest("button[data-contest-log-id]");
  if (!btn) {
    return;
  }

  handleCreateContestPoll(btn.dataset.contestLogId);
}

async function handleCreateContestPoll(logId) {
  const entry = state.personalLogs.find((row) => row.id === logId);
  if (!entry) {
    toast("History item not found.");
    return;
  }

  const meta = getContestMeta(entry);
  if (!meta.canContest) {
    toast(meta.message || "This record cannot be contested.");
    return;
  }

  try {
    await addDoc(collection(db, "auraContestPolls"), {
      timestamp: serverTimestamp(),
      createdByUserId: auth.currentUser.uid,
      logId: entry.id,
      targetUserId: entry.targetUserId,
      sourceAdminId: entry.adminId,
      originalAmount: Number(entry.amount || 0),
      reason: entry.description || "Aura history contested by participant.",
      status: "open",
      upVotes: 0,
      downVotes: 0
    });

    toast("Contest poll created.");
  } catch (error) {
    toast(error.message || "Could not create contest poll.");
  }
}

function handleContestPollsClick(event) {
  const btn = event.target.closest("button[data-contest-id][data-vote]");
  if (!btn) {
    return;
  }

  handleCastContestVote(btn.dataset.contestId, btn.dataset.vote);
}

async function handleCastContestVote(pollId, voteType) {
  if (!pollId || !["up", "down"].includes(voteType)) {
    return;
  }

  if (state.myContestVotes.has(pollId)) {
    toast("You already voted on this contest.");
    return;
  }

  try {
    await runTransaction(db, async (transaction) => {
      const pollRef = doc(db, "auraContestPolls", pollId);
      const voteRef = doc(db, "auraContestVotes", `${pollId}_${auth.currentUser.uid}`);

      const [pollSnap, voteSnap] = await Promise.all([
        transaction.get(pollRef),
        transaction.get(voteRef)
      ]);

      if (!pollSnap.exists()) {
        throw new Error("Contest poll not found.");
      }

      const poll = pollSnap.data();
      if (poll.status !== "open") {
        throw new Error("Contest poll is closed.");
      }
      if (isContestExpired(poll.timestamp)) {
        throw new Error("Contest voting window has expired.");
      }
      if (voteSnap.exists()) {
        throw new Error("You already voted on this contest.");
      }

      const nextUp = Number(poll.upVotes || 0) + (voteType === "up" ? 1 : 0);
      const nextDown = Number(poll.downVotes || 0) + (voteType === "down" ? 1 : 0);

      transaction.update(pollRef, { upVotes: nextUp, downVotes: nextDown });
      transaction.set(voteRef, {
        timestamp: serverTimestamp(),
        pollId,
        voterId: auth.currentUser.uid,
        voteType
      });
    });

    toast("Contest vote submitted.");
  } catch (error) {
    toast(error.message || "Could not submit contest vote.");
  }
}

function renderContestPolls() {
  if (!els.contestPollsList) {
    return;
  }

  els.contestPollsList.innerHTML = "";

  if (state.contestPolls.length === 0) {
    const li = document.createElement("li");
    li.className = "list-item";
    li.textContent = "No contest polls yet.";
    els.contestPollsList.appendChild(li);
    return;
  }

  state.contestPolls.forEach((poll) => {
    const targetUser = state.usersById.get(poll.targetUserId);
    const targetName = targetUser?.displayName || "Unknown";
    const targetColor = safeProfileColor(targetUser?.profileColor);
    const myVote = state.myContestVotes.get(poll.id);
    const expired = isContestExpired(poll.timestamp);
    const canVote = poll.status === "open" && !expired && !myVote;

    const li = document.createElement("li");
    li.className = "list-item";
    li.innerHTML = `
      <div class="list-item-head">
        <strong style="color:${targetColor}">${escapeHtml(targetName)}</strong>
        <span class="${amountClass(poll.originalAmount)}">${formatSigned(poll.originalAmount || 0)} Aura</span>
      </div>
      <p class="muted">Contested reason: ${escapeHtml(poll.reason || "No reason")}</p>
      <p class="muted">Votes: UP ${formatNumber(poll.upVotes || 0)} • DOWN ${formatNumber(poll.downVotes || 0)}</p>
      <p class="muted">Status: ${escapeHtml(String(poll.status || "open").toUpperCase())}${expired ? " (EXPIRED)" : ""}</p>
      <div class="row">
        <button class="btn btn-tonal" type="button" data-contest-id="${poll.id}" data-vote="up" ${canVote ? "" : "disabled"}>Upvote</button>
        <button class="btn btn-danger" type="button" data-contest-id="${poll.id}" data-vote="down" ${canVote ? "" : "disabled"}>Downvote</button>
      </div>
      ${myVote ? `<p class="muted">Your vote: ${escapeHtml(myVote)}</p>` : ""}
    `;
    els.contestPollsList.appendChild(li);
  });
}

function getContestMeta(entry) {
  const ageMs = Date.now() - toMillis(entry.timestamp);
  const windowMs = 48 * 60 * 60 * 1000;
  const existing = state.contestPollByLogId.get(entry.id);

  if (!entry?.id) {
    return { canContest: false, message: "" };
  }
  if (ageMs < 0 || ageMs > windowMs) {
    return { canContest: false, message: "Contest window expired (48h)." };
  }
  if (existing) {
    return { canContest: false, message: "Already contested." };
  }

  return { canContest: true, message: "Contest available within 48h." };
}

function isContestExpired(timestampValue) {
  const created = toMillis(timestampValue);
  if (!created) {
    return false;
  }
  return Date.now() - created > 48 * 60 * 60 * 1000;
}

function filterPersonalLogs(logs, uid) {
  return logs
    .filter((entry) => entry.targetUserId === uid || entry.adminId === uid)
    .sort((a, b) => toMillis(b.timestamp) - toMillis(a.timestamp));
}

async function handleProfileSubmit(event) {
  event.preventDefault();

  const uid = auth.currentUser?.uid;
  if (!uid || state.selectedProfileUserId !== uid) {
    toast("You can only edit your own profile.");
    return;
  }

  const displayName = els.profileDisplayName?.value.trim();
  const email = els.profileEmail?.value.trim();
  const photoURL = els.profilePhotoUrl?.value.trim() || "";
  const profileColor = safeProfileColor(els.profileColor?.value);

  if (!uid || !displayName || displayName.length < 2) {
    toast("Display name must be at least 2 characters.");
    return;
  }

  if (!email || !email.includes("@")) {
    toast("Provide a valid email for your profile.");
    return;
  }

  try {
    await updateDoc(doc(db, "users", uid), {
      displayName,
      email,
      photoURL,
      profileColor
    });
    toast("Profile updated.");
  } catch (error) {
    toast(error.message || "Could not save profile.");
  }
}

function syncProfileForm() {
  if (!state.me || !els.profileDisplayName || !els.profileEmail || !els.profilePhotoUrl || !els.profileColor) {
    return;
  }

  const nextName = state.me.displayName || "";
  const nextEmail = state.me.email || auth.currentUser?.email || "";
  const nextPhoto = state.me.photoURL || "";
  const nextColor = safeProfileColor(state.me.profileColor);

  if (els.profileDisplayName.value !== nextName) {
    els.profileDisplayName.value = nextName;
  }
  if (els.profileEmail.value !== nextEmail) {
    els.profileEmail.value = nextEmail;
  }
  if (els.profilePhotoUrl.value !== nextPhoto) {
    els.profilePhotoUrl.value = nextPhoto;
  }
  if (els.profileColor.value !== nextColor) {
    els.profileColor.value = nextColor;
  }
}

function setProfileEditability(canEdit, profileUser) {
  const controls = [els.profileDisplayName, els.profileEmail, els.profilePhotoUrl, els.profileColor];
  controls.forEach((input) => {
    if (input) {
      input.disabled = !canEdit;
    }
  });

  if (els.profileSaveBtn) {
    els.profileSaveBtn.disabled = !canEdit;
    els.profileSaveBtn.textContent = canEdit ? "Save Profile" : "Read Only Profile";
  }

  if (!profileUser) {
    return;
  }

  if (!canEdit) {
    if (els.profileDisplayName) {
      els.profileDisplayName.value = profileUser.displayName || "";
    }
    if (els.profileEmail) {
      els.profileEmail.value = profileUser.email || "";
    }
    if (els.profilePhotoUrl) {
      els.profilePhotoUrl.value = profileUser.photoURL || "";
    }
    if (els.profileColor) {
      els.profileColor.value = safeProfileColor(profileUser.profileColor);
    }
  }
}

async function loadViewPartials() {
  const sections = Array.from(document.querySelectorAll("[data-partial]"));

  await Promise.all(
    sections.map(async (section) => {
      const path = section.dataset.partial;
      if (!path) {
        return;
      }

      const rootPath = path.replace(/^views\//, "");
      const candidates = path === rootPath ? [path] : [rootPath, path];

      let html = null;
      for (const candidate of candidates) {
        try {
          const response = await fetch(candidate, { cache: "no-store" });
          if (!response.ok) {
            continue;
          }
          html = await response.text();
          break;
        } catch {
          // Try next candidate.
        }
      }

      if (html == null) {
        const name = path.replace(/^views\//, "").replace(/\.html$/, "");
        section.innerHTML = `<article class="panel glass"><p class="muted">⚠️ Could not load ${escapeHtml(name)}. Check file is at root or views/.</p></article>`;
        return;
      }

      section.innerHTML = html;
    })
  );
}

function cacheDynamicElements() {
  els = {
    myAura: document.getElementById("my-aura"),
    myLevel: document.getElementById("my-level"),
    myMastery: document.getElementById("my-mastery"),
    myCoins: document.getElementById("my-coins"),
    bankAura: document.getElementById("bank-aura"),
    bankCoins: document.getElementById("bank-coins"),
    auraHistoryList: document.getElementById("aura-history-list"),
    coinHistoryList: document.getElementById("coin-history-list"),
    tradeAuraForm: document.getElementById("trade-aura-form"),
    tradeAuraAmount: document.getElementById("trade-aura-amount"),
    tradeAuraRaw: document.getElementById("trade-aura-raw"),
    tradeAuraNet: document.getElementById("trade-aura-net"),
    tradeCoinsForm: document.getElementById("trade-coins-form"),
    tradeCoinsAmount: document.getElementById("trade-coins-amount"),
    tradeCoinsRaw: document.getElementById("trade-coins-raw"),
    tradeCoinsNet: document.getElementById("trade-coins-net"),
    groupParticipantsList: document.getElementById("group-participants-list"),
    statTotalAura: document.getElementById("stat-total-aura"),
    statAvgDailyWon: document.getElementById("stat-avg-daily-won"),
    statAvgDailyLost: document.getElementById("stat-avg-daily-lost"),
    statTopLoser: document.getElementById("stat-top-loser"),
    statTopWinner: document.getElementById("stat-top-winner"),
    leaderboardList: document.getElementById("leaderboard-list"),
    auraMovementsList: document.getElementById("aura-movements-list"),
    contestPollsList: document.getElementById("contest-polls-list"),
    requestForm: document.getElementById("request-form"),
    requestTarget: document.getElementById("request-target"),
    requestAmount: document.getElementById("request-amount"),
    requestReason: document.getElementById("request-reason"),
    voteSessionForm: document.getElementById("vote-session-form"),
    voteSessionTarget: document.getElementById("vote-session-target"),
    voteSessionReason: document.getElementById("vote-session-reason"),
    voteSessionAmount: document.getElementById("vote-session-amount"),
    auraVoteSessionsList: document.getElementById("aura-vote-sessions-list"),
    adminVoteSessionsList: document.getElementById("admin-vote-sessions-list"),
    coinForm: document.getElementById("coin-form"),
    coinTarget: document.getElementById("coin-target"),
    adminView: document.getElementById("admin-view"),
    adminAuraForm: document.getElementById("admin-aura-form"),
    adminTarget: document.getElementById("admin-target"),
    adminAmount: document.getElementById("admin-amount"),
    adminTitle: document.getElementById("admin-title"),
    adminDescription: document.getElementById("admin-description"),
    adminPhoto: document.getElementById("admin-photo"),
    voteApprovalsList: document.getElementById("vote-approvals-list"),
    pendingRequestsList: document.getElementById("pending-requests-list"),
    logsList: document.getElementById("logs-list"),
    profileForm: document.getElementById("profile-form"),
    profileDisplayName: document.getElementById("profile-display-name"),
    profileEmail: document.getElementById("profile-email"),
    profilePhotoUrl: document.getElementById("profile-photo-url"),
    profileColor: document.getElementById("profile-color"),
    profileSaveBtn: document.getElementById("profile-save-btn"),
    profileInspectorCard: document.getElementById("profile-inspector-card"),
    inspectorPhoto: document.getElementById("inspector-photo"),
    inspectorName: document.getElementById("inspector-name"),
    inspectorEmail: document.getElementById("inspector-email"),
    inspectorGps: document.getElementById("inspector-gps"),
    inspectorGpsUpdated: document.getElementById("inspector-gps-updated"),
    inspectorLastLogin: document.getElementById("inspector-last-login"),
    inspectorLevel: document.getElementById("inspector-level"),
    inspectorMastery: document.getElementById("inspector-mastery"),
    inspectorPerformance: document.getElementById("inspector-performance"),
  };
}

export function calculateLevelAndMastery(auraPoints) {
  const points = Math.max(0, Number(auraPoints || 0));
  const totalLevels = Math.floor(points / 1000);
  const masteryTier = Math.floor(totalLevels / 100);
  const level = totalLevels % 100;

  return {
    totalLevels,
    masteryTier,
    level,
    masteryLabel: masteryTier > 0 ? `Mastery ${toRoman(masteryTier)}` : "None"
  };
}

function toRoman(value) {
  const base = [
    [1000, "M"],
    [900, "CM"],
    [500, "D"],
    [400, "CD"],
    [100, "C"],
    [90, "XC"],
    [50, "L"],
    [40, "XL"],
    [10, "X"],
    [9, "IX"],
    [5, "V"],
    [4, "IV"],
    [1, "I"]
  ];

  if (value <= 0 || value > 3999) {
    return String(value);
  }

  let result = "";
  let remaining = value;
  for (const [num, symbol] of base) {
    while (remaining >= num) {
      result += symbol;
      remaining -= num;
    }
  }

  return result;
}

function toast(message) {
  shell.toast.textContent = message;
  shell.toast.classList.remove("hidden");

  setTimeout(() => {
    shell.toast.classList.add("hidden");
  }, 2200);
}

function formatNumber(value) {
  return new Intl.NumberFormat().format(Number(value || 0));
}

function formatSigned(value) {
  const n = Number(value || 0);
  return `${n > 0 ? "+" : ""}${formatNumber(n)}`;
}

function amountClass(value) {
  const n = Number(value || 0);
  if (n > 0) {
    return "amount-positive";
  }
  if (n < 0) {
    return "amount-negative";
  }
  return "amount-neutral";
}

function safeProfileColor(color) {
  const value = String(color || "").trim();
  return /^#[0-9a-fA-F]{6}$/.test(value) ? value : "#86b8ff";
}

function calculatePerformanceStatus(auraPoints) {
  const score = Number(auraPoints || 0);
  const participants = state.users.filter((user) => user.role !== "admin");
  const average =
    participants.length > 0
      ? participants.reduce((sum, user) => sum + Number(user.auraPoints || 0), 0) / participants.length
      : 0;

  if (score < 0 || (average > 0 && score < average * 0.4)) {
    return { label: "Way Below Group Average", className: "status-danger" };
  }
  if (average > 0 && score < average * 0.85) {
    return { label: "Below Group Average", className: "status-warn" };
  }
  return { label: "At/Close/Above Group Average", className: "status-good" };
}

function formatTimestamp(timestampValue) {
  const ms = toMillis(timestampValue);
  if (!ms) {
    return "Unknown";
  }
  return new Date(ms).toLocaleString();
}

function toMillis(timestampValue) {
  if (!timestampValue) {
    return 0;
  }
  if (typeof timestampValue.toMillis === "function") {
    return timestampValue.toMillis();
  }
  if (typeof timestampValue.toDate === "function") {
    return timestampValue.toDate().getTime();
  }
  if (typeof timestampValue === "number") {
    return timestampValue;
  }
  return 0;
}

function escapeHtml(input) {
  return String(input)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#039;");
}






