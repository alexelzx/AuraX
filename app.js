const STORAGE_KEY = "aurax.state.v1";
const PROFILE_KEY = "aurax.profile.v1";
const EVENT_LIMIT = 120;
const MASTERIES = [
  "Mastery I",
  "Mastery II",
  "Mastery III",
  "Mastery IV",
  "Mastery V",
  "Mastery VI",
  "Mastery VII",
  "Mastery VIII",
  "Mastery IX",
  "Mastery X",
];

const FIREBASE_DOC_PATH = ["aurax", "state"];

const seedUsers = [
  { id: "u1", name: "Nova", aura: 124500, auraCoins: 12, votes: 84, voteAuraReceived: 15600, lat: 37.7749, lng: -122.4194 },
  { id: "u2", name: "Lumen", aura: 98200, auraCoins: 9, votes: 63, voteAuraReceived: 9200, lat: 34.0522, lng: -118.2437 },
  { id: "u3", name: "Prism", aura: 88600, auraCoins: 8, votes: 51, voteAuraReceived: 7300, lat: 40.7128, lng: -74.006 },
  { id: "u4", name: "Flux", aura: 76000, auraCoins: 7, votes: 42, voteAuraReceived: 5400, lat: 51.5074, lng: -0.1278 },
  { id: "u5", name: "Halo", aura: 65100, auraCoins: 6, votes: 37, voteAuraReceived: 4300, lat: 48.8566, lng: 2.3522 },
  { id: "u6", name: "Orbit", aura: 52200, auraCoins: 5, votes: 29, voteAuraReceived: 3100, lat: 35.6762, lng: 139.6503 },
  { id: "u7", name: "Pulse", aura: 43800, auraCoins: 4, votes: 21, voteAuraReceived: 2400, lat: 41.8781, lng: -87.6298 },
  { id: "u8", name: "Zenith", aura: 31500, auraCoins: 3, votes: 18, voteAuraReceived: 1800, lat: 52.52, lng: 13.405 },
];

const state = {
  users: [],
  requests: [],
  events: [],
  currentUserId: "u1",
  mode: "participant",
  uiMode: "mobile",
  activeTab: "dashboard",
  location: null,
  selectedTargetId: null,
};

const els = {};
const toastTimers = new Map();
const txSeed = Date.now();
const firebaseState = {
  ready: false,
  syncing: false,
  disabled: false,
  remoteVersion: 0,
  app: null,
  db: null,
  auth: null,
  mods: null,
};

init();

function init() {
  bindElements();
  hydrateState();
  wireEvents();
  ensureCurrentTarget();
  render();
  void initFirebase();
  requestAnimationFrame(() => {
    document.body.style.opacity = "1";
  });
}

async function initFirebase() {
  const config = window.AURAX_FIREBASE_CONFIG;
  if (!config) return;

  try {
    const [{ initializeApp }, { getAuth, onAuthStateChanged, signInWithEmailAndPassword, createUserWithEmailAndPassword, signOut }, { getFirestore, doc, onSnapshot, setDoc, serverTimestamp }] = await Promise.all([
      import("https://www.gstatic.com/firebasejs/9.23.0/firebase-app.js"),
      import("https://www.gstatic.com/firebasejs/9.23.0/firebase-auth.js"),
      import("https://www.gstatic.com/firebasejs/9.23.0/firebase-firestore.js"),
    ]);

    firebaseState.app = initializeApp(config);
    firebaseState.auth = getAuth(firebaseState.app);
    firebaseState.mods = { doc, onSnapshot, setDoc, serverTimestamp, signInWithEmailAndPassword, createUserWithEmailAndPassword, signOut };

    onAuthStateChanged(firebaseState.auth, (user) => {
      if (user) {
        els.userEmail.textContent = user.email;
        els.signOutBtn.classList.remove("hidden");
        els.authToggleBtn.textContent = "Account";
        // now initialize Firestore syncing
        initFirestoreSync();

        // Ensure a users/{uid} document exists and load role
        (async () => {
          try {
            const { getFirestore, doc, getDoc, setDoc, serverTimestamp } = await import("https://www.gstatic.com/firebasejs/9.23.0/firebase-firestore.js");
            firebaseState.db = firebaseState.db || getFirestore(firebaseState.app);
            const userRef = doc(firebaseState.db, "users", user.uid);
            const snap = await getDoc(userRef);
            if (!snap.exists()) {
              await setDoc(userRef, { email: user.email, role: "participant", createdAt: serverTimestamp() });
              state.mode = "participant";
            } else {
              const data = snap.data();
              if (data?.role === "admin") {
                state.mode = "admin";
              } else {
                state.mode = "participant";
              }
            }
            persist();
            render();
          } catch (e) {
            console.warn("User doc check failed", e);
          }
        })();
      } else {
        els.userEmail.textContent = "";
        els.signOutBtn.classList.add("hidden");
        els.authToggleBtn.textContent = "Sign in";
      }
    });

    firebaseState.ready = true;
  } catch (error) {
    console.warn("Firebase initialization failed", error);
  }
}

async function initFirestoreSync() {
  if (!firebaseState.app || firebaseState.syncing) return;
  try {
    const { getFirestore, doc, onSnapshot } = await import("https://www.gstatic.com/firebasejs/9.23.0/firebase-firestore.js");
    firebaseState.db = getFirestore(firebaseState.app);
    const stateRef = doc(firebaseState.db, FIREBASE_DOC_PATH[0], FIREBASE_DOC_PATH[1]);

    firebaseState.syncing = true;
    firebaseState.mods.onSnapshot = onSnapshot;
    firebaseState.mods.doc = doc;

    onSnapshot(
      stateRef,
      (snapshot) => {
        const remote = snapshot.data();
        if (!remote?.state) {
          void queueFirebaseWrite();
          return;
        }

        const remoteVersion = remote.version ?? 0;
        if (remoteVersion <= firebaseState.remoteVersion) return;

        firebaseState.remoteVersion = remoteVersion;
        Object.assign(state, remote.state);
        ensureCurrentTarget();
        localStorage.setItem(STORAGE_KEY, JSON.stringify(serializeState()));
        render();
        firebaseState.syncing = false;
      },
      (error) => {
        firebaseState.disabled = true;
        firebaseState.ready = false;
        firebaseState.syncing = false;
        console.warn("Firebase snapshot disabled", error);
      },
    );

    void queueFirebaseWrite();
    toast("Firebase sync connected");
  } catch (error) {
    console.warn("Firestore sync unavailable", error);
  }
}

function bindElements() {
  const ids = [
    "themeModeBtn",
    "mobileModeBtn",
    "desktopModeBtn",
    "modeLabel",
    "balanceValue",
    "levelValue",
    "coinValue",
    "profileMasteryHint",
    "ringLevel",
    "ringMastery",
    "coinProgress",
    "leaderboard",
    "voteTargets",
    "requestTarget",
    "requestForm",
    "requestDelta",
    "requestTitle",
    "requestDescription",
    "coinForm",
    "coinTarget",
    "coinAmount",
    "adminForm",
    "adminTarget",
    "adminDelta",
    "adminTitle",
    "adminDescription",
    "adminPhoto",
    "requestInbox",
    "activityFeed",
    "seedBtn",
    "refreshViewBtn",
    "profileBadges",
    "badgeShelf",
    "achievementShelf",
    "masteryRing",
    "appTabs",
    "leaderboardItemTemplate",
      "userEmail",
      "authOverlay",
      "authForm",
      "authEmail",
      "authPassword",
      "signInBtn",
      "registerBtn",
      "signOutBtn",
      "authToggleBtn",
      "closeAuthBtn",
  ];

  for (const id of ids) {
    els[id] = document.getElementById(id);
  }
}

function hydrateState() {
  const saved = readJSON(STORAGE_KEY);
  if (saved?.users?.length) {
    state.users = saved.users;
    state.requests = saved.requests ?? [];
    state.events = saved.events ?? [];
    state.currentUserId = saved.currentUserId ?? "u1";
    state.mode = saved.mode ?? "participant";
    state.uiMode = saved.uiMode ?? "mobile";
    state.activeTab = saved.activeTab ?? "dashboard";
    state.location = saved.location ?? null;
    state.selectedTargetId = saved.selectedTargetId ?? null;
    return;
  }

  state.users = seedUsers.map((user, index) => ({
    ...user,
    votes: user.votes ?? 0,
    auraCoins: user.auraCoins ?? Math.floor(user.aura / 10000),
    avatar: `https://api.dicebear.com/9.x/identicon/svg?seed=AuraX-${index + 1}`,
  }));
  state.requests = [
    {
      id: createId("req"),
      targetId: "u1",
      delta: 1500,
      title: "Community spotlight",
      description: "Nova completed a 3x support streak and should get bonus aura.",
      status: "pending",
      author: "Pulse",
      createdAt: Date.now() - 1000 * 60 * 35,
    },
  ];
  state.events = [
    eventRecord("system", "AuraX initialized", "Demo board ready with live leaderboards, requests, and economy."),
    eventRecord("mint", "AuraCoin mint", "Users generate 1 AuraCoin for every 10,000 Aura."),
  ];
  state.selectedTargetId = "u2";
  persist();
}

function wireEvents() {
  els.themeModeBtn.addEventListener("click", () => {
    state.mode = state.mode === "admin" ? "participant" : "admin";
    if (state.mode === "participant" && state.activeTab === "admin") {
      state.activeTab = "dashboard";
    }
    addEvent("system", `Switched to ${capitalize(state.mode)} mode`, "Mode toggle changed the visible admin workflow.");
    persist();
    render();
  });

  els.mobileModeBtn.addEventListener("click", () => setUiMode("mobile"));
  els.desktopModeBtn.addEventListener("click", () => setUiMode("desktop"));

  els.appTabs.addEventListener("click", (event) => {
    const button = event.target.closest("[data-tab]");
    if (!button) return;
    setActiveTab(button.dataset.tab);
  });

  els.refreshViewBtn.addEventListener("click", () => {
    render();
    toast("View refreshed");
  });

  els.seedBtn.addEventListener("click", resetDemo);

  // Auth events
  if (els.authToggleBtn) {
    els.authToggleBtn.addEventListener("click", () => {
      els.authOverlay.classList.toggle("hidden");
    });
  }
  if (els.closeAuthBtn) {
    els.closeAuthBtn.addEventListener("click", () => {
      els.authOverlay.classList.add("hidden");
    });
  }
  if (els.signInBtn) {
    els.signInBtn.addEventListener("click", onSignIn);
  }
  if (els.registerBtn) {
    els.registerBtn.addEventListener("click", onRegister);
  }
  if (els.signOutBtn) {
    els.signOutBtn.addEventListener("click", onSignOut);
  }

  els.requestForm.addEventListener("submit", onRequestSubmit);
  els.coinForm.addEventListener("submit", onCoinSubmit);
  els.adminForm.addEventListener("submit", onAdminSubmit);

  document.addEventListener("click", onActionClick);
  document.addEventListener("input", onActionInput);
  window.addEventListener("storage", onStorageSync);
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      render();
    }
  });
}

async function onSignIn() {
  const email = els.authEmail.value.trim();
  const password = els.authPassword.value;
  if (!email || !password) {
    toast("Provide email and password");
    return;
  }
  try {
    const { signInWithEmailAndPassword } = firebaseState.mods;
    await signInWithEmailAndPassword(firebaseState.auth, email, password);
    toast("Signed in");
    els.authOverlay.classList.add("hidden");
  } catch (err) {
    console.warn(err);
    toast(err.message || "Sign-in failed");
  }
}

async function onRegister() {
  const email = els.authEmail.value.trim();
  const password = els.authPassword.value;
  if (!email || !password) {
    toast("Provide email and password");
    return;
  }
  try {
    const { createUserWithEmailAndPassword } = firebaseState.mods;
    const userCred = await createUserWithEmailAndPassword(firebaseState.auth, email, password);
    // create a users/{uid} doc for role and basic profile
    try {
      const { getFirestore, doc, setDoc, serverTimestamp } = await import("https://www.gstatic.com/firebasejs/9.23.0/firebase-firestore.js");
      firebaseState.db = firebaseState.db || getFirestore(firebaseState.app);
      await setDoc(doc(firebaseState.db, "users", userCred.user.uid), {
        email: userCred.user.email,
        role: "participant",
        createdAt: serverTimestamp(),
      });
    } catch (e) {
      console.warn("Failed to create user doc", e);
    }
    toast("Account created and signed in");
    els.authOverlay.classList.add("hidden");
  } catch (err) {
    console.warn(err);
    toast(err.message || "Registration failed");
  }
}

async function onSignOut() {
  try {
    const { signOut } = firebaseState.mods;
    await signOut(firebaseState.auth);
    toast("Signed out");
  } catch (err) {
    console.warn(err);
    toast(err.message || "Sign-out failed");
  }
}

function onStorageSync(event) {
  if (event.key !== STORAGE_KEY || !event.newValue) {
    return;
  }
  const next = readJSON(STORAGE_KEY);
  if (!next?.users?.length) {
    return;
  }
  state.users = next.users;
  state.requests = next.requests ?? [];
  state.events = next.events ?? [];
  state.currentUserId = next.currentUserId ?? state.currentUserId;
  state.mode = next.mode ?? state.mode;
  state.uiMode = next.uiMode ?? state.uiMode;
  state.activeTab = next.activeTab ?? state.activeTab;
  state.location = next.location ?? state.location;
  state.selectedTargetId = next.selectedTargetId ?? state.selectedTargetId;
  render();
}

function onActionInput(event) {
  const { target } = event;
  if (!(target instanceof HTMLElement)) return;

  if (target.id === "requestDelta" || target.id === "adminDelta" || target.id === "coinAmount") {
    target.value = clampNumber(target.value, -100000, 100000, target.id === "coinAmount" ? 1 : 0);
  }
}

function onActionClick(event) {
  const button = event.target.closest("[data-action]");
  if (!button) return;

  const action = button.dataset.action;
  const id = button.dataset.id;

  if (action === "vote-up") {
    castVote(id, 100, 1);
  }
  if (action === "vote-down") {
    castVote(id, -100, -1);
  }
  if (action === "approve-request") {
    approveRequest(id, true);
  }
  if (action === "reject-request") {
    approveRequest(id, false);
  }
  if (action === "select-target") {
    state.selectedTargetId = id;
    persist();
    renderTargets();
    toast("Participant selected");
  }
}

function onRequestSubmit(event) {
  event.preventDefault();
  const targetId = els.requestTarget.value;
  const delta = Number(els.requestDelta.value);
  const title = els.requestTitle.value.trim() || "Aura request";
  const description = els.requestDescription.value.trim() || "Submitted from AuraX participant mode.";

  const request = {
    id: createId("req"),
    targetId,
    delta,
    title,
    description,
    status: "pending",
    author: getCurrentUser().name,
    createdAt: Date.now(),
  };

  state.requests.unshift(request);
  addEvent("request", `Request submitted: ${title}`, `${request.author} asked for ${formatDelta(delta)} aura for ${getUser(targetId).name}.`);
  persist();
  eventReset(event.target);
  toast("Request sent to admin");
  render();
}

function onCoinSubmit(event) {
  event.preventDefault();
  const targetId = els.coinTarget.value;
  const amount = Math.max(1, Math.floor(Number(els.coinAmount.value || 1)));
  const sender = getCurrentUser();
  const receiver = getUser(targetId);

  if (sender.id === receiver.id) {
    toast("Pick another participant");
    return;
  }
  if (sender.auraCoins < amount) {
    toast("Not enough AuraCoins");
    return;
  }

  sender.auraCoins -= amount;
  const auraDeposit = amount * 5000;
  receiver.aura += auraDeposit;
  receiver.votes += amount;
  addEvent("economy", `${sender.name} sent ${amount} AuraCoin(s)`, `${receiver.name} received +${formatNumber(auraDeposit)} Aura after the 50% fee.`);
  state.requests.unshift({
    id: createId("txn"),
    targetId: receiver.id,
    delta: auraDeposit,
    title: "AuraCoin deposit",
    description: `${sender.name} sent ${amount} AuraCoin(s). Receiver credited immediately.`,
    status: "approved",
    author: sender.name,
    createdAt: Date.now(),
    system: true,
  });
  persist();
  eventReset(event.target);
  toast(`${receiver.name} got +${formatNumber(auraDeposit)} Aura`);
  render();
}

function onAdminSubmit(event) {
  event.preventDefault();
  if (state.mode !== "admin") {
    toast("Switch to admin mode first");
    return;
  }

  const targetId = els.adminTarget.value;
  const delta = Number(els.adminDelta.value);
  const title = els.adminTitle.value.trim() || "Admin aura adjustment";
  const description = els.adminDescription.value.trim() || "Manual admin adjustment from AuraX.";
  const photoFile = els.adminPhoto.files?.[0] ?? null;

  const request = {
    id: createId("adm"),
    targetId,
    delta,
    title,
    description,
    status: "approved",
    author: "Admin",
    createdAt: Date.now(),
    approvedBy: "Admin",
    photo: null,
  };

  if (photoFile) {
    readImageAsDataURL(photoFile).then((dataUrl) => {
      request.photo = dataUrl;
      applyRequest(request);
    });
    eventReset(event.target);
    return;
  }

  applyRequest(request);
  eventReset(event.target);
}

function applyRequest(request) {
  const user = getUser(request.targetId);
  user.aura += request.delta;
  user.votes += request.delta > 0 ? Math.ceil(request.delta / 1000) : 0;
  state.events.unshift(
    eventRecord(
      request.delta >= 0 ? "admin-credit" : "admin-deduct",
      `${request.title} for ${user.name}`,
      `${formatDelta(request.delta)} aura applied by ${request.author}. ${request.description}`,
      request.photo ? { photo: request.photo } : undefined,
    ),
  );
  state.requests.unshift({ ...request, status: "approved" });
  persist();
  toast(`${formatDelta(request.delta)} applied to ${user.name}`);
  render();
}

function approveRequest(requestId, accepted) {
  if (state.mode !== "admin") {
    toast("Admin mode required");
    return;
  }

  const request = state.requests.find((item) => item.id === requestId);
  if (!request || request.status !== "pending") return;

  request.status = accepted ? "approved" : "rejected";
  request.approvedBy = "Admin";
  request.reviewedAt = Date.now();

  if (accepted) {
    const user = getUser(request.targetId);
    user.aura += request.delta;
    state.events.unshift(
      eventRecord(
        request.delta >= 0 ? "admin-credit" : "admin-deduct",
        `${request.title} approved`,
        `${formatDelta(request.delta)} applied to ${user.name}. Requested by ${request.author}.`,
      ),
    );
    toast(`Approved for ${user.name}`);
  } else {
    state.events.unshift(eventRecord("admin-reject", `${request.title} rejected`, `No aura changes were applied.`));
    toast("Request rejected");
  }

  persist();
  render();
}

function castVote(targetId, delta, direction) {
  if (targetId === state.currentUserId) {
    toast("You cannot vote on yourself");
    return;
  }
  const target = getUser(targetId);
  target.aura += delta;
  target.votes += direction;
  target.voteAuraReceived = (target.voteAuraReceived ?? 0) + Math.abs(delta);
  addEvent(direction > 0 ? "vote-up" : "vote-down", `${direction > 0 ? "Upvote" : "Downvote"} for ${target.name}`, `${getCurrentUser().name} voted ${direction > 0 ? "+1" : "-1"} on the participant podium.`);
  persist();
  toast(`${direction > 0 ? "+" : ""}${delta} Aura to ${target.name}`);
  render();
}

function resetDemo() {
  localStorage.removeItem(STORAGE_KEY);
  hydrateState();
  addEvent("system", "Demo reset", "AuraX returned to the seeded participant board.");
  persist();
  render();
  toast("Demo reset complete");
}

function render() {
  if (state.mode !== "admin" && state.activeTab === "admin") {
    state.activeTab = "dashboard";
  }
  renderProfile();
  renderModeControls();
  renderTabs();
  renderDropdowns();
  renderLeaderboard();
  renderTargets();
  renderRequests();
  renderFeed();
  renderBadges();
  renderPanelVisibility();
  updateModeUI();
}

function renderProfile() {
  const user = getCurrentUser();
  const profile = computeProfile(user);
  els.balanceValue.textContent = formatNumber(user.aura);
  els.levelValue.textContent = `${profile.level}`;
  els.coinValue.textContent = formatNumber(user.auraCoins);
  els.profileMasteryHint.textContent = profile.mastery;
  els.ringLevel.textContent = profile.level;
  els.ringMastery.textContent = profile.mastery;
  els.masteryRing.dataset.mastery = profile.mastery;
  els.modeLabel.textContent = capitalize(state.mode);
  const auraUntilNextCoin = 10000 - (user.aura % 10000 || 0);
  els.coinProgress.textContent = auraUntilNextCoin === 10000 ? "Ready now" : `${formatNumber(auraUntilNextCoin)} Aura`;
}

function renderModeControls() {
  els.mobileModeBtn.classList.toggle("is-active", state.uiMode === "mobile");
  els.desktopModeBtn.classList.toggle("is-active", state.uiMode === "desktop");
  document.body.dataset.uiMode = state.uiMode;
}

function renderTabs() {
  const tabButtons = els.appTabs.querySelectorAll("[data-tab]");
  tabButtons.forEach((button) => {
    button.classList.toggle("is-active", button.dataset.tab === state.activeTab);
    button.classList.toggle("is-hidden", button.dataset.adminOnly === "true" && state.mode !== "admin");
  });
}

function renderBadges() {
  const user = getCurrentUser();
  const profile = computeProfile(user);
  const auraCoins = user.auraCoins;
  const auraTier = user.aura >= 100000 ? "Elite" : user.aura >= 50000 ? "Ascendant" : "Rising";
  const rank = [...state.users].sort((a, b) => b.aura - a.aura).findIndex((entry) => entry.id === user.id) + 1;
  const badges = [
    { label: profile.mastery, tone: "primary", icon: masteryBadgeIcon(profile.mastery), extraClass: masteryBadgeClass(profile.mastery) },
    { label: `Level ${profile.level}`, tone: "soft", icon: "trending_up", extraClass: "badge-level" },
    { label: `${formatNumber(user.aura)} Aura`, tone: "accent", icon: "auto_awesome", extraClass: "badge-aura" },
    { label: `${formatNumber(auraCoins)} AuraCoins`, tone: "mint", icon: "payments", extraClass: "badge-coins" },
    { label: `Rank #${rank}`, tone: "violet", icon: "leaderboard", extraClass: "badge-rank" },
    { label: auraTier, tone: "gold", icon: "workspace_premium", extraClass: "badge-tier" },
  ];

  const badgeHTML = badges
    .map((badge) => `<span class="badge badge-${badge.tone} badge-badge ${badge.extraClass}"><span class="material-symbols-rounded badge-icon" aria-hidden="true">${badge.icon}</span><span>${escapeHTML(badge.label)}</span></span>`)
    .join("");
  els.profileBadges.innerHTML = badgeHTML;
  els.badgeShelf.innerHTML = badgeHTML;
  els.achievementShelf.innerHTML = renderAchievementCards(user, profile);
}

function renderDropdowns() {
  const options = state.users
    .map((user) => `<option value="${user.id}" ${user.id === state.selectedTargetId ? "selected" : ""}>${user.name} · ${formatNumber(user.aura)} Aura</option>`)
    .join("");
  els.requestTarget.innerHTML = options;
  els.coinTarget.innerHTML = options;
  els.adminTarget.innerHTML = options;

  if (!state.selectedTargetId || !state.users.some((user) => user.id === state.selectedTargetId)) {
    state.selectedTargetId = state.users[0]?.id ?? null;
  }
  els.requestTarget.value = state.selectedTargetId ?? state.users[0]?.id;
  els.coinTarget.value = state.selectedTargetId ?? state.users[1]?.id ?? state.users[0]?.id;
  els.adminTarget.value = state.selectedTargetId ?? state.users[0]?.id;
}

function renderLeaderboard() {
  const users = [...state.users].sort((a, b) => b.aura - a.aura);
  const topAura = users[0]?.aura ?? 1;
  const template = els.leaderboardItemTemplate;
  els.leaderboard.innerHTML = "";

  users.forEach((user, index) => {
    const node = template.content.firstElementChild.cloneNode(true);
    node.querySelector(".leader-rank").textContent = index + 1;
    node.querySelector(".leader-name").textContent = user.name;
    node.querySelector(".leader-tag").textContent = `${computeProfile(user).mastery} · ${formatNumber(user.auraCoins)} coins`;
    node.querySelector(".leader-meta").textContent = `${formatNumber(user.aura)} Aura · ${formatNumber(user.votes)} votes`;
    node.querySelector(".leader-bar span").style.width = `${Math.max(8, (user.aura / topAura) * 100)}%`;
    if (user.id === state.currentUserId) {
      node.style.borderColor = "rgba(120, 255, 214, 0.35)";
    }
    const barLabel = node.querySelector(".leader-meta");
    barLabel.style.display = "flex";
    els.leaderboard.appendChild(node);
  });
}

function renderTargets() {
  const others = state.users.filter((user) => user.id !== state.currentUserId);
  els.voteTargets.innerHTML = others
    .map((user) => {
      const profile = computeProfile(user);
      return `
        <div class="vote-card">
          <div class="vote-head">
            <div>
              <strong>${escapeHTML(user.name)}</strong>
              <span class="stat-label">${profile.mastery} · Level ${profile.level}</span>
            </div>
            <strong>${formatNumber(user.aura)} Aura</strong>
          </div>
          <div class="vote-actions">
            <button class="mini-button up" data-action="vote-up" data-id="${user.id}" type="button">Upvote +100</button>
            <button class="mini-button down" data-action="vote-down" data-id="${user.id}" type="button">Downvote -100</button>
            <button class="mini-button" data-action="select-target" data-id="${user.id}" type="button">Use in forms</button>
          </div>
        </div>
      `;
    })
    .join("");
}

function renderRequests() {
  const visible = state.requests.slice(0, 12);
  els.requestInbox.innerHTML = visible
    .map((request) => {
      const user = getUser(request.targetId);
      const statusClass = request.status === "approved" ? "accept" : request.status === "rejected" ? "reject" : "";
      return `
        <div class="request-card">
          <div class="request-head">
            <div>
              <strong>${escapeHTML(request.title)}</strong>
              <span class="stat-label">${escapeHTML(request.author)} · ${request.status}</span>
            </div>
            <strong class="${statusClass}">${formatDelta(request.delta)}</strong>
          </div>
          <p class="event-desc">${escapeHTML(request.description)}</p>
          <div class="event-desc" style="margin-top: 8px;">Target: ${escapeHTML(user.name)}</div>
          ${request.photo ? `<img class="preview" src="${request.photo}" alt="Request photo" />` : ""}
          <div class="request-actions${request.status !== 'pending' ? ' hidden' : ''}">
            <button class="mini-button accept" type="button" data-action="approve-request" data-id="${request.id}">Approve</button>
            <button class="mini-button reject" type="button" data-action="reject-request" data-id="${request.id}">Reject</button>
          </div>
        </div>
      `;
    })
    .join("");
}

function renderFeed() {
  els.activityFeed.innerHTML = state.events
    .slice(0, EVENT_LIMIT)
    .map((event) => {
      return `
        <article class="event">
          <div class="event-top">
            <strong class="event-title">${escapeHTML(event.title)}</strong>
            <span class="event-time">${formatTime(event.createdAt)}</span>
          </div>
          <div class="event-desc">${escapeHTML(event.description)}</div>
          ${event.meta?.photo ? `<img class="preview" src="${event.meta.photo}" alt="Event photo" />` : ""}
        </article>
      `;
    })
    .join("");
}

function renderAchievementCards(user, profile) {
  const achievements = getAchievementStates(user, profile);
  return achievements
    .map((achievement) => {
      const icon = achievement.unlocked ? achievement.icon : "lock";
      const iconClass = achievement.unlocked ? "achievement-icon" : "achievement-icon achievement-locked";
      const stateLabel = achievement.unlocked ? (achievement.unlockText ?? "Unlocked") : achievement.progress;
      return `
        <article class="achievement-card ${achievement.unlocked ? "is-unlocked" : "is-locked"}">
          <span class="material-symbols-rounded ${iconClass}" aria-hidden="true">${icon}</span>
          <div class="achievement-meta">
            <div class="achievement-topline">
              <strong>${escapeHTML(achievement.title)}</strong>
              <span class="achievement-state ${achievement.unlocked ? "is-unlocked" : "is-locked"}">${achievement.unlocked ? "Unlocked" : "Locked"}</span>
            </div>
            <span>${escapeHTML(achievement.description)}</span>
            <span>${escapeHTML(stateLabel)}</span>
          </div>
        </article>
      `;
    })
    .join("");
}

function getAchievementStates(user, profile) {
  const voteAuraReceived = user.voteAuraReceived ?? 0;
  const rank = [...state.users].sort((a, b) => b.aura - a.aura).findIndex((entry) => entry.id === user.id) + 1;
  const achievementDefs = [
    {
      title: "Aura Boss",
      description: "Reach 100,000 Aura and take the throne.",
      icon: "workspace_premium",
      unlocked: user.aura >= 100000,
      unlockText: "Crowned at 100k",
      progress: `${formatNumber(Math.max(0, 100000 - user.aura))} Aura to go`,
    },
    {
      title: "Who's on ballot?",
      description: "Receive at least 10k in aura votations.",
      icon: "how_to_vote",
      unlocked: voteAuraReceived >= 10000,
      unlockText: "Ballot aura recorded",
      progress: `${formatNumber(Math.max(0, 10000 - voteAuraReceived))} vote Aura to go`,
    },
    {
      title: "Podium Regular",
      description: "Break into the top 3 on the leaderboard.",
      icon: "emoji_events",
      unlocked: rank <= 3,
      unlockText: "Podium presence locked",
      progress: rank <= 3 ? "On podium" : `Need top ${Math.max(1, rank - 2)} climb`,
    },
    {
      title: "Aura Collector",
      description: "Stack 10 AuraCoins from your Aura balance.",
      icon: "savings",
      unlocked: user.auraCoins >= 10,
      unlockText: "Coin stack assembled",
      progress: `${Math.max(0, 10 - user.auraCoins)} AuraCoin(s) to go`,
    },
    {
      title: "Mastery Sprint",
      description: "Hit Mastery V or higher.",
      icon: "local_fire_department",
      unlocked: profile.masteryIndex >= 4,
      unlockText: "Mastery tempo activated",
      progress: profile.masteryIndex >= 4 ? "Mastery streak active" : `Reach ${MASTERIES[4]}`,
    },
    {
      title: "Holo Apex",
      description: "Ascend to Mastery X and unlock the holographic frame.",
      icon: "diamond",
      unlocked: profile.mastery === "Mastery X",
      unlockText: "Holographic aura online",
      progress: profile.mastery === "Mastery X" ? "Holographic aura active" : `Reach ${MASTERIES[9]}`,
    },
  ];

  return achievementDefs;
}

function masteryBadgeClass(mastery) {
  return `badge-mastery-${mastery.split(" ")[1].toLowerCase()}`;
}

function masteryBadgeIcon(mastery) {
  const map = {
    "Mastery I": "looks_one",
    "Mastery II": "looks_two",
    "Mastery III": "looks_3",
    "Mastery IV": "looks_4",
    "Mastery V": "looks_5",
    "Mastery VI": "hexagon",
    "Mastery VII": "flare",
    "Mastery VIII": "electric_bolt",
    "Mastery IX": "rocket",
    "Mastery X": "diamond",
  };

  return map[mastery] ?? "shield";
}

function renderPanelVisibility() {
  const panels = document.querySelectorAll("[data-panel]");
  panels.forEach((panel) => {
    const isActive = panel.dataset.panel === state.activeTab;
    panel.classList.toggle("is-hidden", state.uiMode === "mobile" && !isActive);
    panel.classList.toggle("admin-locked", panel.dataset.panel === "admin" && state.mode !== "admin");
  });
  document.body.classList.toggle("is-desktop", state.uiMode === "desktop");
  document.body.classList.toggle("is-mobile", state.uiMode === "mobile");
  document.body.dataset.adminVisible = String(state.mode === "admin");
}

function updateModeUI() {
  const adminVisible = state.mode === "admin";
  els.adminForm.closest("article").style.opacity = adminVisible ? "1" : "0.65";
  els.adminForm.closest("article").style.filter = adminVisible ? "none" : "saturate(0.85)";
}

function setUiMode(mode) {
  state.uiMode = mode;
  persist();
  render();
}

function setActiveTab(tab) {
  if (tab === "admin" && state.mode !== "admin") {
    return;
  }
  state.activeTab = tab;
  persist();
  render();
}

function getCurrentUser() {
  return getUser(state.currentUserId);
}

function getUser(id) {
  const user = state.users.find((item) => item.id === id);
  if (!user) {
    throw new Error(`Unknown user: ${id}`);
  }
  return user;
}

function ensureCurrentTarget() {
  if (!state.users.find((user) => user.id === state.currentUserId)) {
    state.currentUserId = state.users[0]?.id ?? "u1";
  }
  if (!state.selectedTargetId) {
    state.selectedTargetId = state.users[1]?.id ?? state.users[0]?.id ?? null;
  }
}

function computeProfile(user) {
  const rawLevel = Math.floor(user.aura / 1000);
  const masteryIndex = Math.min(9, Math.floor(rawLevel / 100));
  const level = rawLevel % 100;
  return {
    level,
    masteryIndex,
    mastery: MASTERIES[masteryIndex],
  };
}

function addEvent(kind, title, description, meta) {
  state.events.unshift(eventRecord(kind, title, description, meta));
}

function eventRecord(kind, title, description, meta = null) {
  return {
    id: createId("evt"),
    kind,
    title,
    description,
    createdAt: Date.now() + (txSeed % 1000),
    meta,
  };
}

function persist(write = true) {
  if (write) {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify(serializeState()),
    );
  }

  if (firebaseState.ready && !firebaseState.syncing && !firebaseState.disabled) {
    void queueFirebaseWrite();
  }
}

function serializeState() {
  return {
    users: state.users,
    requests: state.requests,
    events: state.events,
    currentUserId: state.currentUserId,
    mode: state.mode,
    uiMode: state.uiMode,
    activeTab: state.activeTab,
    location: state.location,
    selectedTargetId: state.selectedTargetId,
  };
}

async function queueFirebaseWrite() {
  if (!firebaseState.ready || !firebaseState.db || firebaseState.syncing || firebaseState.disabled || !firebaseState.mods) {
    return;
  }

  try {
    const { doc, setDoc, serverTimestamp } = firebaseState.mods;
    const stateRef = doc(firebaseState.db, FIREBASE_DOC_PATH[0], FIREBASE_DOC_PATH[1]);
    firebaseState.remoteVersion += 1;
    await setDoc(stateRef, {
      version: firebaseState.remoteVersion,
      updatedAt: serverTimestamp(),
      state: serializeState(),
    });
  } catch (error) {
    console.warn("Firebase write failed", error);
  }
}

function readJSON(key) {
  try {
    return JSON.parse(localStorage.getItem(key) || "null");
  } catch {
    return null;
  }
}

function formatNumber(value) {
  return new Intl.NumberFormat("en-US").format(Math.round(value));
}

function formatDelta(value) {
  return `${value >= 0 ? "+" : ""}${formatNumber(value)}`;
}

function formatTime(value) {
  return new Intl.DateTimeFormat("en-US", {
    hour: "numeric",
    minute: "2-digit",
    month: "short",
    day: "numeric",
  }).format(new Date(value));
}

function clampNumber(value, min, max, fallback) {
  const parsed = Number(value);
  if (Number.isNaN(parsed)) return fallback;
  return String(Math.max(min, Math.min(max, parsed)));
}

function capitalize(value) {
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function eventReset(form) {
  form.reset();
  els.requestDelta.value = 1000;
  els.coinAmount.value = 1;
  els.adminDelta.value = 500;
}

function createId(prefix) {
  return `${prefix}_${Math.random().toString(36).slice(2, 10)}_${Date.now().toString(36)}`;
}

function toast(message) {
  const existing = document.querySelector(".toast");
  if (existing) existing.remove();
  const node = document.createElement("div");
  node.className = "toast";
  node.textContent = message;
  document.body.appendChild(node);
  clearTimeout(toastTimers.get("main"));
  toastTimers.set(
    "main",
    setTimeout(() => {
      node.remove();
    }, 2200),
  );
}

function escapeHTML(input) {
  return String(input)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function readImageAsDataURL(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(file);
  });
}
