(() => {
  "use strict";
  const AVATARS = ["😀","😎","🤓","🦊","🐼","🐯","🐸","🦁","🐵","🐧","🦄","🚀","🌟","🔥","🍀","🎯"];
  const REACTIONS = ["❤️","😂","🔥","👏","💡","🎯"];
  const STORAGE_KEYS = { backendUrl:"ewc_render_backend", playerId:"ewc_render_player", nick:"ewc_render_nick", avatar:"ewc_render_avatar", theme:"ewc_theme", sound:"ewc_sound" };
  const $ = (id) => document.getElementById(id);

  const state = {
    backendUrl: "",
    socket: null,
    connected: false,
    playerId: localStorage.getItem(STORAGE_KEYS.playerId) || randomId("P"),
    nickname: localStorage.getItem(STORAGE_KEYS.nick) || "",
    avatar: localStorage.getItem(STORAGE_KEYS.avatar) || AVATARS[0],
    roomCode: "",
    roomPassword: "",
    room: null,
    players: [],
    chain: [],
    chat: [],
    dictSet: new Set(),
    dictLoaded: false,
    lastVersion: 0,
    countdownTimer: null,
    lastEvent: "",
    soundOn: localStorage.getItem(STORAGE_KEYS.sound) !== "off",
    pending: false,
    rooms: []
  };

  init();

  async function init() {
    initConfig();
    initTheme();
    fillAvatars();
    wireEvents();
    buildReactions();
    hydrateFromUrl();
    await loadDictionary();
    if (state.backendUrl) connectSocket(); else openConfigModal("Anh cần dán URL Render backend trước khi chơi.");
  }

  function initConfig() {
    const embedded = (window.EWC_CONFIG?.BACKEND_URL || "").trim();
    const saved = localStorage.getItem(STORAGE_KEYS.backendUrl) || "";
    state.backendUrl = normalizeBackendUrl(saved || embedded || "");
    $("backendUrlInput").value = state.backendUrl;
    $("soundToggle").textContent = state.soundOn ? "🔊" : "🔇";
  }

  function initTheme() {
    const saved = localStorage.getItem(STORAGE_KEYS.theme) || "light";
    document.documentElement.dataset.theme = saved;
    $("themeToggle").textContent = saved === "dark" ? "☀️" : "🌙";
  }

  function fillAvatars() {
    [$("hostAvatar"), $("joinAvatar")].forEach(select => {
      select.innerHTML = AVATARS.map(a => `<option value="${a}">${a}</option>`).join("");
      select.value = state.avatar;
    });
    $("hostNick").value = state.nickname;
    $("joinNick").value = state.nickname;
  }

  function hydrateFromUrl() {
    const params = new URLSearchParams(location.search);
    const room = sanitizeRoom(params.get("room") || "");
    if (room) $("joinRoomCode").value = room;
    const server = params.get("server");
    if (server && !state.backendUrl) {
      state.backendUrl = normalizeBackendUrl(server);
      localStorage.setItem(STORAGE_KEYS.backendUrl, state.backendUrl);
      $("backendUrlInput").value = state.backendUrl;
    }
  }

  async function loadDictionary() {
    try {
      const url = window.EWC_CONFIG?.DICTIONARY_URL || "data/valid_words.json";
      const res = await fetch(`${url}?v=${Date.now()}`, { cache: "no-store" });
      const data = await res.json();
      const add = (w) => { w = normalizeWord(w); if (w.length >= 3) state.dictSet.add(w); };
      if (Array.isArray(data.words)) data.words.forEach(add);
      if (Array.isArray(data.customWords)) data.customWords.forEach(add);
      if (data.wordsByLetter) Object.values(data.wordsByLetter).forEach(list => Array.isArray(list) && list.forEach(add));
      state.dictLoaded = true;
      $("dictStatus").textContent = `đã tải ${state.dictSet.size.toLocaleString("vi-VN")} từ`;
    } catch (err) {
      console.warn(err);
      $("dictStatus").textContent = "chưa tải được JSON, server vẫn kiểm tra được";
    }
  }

  function wireEvents() {
    $("configBtn").addEventListener("click", () => openConfigModal());
    $("closeConfigBtn").addEventListener("click", closeConfigModal);
    $("saveConfigBtn").addEventListener("click", saveConfig);
    $("themeToggle").addEventListener("click", toggleTheme);
    $("soundToggle").addEventListener("click", toggleSound);
    $("addWordBtn").addEventListener("click", () => $("addWordModal").classList.remove("hidden"));
    $("closeAddWordBtn").addEventListener("click", () => $("addWordModal").classList.add("hidden"));
    $("saveWordBtn").addEventListener("click", addCustomWord);
    $("infiniteMode").addEventListener("change", () => $("totalRounds").disabled = $("infiniteMode").checked);
    $("createForm").addEventListener("submit", onCreateRoom);
    $("joinForm").addEventListener("submit", onJoinRoom);
    $("refreshRoomsBtn").addEventListener("click", requestRooms);
    $("leaveBtn").addEventListener("click", leaveRoom);
    $("copyLinkBtn").addEventListener("click", copyInviteLink);
    $("startGameBtn").addEventListener("click", startGame);
    $("wordForm").addEventListener("submit", submitWord);
    $("overlayWordForm").addEventListener("submit", submitOverlayWord);
    $("passTurnBtn").addEventListener("click", passTurn);
    $("overlayPassBtn").addEventListener("click", passTurn);
    $("chatForm").addEventListener("submit", sendChat);
    document.querySelectorAll(".team-btn").forEach(btn => btn.addEventListener("click", () => setTeam(btn.dataset.team)));
    window.addEventListener("beforeunload", () => { try { if (state.roomCode) emit("room:leave", { roomCode: state.roomCode, playerId: state.playerId }, 300); } catch(_){} });
  }

  function connectSocket() {
    if (!window.io) return toast("Không tải được thư viện Socket.IO. Kiểm tra internet hoặc CDN.", "error");
    if (state.socket) state.socket.disconnect();
    setConnection(false, "Đang kết nối Render...");
    state.socket = io(state.backendUrl, { transports:["websocket","polling"], reconnection:true, reconnectionAttempts:Infinity, reconnectionDelay:600, timeout:12000 });
    const s = state.socket;
    s.on("connect", () => {
      setConnection(true, "Đã kết nối Render");
      requestRooms();
      if (state.roomCode && state.nickname) rejoinCurrentRoom();
    });
    s.on("disconnect", () => setConnection(false, "Mất kết nối, đang tự nối lại..."));
    s.on("connect_error", (err) => { console.warn(err); setConnection(false, "Chưa kết nối được Render"); });
    s.on("rooms:update", data => renderOpenRooms(data.rooms || []));
    s.on("room:state", snapshot => renderState(snapshot));
    s.on("chat:update", data => { state.chat = data.chat || []; renderChat(); });
    s.on("reaction:new", data => spawnReaction(data.emoji || "👏"));
    s.on("dictionary:customWord", data => { if (data?.word) state.dictSet.add(data.word); $("dictStatus").textContent = `đã tải ${state.dictSet.size.toLocaleString("vi-VN")} từ`; });
  }

  function setConnection(ok, text) {
    state.connected = ok;
    const badge = $("connectionBadge");
    badge.textContent = text;
    badge.className = `pill ${ok ? "connection-ok" : "danger"}`;
  }

  async function onCreateRoom(event) {
    event.preventDefault();
    if (!ensureSocket()) return;
    const nickname = sanitizeName($("hostNick").value);
    const avatar = $("hostAvatar").value || AVATARS[0];
    saveIdentity(nickname, avatar);
    const payload = {
      playerId: state.playerId, nickname, avatar,
      roomName: sanitizeTopic($("roomName").value || `${nickname} - Nối từ`), roomPassword: $("roomPassword").value.trim(),
      maxPlayers: clamp($("maxPlayers").value, 2, 12, 8), botCount: clamp($("botCount").value, 0, 3, 0),
      turnSeconds: clamp($("turnSeconds").value, 2, 200, 15), totalRounds: clamp($("totalRounds").value, 1, 99, 4),
      roundMode: $("infiniteMode").checked ? "infinite" : "finite", topic: $("topicSelect").value,
      chainRule: $("chainRule").value, mode: $("gameMode").value
    };
    const res = await emit("room:create", payload);
    state.roomPassword = payload.roomPassword;
    enterRoom(res.roomCode, res.state);
    toast(`Đã tạo phòng ${res.roomCode}.`, "success");
  }

  async function onJoinRoom(event) {
    event.preventDefault();
    if (!ensureSocket()) return;
    const roomCode = sanitizeRoom($("joinRoomCode").value);
    const nickname = sanitizeName($("joinNick").value);
    const avatar = $("joinAvatar").value || AVATARS[0];
    saveIdentity(nickname, avatar);
    state.roomPassword = $("joinRoomPassword").value.trim();
    const res = await emit("room:join", { roomCode, roomPassword: state.roomPassword, playerId: state.playerId, nickname, avatar });
    enterRoom(roomCode, res.state);
    toast(`Đã vào phòng ${roomCode}.`, "success");
  }

  async function rejoinCurrentRoom() {
    try {
      const res = await emit("room:join", { roomCode: state.roomCode, roomPassword: state.roomPassword, playerId: state.playerId, nickname: state.nickname, avatar: state.avatar }, 6000, true);
      if (res?.state) renderState(res.state);
    } catch (err) { console.warn("rejoin failed", err); }
  }

  function enterRoom(roomCode, snapshot) {
    state.roomCode = roomCode;
    $("lobbyScreen").classList.add("hidden");
    $("roomScreen").classList.remove("hidden");
    $("roomCodeText").textContent = roomCode;
    updateUrlRoom(roomCode);
    renderState(snapshot);
  }

  async function leaveRoom() {
    try { if (state.roomCode) await emit("room:leave", { roomCode: state.roomCode, playerId: state.playerId }, 1200, true); } catch(_) {}
    state.roomCode = ""; state.room = null; state.chain = []; state.chat = [];
    stopCountdown();
    $("roomScreen").classList.add("hidden"); $("lobbyScreen").classList.remove("hidden");
    if (location.protocol !== "file:") { const url = new URL(location.href); url.searchParams.delete("room"); history.replaceState({}, "", url); }
    requestRooms();
  }

  async function startGame() { const res = await emit("room:start", { roomCode: state.roomCode, playerId: state.playerId }); renderState(res.state); }
  async function setTeam(team) { const res = await emit("room:setTeam", { roomCode: state.roomCode, playerId: state.playerId, team }); renderState(res.state); }

  async function submitWord(event) { event.preventDefault(); await submitWordFromInput($("wordInput")); }
  async function submitOverlayWord(event) { event.preventDefault(); await submitWordFromInput($("overlayWordInput")); }

  async function submitWordFromInput(input) {
    const word = normalizeWord(input.value);
    if (!word) return;
    if (!isMyTurn()) return toast("Chưa đến lượt bạn.", "error");
    const req = requiredLetter();
    if (req && word[0] !== req) return showBigEvent(`Từ phải bắt đầu bằng chữ ${req.toUpperCase()}.`, "error");
    if (state.dictLoaded && !state.dictSet.has(word)) return showBigEvent(`Từ “${word}” chưa có trong JSON. Có thể bấm Thêm từ nếu đúng.`, "error");
    setWordControls(true);
    showBigEvent("Đang gửi từ lên Render...", "info");
    try {
      const res = await emit("word:submit", { roomCode: state.roomCode, playerId: state.playerId, word }, 10000);
      renderState(res.state);
      input.value = ""; $("wordInput").value = ""; $("overlayWordInput").value = "";
      showBigEvent(res.message, res.accepted ? "success" : "error");
      playTone(res.accepted ? "success" : "error");
    } finally { setWordControls(false); }
  }

  async function passTurn() {
    if (!isMyTurn()) return toast("Chưa đến lượt bạn.", "error");
    setWordControls(true);
    try {
      const res = await emit("turn:pass", { roomCode: state.roomCode, playerId: state.playerId });
      renderState(res.state); showBigEvent(res.message, "error"); playTone("error");
    } finally { setWordControls(false); }
  }

  async function sendChat(event) {
    event.preventDefault();
    const input = $("chatInput");
    const message = input.value.trim();
    if (!message) return;
    input.value = "";
    try { await emit("chat:send", { roomCode: state.roomCode, playerId: state.playerId, message }, 5000, true); } catch (err) { toast(err.message, "error"); }
  }

  async function addCustomWord() {
    if (!ensureSocket()) return;
    const word = normalizeWord($("newWordInput").value);
    const topic = sanitizeTopic($("newWordTopic").value || "Custom");
    const level = $("newWordLevel").value;
    const password = $("adminPasswordInput").value;
    if (!word || word.length < 3) return toast("Từ cần tối thiểu 3 chữ cái.", "error");
    try {
      const res = await emit("word:addCustom", { word, topic, level, password }, 12000);
      state.dictSet.add(res.word);
      $("newWordInput").value = ""; $("adminPasswordInput").value = "";
      $("addWordModal").classList.add("hidden");
      toast(res.persisted ? `Đã thêm và lưu JSON: ${res.word}` : `Đã thêm tạm: ${res.word}. ${res.note || ""}`, res.persisted ? "success" : "info", 5200);
      $("dictStatus").textContent = `đã tải ${state.dictSet.size.toLocaleString("vi-VN")} từ`;
    } catch (err) { toast(err.message, "error", 5200); }
  }

  function requestRooms() { if (state.socket?.connected) emit("rooms:list", {}, 3000, true).then(res => renderOpenRooms(res.rooms || [])).catch(()=>{}); }

  function renderOpenRooms(rooms) {
    state.rooms = rooms;
    const box = $("openRoomsList");
    if (!rooms.length) { box.innerHTML = `<div class="empty-rooms">Chưa có phòng nào đang mở.</div>`; return; }
    box.innerHTML = rooms.map(r => {
      const canJoin = r.status === "lobby" && Number(r.activeHumans || 0) < Number(r.maxPlayers || 12);
      return `<button class="room-card-mini ${canJoin ? "" : "disabled"}" type="button" data-room="${escapeAttr(r.roomCode)}" ${canJoin ? "" : "disabled"}>
        <div class="room-card-top"><strong>${escapeHtml(r.roomName || "Phòng nối từ")}</strong><span>${r.passwordProtected ? "🔒" : "🔓"}</span></div>
        <div class="room-card-code">${escapeHtml(r.roomCode)} · ${r.status === "lobby" ? "Đang chờ" : r.status === "playing" ? "Đang chơi" : "Kết thúc"}</div>
        <div class="room-card-meta">${Number(r.connectedHumans || 0)} online · ${Number(r.activeHumans || 0)}/${Number(r.maxPlayers || 12)} người · ${Number(r.botCount || 0)} bot</div>
        <div class="room-card-meta">${escapeHtml(r.mode === "team" ? "Team Battle" : "Đấu cá nhân")} · ${Number(r.totalRounds || 0)} vòng · ${Number(r.turnSeconds || 30)} giây/lượt</div>
      </button>`;
    }).join("");
    box.querySelectorAll("button[data-room]").forEach(btn => btn.addEventListener("click", () => { $("joinRoomCode").value = btn.dataset.room; $("joinNick").focus(); }));
  }

  function renderState(snapshot) {
    if (!snapshot) return;
    state.room = snapshot.room;
    state.players = snapshot.players || [];
    state.chain = snapshot.chain || [];
    state.chat = snapshot.chat || [];
    state.lastVersion = Number(state.room.version || 0);
    renderHeader(); renderPreGame(); renderTurn(); renderTurnOverlay(); renderChain(); renderPlayers(); renderChat(); reactToEvent();
  }

  function renderHeader() {
    const r = state.room;
    $("roomCodeText").textContent = r.roomCode;
    const activeHumans = state.players.filter(p => p.type !== "bot" && p.status === "active").length;
    const connectedHumans = state.players.filter(p => p.type !== "bot" && p.connected && p.status === "active").length;
    $("playersCountText").textContent = `${connectedHumans} online · ${activeHumans}/${r.maxPlayers} người`;
    $("roomModeBadge").textContent = r.mode === "team" ? "Team Battle" : "Đấu cá nhân";
  }

  function renderPreGame() {
    const r = state.room, me = getMe(), isLobby = r.status === "lobby";
    $("preGamePanel").classList.toggle("hidden", !isLobby);
    $("startGameBtn").classList.toggle("hidden", !(isLobby && r.hostId === state.playerId));
    $("teamChooser").classList.toggle("hidden", !(isLobby && r.mode === "team"));
    document.querySelectorAll(".team-btn").forEach(btn => btn.classList.toggle("active", me?.team === btn.dataset.team));
    $("preGameHint").textContent = r.hostId === state.playerId ? "Bạn là chủ phòng. Chờ mọi máy hiện đủ danh sách rồi bấm bắt đầu." : "Chờ chủ phòng bắt đầu.";
  }

  function renderTurn() {
    const r = state.room;
    const current = state.players.find(p => p.playerId === r.currentTurnPlayerId);
    $("turnOwnerText").textContent = r.status === "playing" ? (r.mode === "team" ? `Đội ${r.currentTeam}` : current ? `${current.avatar} ${current.nickname}` : "---") : r.status === "ended" ? "Trận đã kết thúc" : "Phòng chờ";
    $("lastWordText").textContent = r.currentWord || "---";
    $("chainRuleText").textContent = r.chainRule === "first-letter" ? "Nối bằng chữ cái đầu của từ trước" : "Nối bằng chữ cái cuối của từ trước";
    $("roundText").textContent = r.roundMode === "infinite" ? `Vòng ${r.currentRound || 0}` : `Vòng ${r.currentRound || 0}/${r.totalRounds} · lượt ${Math.min((r.turnsCompletedInRound || 0) + 1, r.roundActorCount || 1)}/${r.roundActorCount || 1}`;
    const canPlay = isMyTurn();
    $("wordInput").disabled = !canPlay; $("submitWordBtn").disabled = !canPlay; $("passTurnBtn").disabled = !canPlay;
    $("wordInput").placeholder = canPlay ? "Đến lượt bạn, nhập từ..." : "Chưa đến lượt bạn...";
    $("turnMessage").textContent = getTurnMessage(canPlay);
    startCountdown();
  }

  function getTurnMessage(canPlay) {
    const r = state.room;
    if (r.status === "lobby") return "Trò chơi sẽ bắt đầu khi chủ phòng bấm Bắt đầu.";
    if (r.status === "ended") return r.lastEvent || "Trận đã kết thúc.";
    const req = requiredLetter();
    if (canPlay) return `Bạn cần nhập từ bắt đầu bằng chữ “${req.toUpperCase()}”.`;
    return r.mode === "team" ? `Chờ đội ${r.currentTeam} nhập từ bắt đầu bằng chữ “${req.toUpperCase()}”.` : `Chờ người chơi hiện tại nhập từ bắt đầu bằng chữ “${req.toUpperCase()}”.`;
  }

  function renderTurnOverlay() {
    const overlay = $("turnOverlay");
    const r = state.room;
    const can = isMyTurn();
    if (!r || r.status !== "playing" || !can) { overlay.classList.add("hidden"); return; }
    const wasHidden = overlay.classList.contains("hidden");
    overlay.classList.remove("hidden");
    $("overlayCurrentWord").textContent = r.currentWord || "---";
    $("overlayTurnTitle").textContent = r.mode === "team" ? `Đội ${getMe()?.team} đến lượt` : "Đến lượt bạn";
    $("overlayRuleText").textContent = `Nhập từ bắt đầu bằng chữ “${requiredLetter().toUpperCase()}”.`;
    if (wasHidden) { playTone("turn"); setTimeout(() => $("overlayWordInput").focus(), 80); }
  }

  function renderChain() {
    const box = $("chainWords");
    box.innerHTML = state.chain.filter(w => w.valid).map((w, idx) => `<div class="chain-word" title="${escapeAttr(w.nickname || "")}">${idx > 0 ? `<span class="arrow">→</span>` : ""}<span class="word-text">${escapeHtml(w.word)}</span></div>`).join("");
    box.scrollLeft = box.scrollWidth;
  }

  function renderPlayers() {
    const r = state.room;
    const sorted = [...state.players].sort((a,b)=>Number(b.score||0)-Number(a.score||0)||Number(a.orderIndex||0)-Number(b.orderIndex||0));
    $("playersList").innerHTML = sorted.map(p => {
      const current = r.mode === "team" ? (p.team === r.currentTeam && r.status === "playing") : (p.playerId === r.currentTurnPlayerId);
      const status = p.type === "bot" ? "Bot" : p.connected ? "Online" : p.status === "offline" ? "Offline" : p.status;
      const badges = [status, r.mode === "team" ? `Đội ${p.team || "-"}` : "", `${Number(p.validCount || 0)} từ đúng`, `${Number(p.penalty || 0)} phạt`].filter(Boolean).join(" · ");
      return `<div class="player-row ${current ? "current" : ""} ${p.status || ""}"><div class="avatar">${escapeHtml(p.avatar || "🙂")}</div><div class="player-main"><div class="player-name">${escapeHtml(p.nickname || "Người chơi")}</div><div class="player-sub">${escapeHtml(badges)}</div></div><div class="score-pill">${Number(p.score || 0)}</div></div>`;
    }).join("");
    const teamBox = $("teamScoreBox");
    if (r.mode === "team") {
      const a = state.players.filter(p=>p.team==="A").reduce((s,p)=>s+Number(p.score||0),0);
      const b = state.players.filter(p=>p.team==="B").reduce((s,p)=>s+Number(p.score||0),0);
      teamBox.classList.remove("hidden"); teamBox.innerHTML = `<div class="team-score">Đội A<strong>${a}</strong></div><div class="team-score">Đội B<strong>${b}</strong></div>`;
    } else teamBox.classList.add("hidden");
  }

  function renderChat() {
    const list = $("chatList");
    list.innerHTML = (state.chat || []).slice(-120).map(c => `<div class="chat-item"><div class="chat-meta">${escapeHtml(c.avatar || "🙂")} ${escapeHtml(c.nickname || "")}</div><div>${escapeHtml(c.message || "")}</div></div>`).join("");
    list.scrollTop = list.scrollHeight;
  }

  function startCountdown() { stopCountdown(); updateCountdown(); state.countdownTimer = setInterval(updateCountdown, 200); }
  function stopCountdown() { if (state.countdownTimer) clearInterval(state.countdownTimer); state.countdownTimer = null; }
  function updateCountdown() {
    const r = state.room;
    if (!r || r.status !== "playing") return setTimerDisplay("--", 0);
    const turnMs = Math.max(1, Number(r.turnSeconds || 30) * 1000);
    const leftMs = Math.max(0, Number(r.turnDeadlineAt || 0) - Date.now());
    setTimerDisplay(String(Math.ceil(leftMs / 1000)), Math.max(0, Math.min(100, leftMs / turnMs * 100)));
  }
  function setTimerDisplay(text, pct) { $("timerNumber").textContent = text; $("timerBar").style.width = `${pct}%`; $("overlayTimerNumber").textContent = text; $("overlayTimerBar").style.width = `${pct}%`; }

  function reactToEvent() {
    const msg = state.room?.lastEvent || "";
    if (!msg || msg === state.lastEvent) return;
    state.lastEvent = msg;
    const type = /sai|trừ|hết giờ|bỏ qua/i.test(msg) ? "error" : /đúng|thắng|bắt đầu/i.test(msg) ? "success" : "info";
    showBigEvent(msg, type);
  }
  function showBigEvent(msg, type="info") {
    const a = $("eventBanner"), b = $("overlayEvent");
    [a,b].forEach(el => { el.textContent = msg; el.className = `${el.id === "eventBanner" ? "event-banner" : "overlay-event"} ${type}`; el.classList.remove("hidden"); });
  }

  function buildReactions() { $("reactionBar").innerHTML = REACTIONS.map(e => `<button class="reaction-btn" type="button" data-emoji="${e}">${e}</button>`).join(""); $("reactionBar").querySelectorAll("button").forEach(btn => btn.addEventListener("click", () => emit("reaction:send", { roomCode: state.roomCode, playerId: state.playerId, emoji: btn.dataset.emoji }, 2000, true).catch(()=>{}))); }
  function spawnReaction(emoji) { const host = $("floatingHost"), el = document.createElement("div"); el.className = "float-reaction"; el.textContent = emoji; el.style.left = `${12 + Math.random()*76}%`; el.style.bottom = `${10 + Math.random()*28}%`; host.appendChild(el); setTimeout(()=>el.remove(), 1900); }

  function isMyTurn() { const r = state.room, me = getMe(); if (!r || !me || r.status !== "playing" || me.status !== "active") return false; return r.mode === "team" ? me.team === r.currentTeam : r.currentTurnPlayerId === state.playerId; }
  function getMe() { return state.players.find(p => p.playerId === state.playerId); }
  function requiredLetter() { const w = normalizeWord(state.room?.currentWord || ""); if (!w) return ""; return state.room.chainRule === "first-letter" ? w[0] : w[w.length - 1]; }

  function saveIdentity(nickname, avatar) { state.nickname = nickname; state.avatar = avatar; localStorage.setItem(STORAGE_KEYS.playerId, state.playerId); localStorage.setItem(STORAGE_KEYS.nick, nickname); localStorage.setItem(STORAGE_KEYS.avatar, avatar); }
  function emit(event, payload={}, timeout=8000, silent=false) { return new Promise((resolve, reject) => { if (!state.socket?.connected) { const e = new Error("Chưa kết nối Render backend."); if (!silent) toast(e.message, "error"); return reject(e); } const timer = setTimeout(()=>reject(new Error("Server phản hồi chậm hoặc mất kết nối.")), timeout); state.socket.emit(event, payload, (res) => { clearTimeout(timer); if (!res || !res.ok) { const e = new Error(res?.error || "Server báo lỗi."); if (!silent) toast(e.message, "error", 5200); reject(e); } else resolve(res); }); }); }
  function ensureSocket() { if (state.socket?.connected) return true; if (!state.backendUrl) openConfigModal("Anh cần dán URL Render backend trước."); else toast("Chưa kết nối server Render.", "error"); return false; }
  function openConfigModal(message="") { if (message) toast(message, "error", 4200); $("backendUrlInput").value = state.backendUrl; $("configModal").classList.remove("hidden"); $("backendUrlInput").focus(); }
  function closeConfigModal() { $("configModal").classList.add("hidden"); }
  function saveConfig() { const url = normalizeBackendUrl($("backendUrlInput").value); if (!/^https?:\/\//.test(url)) return toast("URL phải bắt đầu bằng http:// hoặc https://", "error"); state.backendUrl = url; localStorage.setItem(STORAGE_KEYS.backendUrl, url); closeConfigModal(); connectSocket(); }
  function toggleTheme() { const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark"; document.documentElement.dataset.theme = next; localStorage.setItem(STORAGE_KEYS.theme, next); $("themeToggle").textContent = next === "dark" ? "☀️" : "🌙"; }
  function toggleSound() { state.soundOn = !state.soundOn; localStorage.setItem(STORAGE_KEYS.sound, state.soundOn ? "on" : "off"); $("soundToggle").textContent = state.soundOn ? "🔊" : "🔇"; if (state.soundOn) playTone("turn"); }
  async function copyInviteLink() { const base = location.href.split("?")[0]; const link = `${base}?room=${encodeURIComponent(state.roomCode)}`; try { await navigator.clipboard.writeText(link); toast("Đã copy link mời.", "success"); } catch { toast(`Link mời: ${link}`); } }
  function updateUrlRoom(roomCode) { if (location.protocol === "file:") return; const url = new URL(location.href); url.searchParams.set("room", roomCode); history.replaceState({}, "", url); }
  function setWordControls(disabled) { ["wordInput","overlayWordInput","submitWordBtn","overlaySubmitBtn","passTurnBtn","overlayPassBtn"].forEach(id => { const el = $(id); if (el) el.disabled = disabled; }); $("submitWordBtn").textContent = disabled ? "Đang gửi..." : "Submit"; $("overlaySubmitBtn").textContent = disabled ? "Đang gửi..." : "Nộp từ"; }
  function playTone(type) { if (!state.soundOn) return; try { const ctx = new (window.AudioContext || window.webkitAudioContext)(); const osc = ctx.createOscillator(); const gain = ctx.createGain(); const now = ctx.currentTime; const map = { success:[720,920], error:[220,160], turn:[460,620] }; const [f1,f2] = map[type] || map.turn; osc.type="sine"; osc.frequency.setValueAtTime(f1, now); osc.frequency.exponentialRampToValueAtTime(Math.max(1,f2), now+.12); gain.gain.setValueAtTime(.0001, now); gain.gain.exponentialRampToValueAtTime(.08, now+.02); gain.gain.exponentialRampToValueAtTime(.0001, now+.22); osc.connect(gain).connect(ctx.destination); osc.start(now); osc.stop(now+.24); } catch(_){} }
  function toast(message, type="info", duration=3200) { const host = $("toastHost"), el = document.createElement("div"); el.className = `toast ${type}`; el.textContent = message; host.appendChild(el); setTimeout(()=>el.remove(), duration); }
  function normalizeBackendUrl(url) { return String(url || "").trim().replace(/\/$/, ""); }
  function sanitizeRoom(v) { return String(v || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 6); }
  function sanitizeName(v) { return String(v || "").trim().replace(/[<>]/g, "").slice(0, 24) || `Player${Math.floor(Math.random()*999)}`; }
  function sanitizeTopic(v) { return String(v || "General").trim().replace(/[<>]/g, "").slice(0, 50) || "General"; }
  function normalizeWord(v) { return String(v || "").toLowerCase().trim().replace(/[^a-z]/g, ""); }
  function clamp(v,min,max,fallback) { const n = Number(v); return Number.isFinite(n) ? Math.max(min, Math.min(max, Math.floor(n))) : fallback; }
  function randomId(prefix) { return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,10)}`; }
  function escapeHtml(v) { return String(v ?? "").replace(/[&<>"]/g, s => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[s])); }
  function escapeAttr(v) { return escapeHtml(v).replace(/'/g, "&#39;"); }
})();
