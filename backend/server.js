import express from "express";
import http from "http";
import cors from "cors";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import { Server } from "socket.io";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PORT = Number(process.env.PORT || 3000);
const CLIENT_ORIGIN = process.env.CLIENT_ORIGIN || "*";
const ADMIN_WORD_PASSWORD = process.env.ADMIN_WORD_PASSWORD || "";
const ROOM_EMPTY_DELETE_MS = 60_000;
const DISCONNECT_GRACE_MS = 15_000;
const MAX_CHAT_PER_ROOM = 120;
const MAX_ROOMS_PUBLIC = 80;

const app = express();
app.use(cors({ origin: CLIENT_ORIGIN === "*" ? true : CLIENT_ORIGIN.split(",").map(s => s.trim()) }));
app.use(express.json({ limit: "2mb" }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: CLIENT_ORIGIN === "*" ? true : CLIENT_ORIGIN.split(",").map(s => s.trim()), credentials: false },
  transports: ["websocket", "polling"],
  pingInterval: 20_000,
  pingTimeout: 25_000
});

const dictionary = loadDictionary();
const rooms = new Map();
const socketToPlayer = new Map();

app.get("/", (_req, res) => {
  res.json({ ok: true, app: "English Word Chain Realtime", rooms: rooms.size, words: dictionary.words.size });
});

app.get("/health", (_req, res) => {
  res.json({ ok: true, uptime: process.uptime(), rooms: rooms.size, words: dictionary.words.size });
});

app.get("/dictionary-meta", (_req, res) => {
  res.json({ ok: true, version: dictionary.version, totalWords: dictionary.words.size, customWords: dictionary.customWords.size });
});

io.on("connection", (socket) => {
  socket.emit("server:hello", { ok: true, socketId: socket.id, words: dictionary.words.size });

  socket.on("rooms:list", (payload, cb) => safe(socket, cb, () => {
    cbOk(cb, { rooms: getPublicRooms(payload || {}) });
  }));

  socket.on("room:create", (payload, cb) => safe(socket, cb, () => {
    const playerId = cleanId(payload?.playerId) || makeId("P");
    const nickname = cleanName(payload?.nickname);
    const avatar = cleanAvatar(payload?.avatar);
    const roomCode = makeRoomCode();
    const room = createRoom(roomCode, payload || {}, playerId, nickname, avatar, socket.id);
    rooms.set(roomCode, room);
    socket.join(roomCode);
    socketToPlayer.set(socket.id, { roomCode, playerId });
    emitRoomsList();
    emitState(roomCode);
    cbOk(cb, { roomCode, playerId, state: serializeRoom(room) });
  }));

  socket.on("room:join", (payload, cb) => safe(socket, cb, () => {
    const roomCode = cleanRoom(payload?.roomCode);
    const room = getRoomOrThrow(roomCode);
    if (room.status !== "lobby") throw new Error("Phòng đã bắt đầu hoặc đã kết thúc.");
    if (room.password && String(payload?.roomPassword || "") !== room.password) throw new Error("Sai mật khẩu phòng.");
    const activeHumans = [...room.players.values()].filter(p => p.type === "human" && p.status === "active").length;
    const existing = room.players.get(cleanId(payload?.playerId));
    if (!existing && activeHumans >= room.maxPlayers) throw new Error("Phòng đã đủ người.");
    const playerId = cleanId(payload?.playerId) || makeId("P");
    const player = upsertHuman(room, playerId, cleanName(payload?.nickname), cleanAvatar(payload?.avatar), socket.id);
    socket.join(roomCode);
    socketToPlayer.set(socket.id, { roomCode, playerId });
    room.updatedAt = Date.now();
    cancelEmptyDelete(room);
    emitRoomsList();
    emitState(roomCode, `${player.avatar} ${player.nickname} đã vào phòng.`);
    cbOk(cb, { roomCode, playerId, state: serializeRoom(room) });
  }));

  socket.on("room:leave", (_payload, cb) => safe(socket, cb, () => {
    leaveBySocket(socket, true);
    cbOk(cb, { ok: true });
  }));

  socket.on("room:start", (payload, cb) => safe(socket, cb, () => {
    const room = getRoomOrThrow(cleanRoom(payload?.roomCode));
    assertHost(room, payload?.playerId);
    startGame(room);
    emitRoomsList();
    emitState(room.roomCode, "Trận đấu đã bắt đầu.");
    cbOk(cb, { state: serializeRoom(room) });
  }));

  socket.on("room:setTeam", (payload, cb) => safe(socket, cb, () => {
    const room = getRoomOrThrow(cleanRoom(payload?.roomCode));
    const player = getPlayerOrThrow(room, payload?.playerId);
    if (room.status !== "lobby") throw new Error("Chỉ được đổi đội khi còn ở phòng chờ.");
    const team = String(payload?.team || "").toUpperCase() === "B" ? "B" : "A";
    player.team = team;
    room.updatedAt = Date.now();
    emitState(room.roomCode, `${player.nickname} đã chọn đội ${team}.`);
    cbOk(cb, { state: serializeRoom(room) });
  }));

  socket.on("word:submit", (payload, cb) => safe(socket, cb, () => {
    const receivedAt = Date.now();
    const room = getRoomOrThrow(cleanRoom(payload?.roomCode));
    const player = getPlayerOrThrow(room, payload?.playerId);
    const word = normalizeWord(payload?.word);
    const result = submitWord(room, player, word, receivedAt);
    emitState(room.roomCode, result.message);
    cbOk(cb, { ...result, state: serializeRoom(room) });
  }));

  socket.on("turn:pass", (payload, cb) => safe(socket, cb, () => {
    const room = getRoomOrThrow(cleanRoom(payload?.roomCode));
    const player = getPlayerOrThrow(room, payload?.playerId);
    const result = passTurn(room, player);
    emitState(room.roomCode, result.message);
    cbOk(cb, { ...result, state: serializeRoom(room) });
  }));

  socket.on("chat:send", (payload, cb) => safe(socket, cb, () => {
    const room = getRoomOrThrow(cleanRoom(payload?.roomCode));
    const player = getPlayerOrThrow(room, payload?.playerId);
    const message = String(payload?.message || "").trim().slice(0, 220);
    if (!message) throw new Error("Tin nhắn trống.");
    const item = { id: makeId("C"), playerId: player.playerId, nickname: player.nickname, avatar: player.avatar, message, createdAt: Date.now() };
    room.chat.push(item);
    if (room.chat.length > MAX_CHAT_PER_ROOM) room.chat.splice(0, room.chat.length - MAX_CHAT_PER_ROOM);
    room.updatedAt = Date.now();
    io.to(room.roomCode).emit("chat:update", { chat: room.chat.slice(-MAX_CHAT_PER_ROOM) });
    cbOk(cb, { chat: room.chat.slice(-MAX_CHAT_PER_ROOM) });
  }));

  socket.on("reaction:send", (payload, cb) => safe(socket, cb, () => {
    const room = getRoomOrThrow(cleanRoom(payload?.roomCode));
    const player = getPlayerOrThrow(room, payload?.playerId);
    const emoji = cleanReaction(payload?.emoji);
    io.to(room.roomCode).emit("reaction:new", { id: makeId("R"), emoji, nickname: player.nickname, avatar: player.avatar, createdAt: Date.now() });
    cbOk(cb, { ok: true });
  }));

  socket.on("word:addCustom", (payload, cb) => safe(socket, cb, async () => {
    const password = String(payload?.password || "");
    if (!ADMIN_WORD_PASSWORD || password !== ADMIN_WORD_PASSWORD) throw new Error("Mật khẩu quản trị không đúng.");
    const word = normalizeWord(payload?.word);
    if (word.length < 3) throw new Error("Từ cần tối thiểu 3 chữ cái.");
    const topic = cleanTopic(payload?.topic || "Custom");
    const level = cleanLevel(payload?.level || "custom");
    dictionary.words.add(word);
    dictionary.customWords.add(word);
    dictionary.byFirstLetter.set(word[0], [...(dictionary.byFirstLetter.get(word[0]) || []), word]);
    const saveResult = await tryPersistWordToGithub(word, topic, level);
    io.emit("dictionary:customWord", { word, topic, level, totalWords: dictionary.words.size });
    cbOk(cb, { word, topic, level, totalWords: dictionary.words.size, persisted: saveResult.persisted, note: saveResult.note });
  }));

  socket.on("disconnect", () => {
    leaveBySocket(socket, false);
  });
});

function loadDictionary() {
  const mainPath = path.join(__dirname, "data", "valid_words.json");
  const customPath = path.join(__dirname, "data", "custom_words.json");
  const raw = JSON.parse(fs.readFileSync(mainPath, "utf8"));
  const custom = fs.existsSync(customPath) ? JSON.parse(fs.readFileSync(customPath, "utf8")) : { customWords: [] };
  const words = new Set();
  const byFirstLetter = new Map();
  const add = (w) => {
    const word = normalizeWord(w);
    if (word.length < 3) return;
    words.add(word);
    const first = word[0];
    if (!byFirstLetter.has(first)) byFirstLetter.set(first, []);
    byFirstLetter.get(first).push(word);
  };
  if (Array.isArray(raw.words)) raw.words.forEach(add);
  if (raw.wordsByLetter) Object.values(raw.wordsByLetter).forEach(list => Array.isArray(list) && list.forEach(add));
  if (Array.isArray(raw.customWords)) raw.customWords.forEach(add);
  if (Array.isArray(custom.customWords)) custom.customWords.forEach(add);
  return { version: raw.version || "json", words, byFirstLetter, customWords: new Set(custom.customWords || []) };
}

function createRoom(roomCode, payload, hostId, nickname, avatar, socketId) {
  const now = Date.now();
  const room = {
    roomCode,
    roomName: cleanTopic(payload.roomName || `${nickname} - Nối từ`),
    password: String(payload.roomPassword || "").trim().slice(0, 24),
    hostId,
    maxPlayers: clamp(payload.maxPlayers, 2, 12, 8),
    botCount: clamp(payload.botCount, 0, 3, 0),
    turnSeconds: clamp(payload.turnSeconds, 15, 90, 30),
    totalRounds: clamp(payload.totalRounds, 1, 99, 5),
    roundMode: payload.roundMode === "infinite" ? "infinite" : "finite",
    topic: cleanTopic(payload.topic || "All"),
    chainRule: payload.chainRule === "first-letter" ? "first-letter" : "last-letter",
    mode: payload.mode === "team" ? "team" : "solo",
    status: "lobby",
    currentWord: "",
    currentTurnPlayerId: "",
    currentTeam: "A",
    currentRound: 0,
    turnsCompletedInRound: 0,
    roundActorCount: 0,
    turnStartedAt: 0,
    turnDeadlineAt: 0,
    players: new Map(),
    playerOrder: [],
    usedWords: new Set(),
    chain: [],
    chat: [],
    reactions: [],
    lastEvent: "Phòng đã tạo.",
    version: 1,
    createdAt: now,
    updatedAt: now,
    timer: null,
    emptyTimer: null
  };
  upsertHuman(room, hostId, nickname, avatar, socketId);
  for (let i = 0; i < room.botCount; i++) addBot(room, i + 1);
  return room;
}

function upsertHuman(room, playerId, nickname, avatar, socketId) {
  let player = room.players.get(playerId);
  if (!player) {
    player = {
      playerId, socketId, nickname, avatar, type: "human", status: "active", connected: true,
      score: 0, validCount: 0, penalty: 0, team: assignTeam(room), orderIndex: room.playerOrder.length, joinedAt: Date.now(), lastSeenAt: Date.now()
    };
    room.players.set(playerId, player);
    room.playerOrder.push(playerId);
  } else {
    player.socketId = socketId;
    player.nickname = nickname || player.nickname;
    player.avatar = avatar || player.avatar;
    player.status = "active";
    player.connected = true;
    player.lastSeenAt = Date.now();
  }
  return player;
}

function addBot(room, n) {
  const id = makeId("BOT");
  const avatars = ["🤖", "🦊", "🐼", "🐯", "🧠"];
  const player = {
    playerId: id, socketId: "", nickname: `Bot ${n}`, avatar: avatars[n % avatars.length], type: "bot", status: "active", connected: true,
    score: 0, validCount: 0, penalty: 0, team: n % 2 ? "B" : "A", orderIndex: room.playerOrder.length, joinedAt: Date.now(), lastSeenAt: Date.now()
  };
  room.players.set(id, player);
  room.playerOrder.push(id);
}

function assignTeam(room) {
  const a = [...room.players.values()].filter(p => p.team === "A").length;
  const b = [...room.players.values()].filter(p => p.team === "B").length;
  return a <= b ? "A" : "B";
}

function startGame(room) {
  if (room.status !== "lobby") throw new Error("Trận đã bắt đầu hoặc đã kết thúc.");
  const actors = getTurnActors(room);
  if (actors.length < 1) throw new Error("Chưa có người chơi.");
  room.status = "playing";
  room.currentRound = 1;
  room.turnsCompletedInRound = 0;
  room.roundActorCount = actors.length;
  room.currentWord = randomStartWord();
  room.usedWords.add(room.currentWord);
  room.chain.push(systemWord(room.currentWord));
  room.currentTurnPlayerId = room.mode === "solo" ? actors[0] : "";
  room.currentTeam = room.mode === "team" ? actors[0] : "";
  startTurnClock(room);
}

function randomStartWord() {
  const starts = ["apple", "earth", "table", "river", "window", "school", "garden", "animal", "orange", "teacher", "student", "market", "travel", "future", "energy"];
  return starts[Math.floor(Math.random() * starts.length)];
}

function systemWord(word) {
  return { id: makeId("W"), word, playerId: "SYSTEM", nickname: "Hệ thống", avatar: "⚙️", valid: true, scoreDelta: 0, createdAt: Date.now() };
}

function startTurnClock(room) {
  clearRoomTimer(room);
  const now = Date.now();
  room.turnStartedAt = now;
  room.turnDeadlineAt = now + room.turnSeconds * 1000;
  room.updatedAt = now;
  room.version++;
  room.timer = setTimeout(() => onTurnTimeout(room.roomCode), room.turnSeconds * 1000 + 350);
  scheduleBotIfNeeded(room);
}

function scheduleBotIfNeeded(room) {
  if (room.status !== "playing") return;
  const current = getCurrentActorPlayer(room);
  if (!current || current.type !== "bot") return;
  const delay = Math.min(2500, Math.max(800, Math.floor(room.turnSeconds * 1000 * 0.22)));
  setTimeout(() => {
    const fresh = rooms.get(room.roomCode);
    if (!fresh || fresh.status !== "playing") return;
    const bot = getCurrentActorPlayer(fresh);
    if (!bot || bot.playerId !== current.playerId) return;
    const letter = requiredLetter(fresh);
    const word = pickBotWord(letter, fresh.usedWords);
    if (word) submitWord(fresh, bot, word, Date.now());
    else passTurn(fresh, bot, true);
    emitState(fresh.roomCode, fresh.lastEvent);
  }, delay);
}

function onTurnTimeout(roomCode) {
  const room = rooms.get(roomCode);
  if (!room || room.status !== "playing") return;
  const player = getCurrentActorPlayer(room);
  if (player) {
    player.score -= 1;
    player.penalty += 1;
    room.chain.push({ id: makeId("W"), word: "[timeout]", playerId: player.playerId, nickname: player.nickname, avatar: player.avatar, valid: false, scoreDelta: -1, createdAt: Date.now() });
  }
  advanceTurn(room, `Hết giờ: ${player ? player.nickname : "người chơi"} bị trừ 1 điểm.`);
  emitState(room.roomCode, room.lastEvent);
}

function submitWord(room, player, word, receivedAt) {
  if (room.status !== "playing") throw new Error("Trận chưa bắt đầu hoặc đã kết thúc.");
  if (!isPlayersTurn(room, player)) throw new Error("Chưa đến lượt bạn.");
  if (!word || word.length < 3) return invalidWord(room, player, word, "Từ cần tối thiểu 3 chữ cái.");
  if (receivedAt > room.turnDeadlineAt + 1200) return invalidWord(room, player, word, "Đã hết giờ trước khi server nhận được từ.");
  const req = requiredLetter(room);
  if (req && word[0] !== req) return invalidWord(room, player, word, `Từ phải bắt đầu bằng chữ ${req.toUpperCase()}.`);
  if (room.usedWords.has(word)) return invalidWord(room, player, word, "Từ này đã được dùng trong chuỗi.");
  if (!dictionary.words.has(word)) return invalidWord(room, player, word, "Từ này chưa có trong từ điển JSON.");

  const leftMs = Math.max(0, room.turnDeadlineAt - receivedAt);
  const bonus = leftMs > room.turnSeconds * 1000 * 0.65 ? 2 : leftMs > room.turnSeconds * 1000 * 0.35 ? 1 : 0;
  const scoreDelta = 3 + bonus;
  player.score += scoreDelta;
  player.validCount += 1;
  room.usedWords.add(word);
  room.currentWord = word;
  room.chain.push({ id: makeId("W"), word, playerId: player.playerId, nickname: player.nickname, avatar: player.avatar, valid: true, scoreDelta, createdAt: Date.now() });
  advanceTurn(room, `Đúng: ${player.nickname} +${scoreDelta} điểm với từ “${word}”.`);
  return { accepted: true, scoreDelta, message: room.lastEvent };
}

function invalidWord(room, player, word, reason) {
  player.score -= 1;
  player.penalty += 1;
  room.chain.push({ id: makeId("W"), word: word || "[blank]", playerId: player.playerId, nickname: player.nickname, avatar: player.avatar, valid: false, scoreDelta: -1, reason, createdAt: Date.now() });
  advanceTurn(room, `Sai: ${player.nickname} bị trừ 1 điểm. ${reason}`);
  return { accepted: false, scoreDelta: -1, message: room.lastEvent };
}

function passTurn(room, player, silent = false) {
  if (room.status !== "playing") throw new Error("Trận chưa bắt đầu hoặc đã kết thúc.");
  if (!isPlayersTurn(room, player) && !silent) throw new Error("Chưa đến lượt bạn.");
  player.score -= 1;
  player.penalty += 1;
  room.chain.push({ id: makeId("W"), word: "[pass]", playerId: player.playerId, nickname: player.nickname, avatar: player.avatar, valid: false, scoreDelta: -1, reason: "Bỏ qua lượt", createdAt: Date.now() });
  advanceTurn(room, `${player.nickname} bỏ qua lượt và bị trừ 1 điểm.`);
  return { accepted: false, scoreDelta: -1, message: room.lastEvent };
}

function advanceTurn(room, message) {
  clearRoomTimer(room);
  room.turnsCompletedInRound += 1;
  const actors = getTurnActors(room);
  room.roundActorCount = actors.length || room.roundActorCount || 1;

  if (room.roundMode !== "infinite" && room.turnsCompletedInRound >= room.roundActorCount) {
    if (room.currentRound >= room.totalRounds) {
      room.status = "ended";
      room.lastEvent = `Kết thúc trận. ${winnerText(room)}`;
      room.updatedAt = Date.now();
      room.version++;
      return;
    }
    room.currentRound += 1;
    room.turnsCompletedInRound = 0;
  } else if (room.roundMode === "infinite" && room.turnsCompletedInRound >= room.roundActorCount) {
    room.currentRound += 1;
    room.turnsCompletedInRound = 0;
  }

  setNextActor(room, actors);
  room.lastEvent = message;
  startTurnClock(room);
}

function getTurnActors(room) {
  if (room.mode === "team") {
    const teams = new Set([...room.players.values()].filter(p => p.status === "active").map(p => p.team).filter(Boolean));
    return ["A", "B"].filter(t => teams.has(t));
  }
  return room.playerOrder.filter(id => {
    const p = room.players.get(id);
    return p && p.status === "active";
  });
}

function setNextActor(room, actors = getTurnActors(room)) {
  if (!actors.length) return endRoom(room, "Không còn người chơi active.");
  if (room.mode === "team") {
    const idx = Math.max(0, actors.indexOf(room.currentTeam));
    room.currentTeam = actors[(idx + 1) % actors.length];
    room.currentTurnPlayerId = "";
  } else {
    const idx = Math.max(0, actors.indexOf(room.currentTurnPlayerId));
    room.currentTurnPlayerId = actors[(idx + 1) % actors.length];
  }
}

function getCurrentActorPlayer(room) {
  if (room.mode === "team") {
    const teamPlayers = room.playerOrder.map(id => room.players.get(id)).filter(p => p && p.status === "active" && p.team === room.currentTeam);
    if (!teamPlayers.length) return null;
    return teamPlayers.reduce((a, b) => (a.lastSeenAt || 0) <= (b.lastSeenAt || 0) ? a : b);
  }
  return room.players.get(room.currentTurnPlayerId) || null;
}

function isPlayersTurn(room, player) {
  if (room.mode === "team") return player.team && player.team === room.currentTeam && player.status === "active";
  return player.playerId === room.currentTurnPlayerId && player.status === "active";
}

function requiredLetter(room) {
  const w = normalizeWord(room.currentWord);
  if (!w) return "";
  return room.chainRule === "first-letter" ? w[0] : w[w.length - 1];
}

function pickBotWord(letter, usedWords) {
  const list = dictionary.byFirstLetter.get(letter) || [];
  for (let i = 0; i < 50; i++) {
    const w = list[Math.floor(Math.random() * list.length)];
    if (w && !usedWords.has(w)) return w;
  }
  return list.find(w => !usedWords.has(w)) || "";
}

function winnerText(room) {
  const players = [...room.players.values()].sort((a, b) => b.score - a.score);
  const w = players[0];
  return w ? `Người thắng: ${w.avatar} ${w.nickname} (${w.score} điểm).` : "Không có người thắng.";
}

function endRoom(room, message) {
  clearRoomTimer(room);
  room.status = "ended";
  room.lastEvent = message;
  room.updatedAt = Date.now();
  room.version++;
}

function leaveBySocket(socket, explicit) {
  const meta = socketToPlayer.get(socket.id);
  if (!meta) return;
  socketToPlayer.delete(socket.id);
  const room = rooms.get(meta.roomCode);
  if (!room) return;
  const player = room.players.get(meta.playerId);
  if (!player) return;
  player.connected = false;
  player.lastSeenAt = Date.now();
  if (explicit) player.status = "left";
  else setTimeout(() => markDisconnected(meta.roomCode, meta.playerId), DISCONNECT_GRACE_MS);
  socket.leave(meta.roomCode);
  room.updatedAt = Date.now();
  checkRoomEmpty(room);
  emitState(room.roomCode, explicit ? `${player.nickname} đã thoát phòng.` : `${player.nickname} bị mất kết nối tạm thời.`);
  emitRoomsList();
}

function markDisconnected(roomCode, playerId) {
  const room = rooms.get(roomCode);
  if (!room) return;
  const player = room.players.get(playerId);
  if (!player || player.connected) return;
  player.status = "offline";
  room.updatedAt = Date.now();
  checkRoomEmpty(room);
  emitState(roomCode, `${player.nickname} đang offline.`);
}

function checkRoomEmpty(room) {
  const onlineHumans = [...room.players.values()].filter(p => p.type === "human" && p.connected && p.status === "active").length;
  const everActiveHumans = [...room.players.values()].filter(p => p.type === "human" && p.status === "active").length;
  if (onlineHumans === 0 || everActiveHumans === 0) {
    cancelEmptyDelete(room);
    room.emptyTimer = setTimeout(() => {
      const fresh = rooms.get(room.roomCode);
      if (!fresh) return;
      const online = [...fresh.players.values()].filter(p => p.type === "human" && p.connected && p.status === "active").length;
      if (online === 0) {
        clearRoomTimer(fresh);
        rooms.delete(fresh.roomCode);
        emitRoomsList();
      }
    }, ROOM_EMPTY_DELETE_MS);
  }
}

function cancelEmptyDelete(room) {
  if (room.emptyTimer) clearTimeout(room.emptyTimer);
  room.emptyTimer = null;
}

function emitState(roomCode, eventMsg = "") {
  const room = rooms.get(roomCode);
  if (!room) return;
  if (eventMsg) room.lastEvent = eventMsg;
  room.updatedAt = Date.now();
  room.version++;
  io.to(roomCode).emit("room:state", serializeRoom(room));
}

function emitRoomsList() {
  io.emit("rooms:update", { rooms: getPublicRooms({}) });
}

function getPublicRooms() {
  return [...rooms.values()]
    .sort((a, b) => b.updatedAt - a.updatedAt)
    .slice(0, MAX_ROOMS_PUBLIC)
    .map(room => ({
      roomCode: room.roomCode,
      roomName: room.roomName,
      status: room.status,
      activeHumans: [...room.players.values()].filter(p => p.type === "human" && p.status === "active").length,
      connectedHumans: [...room.players.values()].filter(p => p.type === "human" && p.connected && p.status === "active").length,
      maxPlayers: room.maxPlayers,
      botCount: room.botCount,
      mode: room.mode,
      topic: room.topic,
      chainRule: room.chainRule,
      turnSeconds: room.turnSeconds,
      totalRounds: room.totalRounds,
      currentRound: room.currentRound,
      passwordProtected: Boolean(room.password),
      updatedAt: room.updatedAt
    }));
}

function serializeRoom(room) {
  return {
    room: {
      roomCode: room.roomCode,
      roomName: room.roomName,
      hostId: room.hostId,
      maxPlayers: room.maxPlayers,
      botCount: room.botCount,
      turnSeconds: room.turnSeconds,
      totalRounds: room.totalRounds,
      roundMode: room.roundMode,
      topic: room.topic,
      chainRule: room.chainRule,
      mode: room.mode,
      status: room.status,
      currentWord: room.currentWord,
      currentTurnPlayerId: room.currentTurnPlayerId,
      currentTeam: room.currentTeam,
      currentRound: room.currentRound,
      turnsCompletedInRound: room.turnsCompletedInRound,
      roundActorCount: room.roundActorCount,
      turnStartedAt: room.turnStartedAt,
      turnDeadlineAt: room.turnDeadlineAt,
      lastEvent: room.lastEvent,
      version: room.version,
      updatedAt: room.updatedAt
    },
    players: room.playerOrder.map(id => room.players.get(id)).filter(Boolean).map(p => ({ ...p, socketId: undefined })),
    chain: room.chain.slice(-120),
    chat: room.chat.slice(-MAX_CHAT_PER_ROOM),
    dictionaryMeta: { totalWords: dictionary.words.size, customWords: dictionary.customWords.size }
  };
}

function getRoomOrThrow(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) throw new Error("Không tìm thấy phòng.");
  return room;
}
function getPlayerOrThrow(room, playerId) {
  const player = room.players.get(cleanId(playerId));
  if (!player) throw new Error("Không tìm thấy người chơi trong phòng.");
  return player;
}
function assertHost(room, playerId) {
  if (room.hostId !== cleanId(playerId)) throw new Error("Chỉ chủ phòng được thực hiện thao tác này.");
}
function clearRoomTimer(room) { if (room.timer) clearTimeout(room.timer); room.timer = null; }

function safe(socket, cb, fn) {
  Promise.resolve().then(fn).catch(err => {
    console.error("socket error", socket.id, err);
    cbErr(cb, err.message || String(err));
  });
}
function cbOk(cb, data) { if (typeof cb === "function") cb({ ok: true, ...data }); }
function cbErr(cb, error) { if (typeof cb === "function") cb({ ok: false, error }); }

function cleanId(v) { return String(v || "").replace(/[^a-zA-Z0-9_\-]/g, "").slice(0, 80); }
function cleanRoom(v) { return String(v || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 6); }
function cleanName(v) { return String(v || "Player").trim().replace(/[<>]/g, "").slice(0, 24) || "Player"; }
function cleanAvatar(v) { return String(v || "🙂").slice(0, 4); }
function cleanTopic(v) { return String(v || "General").trim().replace(/[<>]/g, "").slice(0, 50) || "General"; }
function cleanLevel(v) { const s = String(v || "custom").toLowerCase(); return ["basic", "intermediate", "advanced", "custom"].includes(s) ? s : "custom"; }
function cleanReaction(v) { return ["❤️", "😂", "🔥", "👏", "💡", "🎯"].includes(v) ? v : "👏"; }
function normalizeWord(v) { return String(v || "").toLowerCase().trim().replace(/[^a-z]/g, ""); }
function clamp(v, min, max, fallback) { const n = Number(v); return Number.isFinite(n) ? Math.max(min, Math.min(max, Math.floor(n))) : fallback; }
function makeId(prefix) { return `${prefix}_${Date.now().toString(36)}_${crypto.randomBytes(4).toString("hex")}`; }
function makeRoomCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  for (let tries = 0; tries < 200; tries++) {
    let code = "";
    for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
    if (!rooms.has(code)) return code;
  }
  throw new Error("Không tạo được mã phòng.");
}

async function tryPersistWordToGithub(word, topic, level) {
  const token = process.env.GITHUB_TOKEN;
  const owner = process.env.GITHUB_OWNER;
  const repo = process.env.GITHUB_REPO;
  const branch = process.env.GITHUB_BRANCH || "main";
  const dictPath = process.env.GITHUB_DICT_PATH || "frontend/data/valid_words.json";
  if (!token || !owner || !repo) return { persisted: false, note: "Đã thêm tạm vào server. Muốn lưu vĩnh viễn, cấu hình GITHUB_TOKEN trên Render hoặc sửa JSON thủ công." };
  try {
    const url = `https://api.github.com/repos/${owner}/${repo}/contents/${dictPath}?ref=${encodeURIComponent(branch)}`;
    const getRes = await fetch(url, { headers: githubHeaders(token) });
    if (!getRes.ok) throw new Error(`GitHub GET ${getRes.status}`);
    const file = await getRes.json();
    const text = Buffer.from(file.content, "base64").toString("utf8");
    const json = JSON.parse(text);
    if (!Array.isArray(json.customWords)) json.customWords = [];
    if (!json.customWords.includes(word)) json.customWords.push(word);
    json.customWords.sort();
    json.updatedAt = new Date().toISOString();
    const newContent = Buffer.from(JSON.stringify(json, null, 2) + "\n", "utf8").toString("base64");
    const putRes = await fetch(`https://api.github.com/repos/${owner}/${repo}/contents/${dictPath}`, {
      method: "PUT",
      headers: githubHeaders(token),
      body: JSON.stringify({ message: `Add word: ${word}`, content: newContent, sha: file.sha, branch })
    });
    if (!putRes.ok) throw new Error(`GitHub PUT ${putRes.status}: ${await putRes.text()}`);
    return { persisted: true, note: "Đã lưu vào GitHub JSON." };
  } catch (err) {
    console.error("GitHub persist failed", err);
    return { persisted: false, note: "Đã thêm tạm vào server nhưng chưa lưu được vào GitHub JSON." };
  }
}
function githubHeaders(token) {
  return {
    "Authorization": `Bearer ${token}`,
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
    "Content-Type": "application/json",
    "User-Agent": "English-Word-Chain-Render"
  };
}

server.listen(PORT, () => {
  console.log(`English Word Chain backend listening on ${PORT}`);
  console.log(`Dictionary words: ${dictionary.words.size}`);
});
