// ═══════════════════════════════════════════════════════════════════
//  RAFT REPLICA — server.js
//  Task W2-T2 complete: Election timeout implemented.
//  Stubs remain for W2-T3 through W2-T8.
// ═══════════════════════════════════════════════════════════════════

// ── 1. IMPORTS ─────────────────────────────────────────────────────
const express = require('express');
const axios   = require('axios');

// ── 2. CREATE THE EXPRESS APP ──────────────────────────────────────
const app = express();
app.use(express.json());

// ── 3. READ ENVIRONMENT VARIABLES ─────────────────────────────────
const REPLICA_ID  = process.env.REPLICA_ID;
const PORT        = process.env.PORT        || 3001;
const PEERS_RAW   = process.env.PEERS       || '';
const GATEWAY_URL = process.env.GATEWAY_URL || 'http://gateway:8080';

const PEERS = PEERS_RAW ? PEERS_RAW.split(',') : [];

// ── 4. RAFT STATE VARIABLES ────────────────────────────────────────
let state       = 'follower';
let currentTerm = 0;
let votedFor    = null;
let log         = [];
let commitIndex = -1;
let leaderId    = null;
let electionTimer = null;

// ── 5. HELPER: LOGGING ────────────────────────────────────────────
function log_msg(msg) {
  console.log(`[R${REPLICA_ID}][term=${currentTerm}] ${msg}`);
}

// ═══════════════════════════════════════════════════════════════════
//  TASK W2-T2 — ELECTION TIMEOUT  ✅ IMPLEMENTED
// ═══════════════════════════════════════════════════════════════════
//
//  HOW IT WORKS:
//  Every replica runs a countdown timer (500–800ms random).
//  When the timer fires → replica assumes leader is dead → starts election.
//  When a heartbeat arrives → timer is reset → no election (leader alive).
//
//  WHY RANDOM? If all timers were the same length (e.g. 500ms), all 3
//  replicas would start elections simultaneously → split vote → nobody
//  wins → infinite loop. Randomness means one fires first and wins cleanly.
//
// ═══════════════════════════════════════════════════════════════════

function resetElectionTimer() {
  // Step 1: Cancel any running timer (safe even if electionTimer is null)
  clearTimeout(electionTimer);

  // Step 2: Pick a random delay between 500ms and 800ms
  //   Math.random()       → float in [0, 1)
  //   Math.random() * 300 → float in [0, 300)
  //   500 + that          → float in [500, 800)
  const timeoutMs = Math.floor(500 + Math.random() * 300);

  // Step 3: Schedule startElection() after timeoutMs
  //   If a heartbeat arrives before this fires → resetElectionTimer() is
  //   called again, cancelling this one and starting a fresh countdown.
  //   If nothing arrives → startElection() fires.
  electionTimer = setTimeout(startElection, timeoutMs);

  log_msg(`⏱  Election timer reset (${timeoutMs}ms)`);
}

function stopElectionTimer() {
  // Called when this replica becomes the leader.
  // Leaders send heartbeats — they don't need to receive them.
  clearTimeout(electionTimer);
  electionTimer = null;
  log_msg('⏹  Election timer stopped (I am the leader)');
}

// ── STUB FUNCTIONS ─────────────────────────────────────────────────

async function startElection() {
  // TODO W2-T3 & W2-T4: become candidate, request votes, become leader
  log_msg('🗳  Election timer fired — startElection() coming in Task W2-T3/4');
  resetElectionTimer(); // safety: restart timer so we don't get stuck
}

function startHeartbeats() {
  // TODO W2-T5: setInterval sending POST /heartbeat to all PEERS every 150ms
  log_msg('💓 startHeartbeats() coming in Task W2-T5');
}

// ── API ROUTES ─────────────────────────────────────────────────────

// GET /status — ✅ fully implemented
app.get('/status', (req, res) => {
  res.json({
    replicaId:   REPLICA_ID,
    state:       state,
    currentTerm: currentTerm,
    leaderId:    leaderId,
    logLength:   log.length,
    commitIndex: commitIndex,
    peers:       PEERS,
  });
});

// POST /request-vote — 🔲 stub (Task W2-T3)
app.post('/request-vote', (req, res) => {
  res.json({ voteGranted: false, term: currentTerm });
});

// POST /heartbeat — ✅ timer reset added (W2-T2), full logic in W2-T5
app.post('/heartbeat', (req, res) => {
  const { term } = req.body;

  // W2-T2: Reset election timer if heartbeat is from a valid leader
  // (term >= currentTerm means it's not from a stale old leader)
  if (term !== undefined && term >= currentTerm) {
    resetElectionTimer(); // ← W2-T2 contribution
    // TODO W2-T5: also update currentTerm, state, leaderId
  }

  res.json({ ok: true, term: currentTerm });
});

// POST /append-entries — 🔲 stub (Task W2-T6)
app.post('/append-entries', (req, res) => {
  res.json({ success: false, logLength: log.length });
});

// POST /stroke — 🔲 stub (Task W2-T7)
app.post('/stroke', (req, res) => {
  if (state !== 'leader') {
    return res.status(403).json({ error: 'not_leader', leaderId, message: 'Not the leader.' });
  }
  res.json({ ok: false, message: 'Not yet implemented.' });
});

// GET /sync-log — 🔲 stub (Task W2-T8)
app.get('/sync-log', (req, res) => {
  const fromIndex = parseInt(req.query.from || '0', 10);
  res.json({ entries: log.slice(fromIndex), commitIndex });
});

// ── START SERVER ───────────────────────────────────────────────────
app.listen(PORT, () => {
  log_msg(`🚀 Server started on port ${PORT}`);
  log_msg(`   State: ${state} | Peers: ${PEERS.join(', ') || 'none'}`);

  // W2-T2: Start election timer on boot.
  // Whichever replica has the shortest random timeout will fire first
  // and trigger the very first election — no human action needed.
  resetElectionTimer();
});

module.exports = { app };