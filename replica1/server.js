// ═══════════════════════════════════════════════════════════════════
//  RAFT REPLICA — server.js   COMPLETE IMPLEMENTATION
//  ✅ W2-T1  Scaffold + /status
//  ✅ W2-T2  Election timeout
//  ✅ W2-T3  /request-vote
//  ✅ W2-T4  startElection()
//  ✅ W2-T5  /heartbeat + startHeartbeats()
//  ✅ W2-T6  /append-entries
//  ✅ W2-T7  /stroke
//  ✅ W2-T8  /sync-log
// ═══════════════════════════════════════════════════════════════════

const express = require('express');
const axios   = require('axios');

const app = express();
app.use(express.json());

// ENVIRONMENT VARIABLES - injected by docker-compose.yml
const REPLICA_ID  = process.env.REPLICA_ID;
const PORT        = parseInt(process.env.PORT || '3001', 10);
const PEERS_RAW   = process.env.PEERS || '';
const GATEWAY_URL = process.env.GATEWAY_URL || 'http://gateway:8080';
const PEERS       = PEERS_RAW ? PEERS_RAW.split(',').map(p => p.trim()) : [];

// RAFT STATE
let state             = 'follower';
let currentTerm       = 0;
let votedFor          = null;
let raftLog           = [];
let commitIndex       = -1;
let leaderId          = null;
let electionTimer     = null;
let heartbeatInterval = null;

function log_msg(msg) {
  console.log(`[R${REPLICA_ID}][term=${currentTerm}] ${msg}`);
}

// ELECTION TIMER
function resetElectionTimer() {
  clearTimeout(electionTimer);
  const ms = Math.floor(500 + Math.random() * 300);
  electionTimer = setTimeout(startElection, ms);
}
function stopElectionTimer() {
  clearTimeout(electionTimer);
  electionTimer = null;
}

// STEP DOWN when higher term seen
function stepDownIfNeeded(incomingTerm) {
  if (incomingTerm > currentTerm) {
    currentTerm = incomingTerm;
    state       = 'follower';
    votedFor    = null;
    leaderId    = null;
    stopHeartbeats();
    resetElectionTimer();
    log_msg('Stepped down - higher term seen');
    return true;
  }
  return false;
}

// LEADER ELECTION
async function startElection() {
  state       = 'candidate';
  currentTerm = currentTerm + 1;
  votedFor    = REPLICA_ID;
  leaderId    = null;
  let votes   = 1;
  log_msg('Election started term=' + currentTerm);

  const requests = PEERS.map(async (peer) => {
    try {
      const res = await axios.post(peer + '/request-vote',
        { term: currentTerm, candidateId: REPLICA_ID },
        { timeout: 300 }
      );
      if (res.data.voteGranted) { log_msg('Vote from ' + peer); return true; }
      return false;
    } catch { return false; }
  });

  const results = await Promise.allSettled(requests);
  results.forEach(r => { if (r.status === 'fulfilled' && r.value) votes++; });

  if (state !== 'candidate') { log_msg('Election aborted'); return; }

  if (votes >= 2) {
    state    = 'leader';
    leaderId = REPLICA_ID;
    stopElectionTimer();
    startHeartbeats();
    log_msg('BECAME LEADER votes=' + votes);
  } else {
    state = 'follower';
    log_msg('Lost election votes=' + votes);
    resetElectionTimer();
  }
}

// HEARTBEATS
function startHeartbeats() {
  clearInterval(heartbeatInterval);
  heartbeatInterval = setInterval(async () => {
    if (state !== 'leader') { clearInterval(heartbeatInterval); return; }
    for (const peer of PEERS) {
      try {
        await axios.post(peer + '/heartbeat',
          { term: currentTerm, leaderId: REPLICA_ID },
          { timeout: 100 }
        );
      } catch { /* peer offline */ }
    }
  }, 150);
}
function stopHeartbeats() {
  clearInterval(heartbeatInterval);
  heartbeatInterval = null;
}

// SYNC FOLLOWER (background)
async function syncFollower(peerUrl, fromIndex) {
  try {
    const missing = raftLog.filter(e => e.index >= fromIndex && e.index <= commitIndex);
    for (const entry of missing) {
      await axios.post(peerUrl + '/append-entries',
        { term: currentTerm, leaderId: REPLICA_ID, entry, leaderCommit: commitIndex },
        { timeout: 300 }
      );
    }
  } catch { /* sync failed, will retry */ }
}

// GET /status
app.get('/status', (req, res) => {
  res.json({ replicaId: REPLICA_ID, state, currentTerm, leaderId,
             logLength: raftLog.length, commitIndex, peers: PEERS });
});

// POST /request-vote
app.post('/request-vote', (req, res) => {
  const { term, candidateId } = req.body;
  stepDownIfNeeded(term);
  const voteGranted = (term >= currentTerm) &&
                      (votedFor === null || votedFor === candidateId);
  if (voteGranted) { votedFor = candidateId; resetElectionTimer(); }
  res.json({ voteGranted, term: currentTerm });
});

// POST /heartbeat
app.post('/heartbeat', (req, res) => {
  const { term, leaderId: incomingLeaderId } = req.body;
  stepDownIfNeeded(term);
  if (term >= currentTerm) {
    currentTerm = term;
    state       = 'follower';
    leaderId    = incomingLeaderId;
    stopHeartbeats();
    resetElectionTimer();
  }
  res.json({ ok: true, term: currentTerm });
});

// POST /append-entries
app.post('/append-entries', (req, res) => {
  const { term, leaderId: incomingLeaderId, entry, leaderCommit } = req.body;
  if (term < currentTerm) return res.json({ success: false, logLength: raftLog.length });
  stepDownIfNeeded(term);
  currentTerm = term;
  state       = 'follower';
  leaderId    = incomingLeaderId;
  resetElectionTimer();
  if (entry) {
    const exists = raftLog.some(e => e.index === entry.index && e.term === entry.term);
    if (!exists) { raftLog.push(entry); }
  }
  if (leaderCommit !== undefined && leaderCommit > commitIndex) {
    commitIndex = Math.min(leaderCommit, raftLog.length - 1);
  }
  res.json({ success: true, logLength: raftLog.length });
});

// POST /stroke  (leader only)
app.post('/stroke', async (req, res) => {
  if (state !== 'leader') return res.status(403).json({ error: 'not_leader', leaderId });
  const stroke = req.body;
  const entry  = { index: raftLog.length, term: currentTerm, stroke };
  raftLog.push(entry);
  let confirmations = 1;
  const reps = PEERS.map(async (peer) => {
    try {
      const r = await axios.post(peer + '/append-entries',
        { term: currentTerm, leaderId: REPLICA_ID, entry, leaderCommit: commitIndex },
        { timeout: 300 }
      );
      if (r.data.success) { confirmations++; return; }
      syncFollower(peer, r.data.logLength).catch(() => {});
    } catch { /* peer offline */ }
  });
  await Promise.allSettled(reps);
  if (confirmations >= 2) {
    commitIndex = entry.index;
    log_msg('Committed stroke #' + entry.index + ' confirmations=' + confirmations);
    try { await axios.post(GATEWAY_URL + '/broadcast', stroke, { timeout: 200 }); }
    catch { /* gateway unreachable */ }
    return res.json({ ok: true, index: entry.index, commitIndex });
  }
  return res.status(503).json({ error: 'no_majority' });
});

// GET /sync-log
app.get('/sync-log', (req, res) => {
  const from    = parseInt(req.query.from || '0', 10);
  const entries = raftLog.filter(e => e.index >= from && e.index <= commitIndex);
  res.json({ entries, commitIndex });
});

// START
app.listen(PORT, () => {
  log_msg('Replica ' + REPLICA_ID + ' started on port ' + PORT);
  resetElectionTimer();
});
module.exports = { app };