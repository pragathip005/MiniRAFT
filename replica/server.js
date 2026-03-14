// ═══════════════════════════════════════════════════════════════════
// RAFT REPLICA SERVER
// Implements a simplified RAFT consensus node.
//
// Responsibilities of each replica:
// 1. Participate in leader election
// 2. Replicate drawing strokes across replicas
// 3. Maintain a consistent log of strokes
// 4. Commit strokes when majority confirms
// 5. Recover missing log entries after restart
// ═══════════════════════════════════════════════════════════════════

// Express → used to create the HTTP API for RAFT endpoints (e.g. /request-vote, /heartbeat, /append-entries, /stroke)
// Axios   → used for inter-replica communication (e.g. sending vote requests, heartbeats, log replication)
const express = require('express');
const axios   = require('axios');

// Create Express application
const app = express();

// Middleware that allows Express to read JSON bodies
app.use(express.json());

// ENVIRONMENT VARIABLES - injected by docker-compose.yml for each replica container 

// Unique ID of this replica (1, 2, or 3)
const REPLICA_ID  = process.env.REPLICA_ID;

// Port this replica runs on
const PORT        = parseInt(process.env.PORT || '3001', 10);

// List of peer replicas (comma separated)
const PEERS_RAW   = process.env.PEERS || '';

// Gateway service URL (used for broadcasting committed strokes)
const GATEWAY_URL = process.env.GATEWAY_URL || 'http://gateway:8080';

// Convert comma separated peers into an array
// Example: "http://replica2:3002,http://replica3:3003"
const PEERS       = PEERS_RAW ? PEERS_RAW.split(',').map(p => p.trim()) : [];

// ═══════════════════════════════════════════════════════════════════
// RAFT STATE VARIABLES
// These represent the current state of this node
// ═══════════════════════════════════════════════════════════════════

// Current role of the node
// follower → default state
// candidate → during election
// leader → after winning election
let state             = 'follower';

// Current election term number
// Each election increments the term
let currentTerm = 0;

// Which candidate this node voted for in this term
let votedFor = null;

// Log of strokes (replicated across nodes)
let raftLog = [];

// Index of the highest committed log entry
let commitIndex = -1;

// ID of current leader (if known)
let leaderId = null;

// Timer used to trigger elections
let electionTimer = null;

// Interval used for sending heartbeats
let heartbeatInterval = null;

// Utility logging function
function log_msg(msg) {
  console.log(`[R${REPLICA_ID}][term=${currentTerm}] ${msg}`);
}

// ═══════════════════════════════════════════════════════════════════
// ELECTION TIMER
//
// Each follower starts a random timer.
// If no heartbeat arrives before the timer expires,
// the follower assumes the leader is dead and starts election.
// ═══════════════════════════════════════════════════════════════════

function resetElectionTimer() {
  // cancel old timer
  clearTimeout(electionTimer);
  // random timeout between 500-800 ms
  const ms = Math.floor(500 + Math.random() * 300);
  // start election if timer expires
  electionTimer = setTimeout(startElection, ms);
}
// stop election timer (used when node becomes leader)
function stopElectionTimer() {
  clearTimeout(electionTimer);
  electionTimer = null;
}

// ═══════════════════════════════════════════════════════════════════
// STEP DOWN LOGIC
//
// If a node sees a higher term, it must step down.
// This prevents outdated leaders from continuing.
// ═══════════════════════════════════════════════════════════════════

function stepDownIfNeeded(incomingTerm) {

  if (incomingTerm > currentTerm) {

    // update to newer term
    currentTerm = incomingTerm;

    // revert to follower
    state = 'follower';

    votedFor = null;
    leaderId = null;

    stopHeartbeats();

    resetElectionTimer();

    log_msg('Stepped down - higher term seen');

    return true;
  }

  return false;
}

// ═══════════════════════════════════════════════════════════════════
// LEADER ELECTION
//
// Called when election timeout expires.
// Node becomes candidate and asks peers for votes.
// ═══════════════════════════════════════════════════════════════════

async function startElection() {

  state = 'candidate';

  // increment term for new election
  currentTerm = currentTerm + 1;

  // vote for itself
  votedFor = REPLICA_ID;

  leaderId = null;

  let votes = 1;

  log_msg('Election started term=' + currentTerm);


  // send vote requests to peers
  const requests = PEERS.map(async (peer) => {

    try {

      const res = await axios.post(
        peer + '/request-vote',
        {
          term: currentTerm,
          candidateId: REPLICA_ID
        },
        { timeout: 300 }
      );

      if (res.data.voteGranted) {

        log_msg('Vote from ' + peer);

        return true;
      }

      return false;

    } catch {

      // peer may be offline
      return false;
    }
  });

  const results = await Promise.allSettled(requests);
  results.forEach(r => { if (r.status === 'fulfilled' && r.value) votes++; });

  // if state changed during election abort
  if (state !== 'candidate') { log_msg('Election aborted'); return; }

  // majority = 2 out of 3
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

// ═══════════════════════════════════════════════════════════════════
// HEARTBEATS
//
// Leaders send heartbeat every 150ms
// to prevent followers from starting elections.
// ═══════════════════════════════════════════════════════════════════

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

// ═══════════════════════════════════════════════════════════════════
// FOLLOWER SYNC
//
// If follower log is behind leader,
// leader sends missing entries.
// ═══════════════════════════════════════════════════════════════════

async function syncFollower(peerUrl, fromIndex) {
  try {
    const missing = raftLog.filter(e => e.index >= fromIndex && e.index <= commitIndex);
    for (const entry of missing) {
      await axios.post(peerUrl + '/append-entries',
        { term: currentTerm, leaderId: REPLICA_ID, entry, leaderCommit: commitIndex },
        { timeout: 300 }
      );
    }
  } catch { /* sync failed, will retry. sync may fail if peer still restarting */ }
}

// ═══════════════════════════════════════════════════════════════════
// GET /status
//
// Used by gateway to discover leader.
// ═══════════════════════════════════════════════════════════════════
app.get('/status', (req, res) => {
  res.json({ replicaId: REPLICA_ID, state, currentTerm, leaderId,
             logLength: raftLog.length, commitIndex, peers: PEERS });
});

// ═══════════════════════════════════════════════════════════════════
// POST /request-vote
//
// Called by candidates during election.
// ═══════════════════════════════════════════════════════════════════
app.post('/request-vote', (req, res) => {
  const { term, candidateId } = req.body;
  stepDownIfNeeded(term);
  const voteGranted = (term >= currentTerm) &&
                      (votedFor === null || votedFor === candidateId);
  if (voteGranted) { votedFor = candidateId; resetElectionTimer(); }
  res.json({ voteGranted, term: currentTerm });
});

// ═══════════════════════════════════════════════════════════════════
// POST /heartbeat
//
// Sent periodically by leader.
// ═══════════════════════════════════════════════════════════════════
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

// ═══════════════════════════════════════════════════════════════════
// POST /append-entries
//
// Used by leader to replicate log entries.
// ═══════════════════════════════════════════════════════════════════
app.post('/append-entries', (req, res) => {
  const { term, leaderId: incomingLeaderId, entry, leaderCommit } = req.body;
  if (term < currentTerm) return res.json({ success: false, logLength: raftLog.length });
  stepDownIfNeeded(term);
  currentTerm = term;
  state       = 'follower';
  leaderId    = incomingLeaderId;
  resetElectionTimer();
  // append entry to log
  if (entry) {
    const exists = raftLog.some(e => e.index === entry.index && e.term === entry.term);
    if (!exists) { raftLog.push(entry); }
  }
  // update commit index
  if (leaderCommit !== undefined && leaderCommit > commitIndex) {
    commitIndex = Math.min(leaderCommit, raftLog.length - 1);
  }
  res.json({ success: true, logLength: raftLog.length });
});

// ═══════════════════════════════════════════════════════════════════
// POST /stroke
//
// Gateway sends drawing strokes here.
// Only leader accepts it.
// ═══════════════════════════════════════════════════════════════════
app.post('/stroke', async (req, res) => {
  if (state !== 'leader') return res.status(403).json({ error: 'not_leader', leaderId });
  const stroke = req.body;
  const entry  = { index: raftLog.length, term: currentTerm, stroke };
  raftLog.push(entry);
  let confirmations = 1;
  // replicate to followers
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
  // commit if majority confirms
  if (confirmations >= 2) {
    commitIndex = entry.index;
    log_msg('Committed stroke #' + entry.index + ' confirmations=' + confirmations);
    try { await axios.post(GATEWAY_URL + '/broadcast', stroke, { timeout: 200 }); }
    catch { /* gateway unreachable */ }
    return res.json({ ok: true, index: entry.index, commitIndex });
  }
  return res.status(503).json({ error: 'no_majority' });
});

// ═══════════════════════════════════════════════════════════════════
// GET /sync-log
//
// Followers call this when they restart
// to fetch missing committed entries.
// ═══════════════════════════════════════════════════════════════════
app.get('/sync-log', (req, res) => {
  const from    = parseInt(req.query.from || '0', 10);
  const entries = raftLog.filter(e => e.index >= from && e.index <= commitIndex);
  res.json({ entries, commitIndex });
});

// START SERVER
app.listen(PORT, () => {
  log_msg('Replica ' + REPLICA_ID + ' started on port ' + PORT);
  resetElectionTimer();
});

// Export app (useful for testing)
module.exports = { app };