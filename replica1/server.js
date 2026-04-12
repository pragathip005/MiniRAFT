const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

const PORT = process.env.PORT;
let state = 'follower', currentTerm = 0, votedFor = null, log = [], commitIndex = -1, leaderId = null;
const REPLICA_ID = process.env.REPLICA_ID;
const PEERS = process.env.PEERS.split(',');

let electionTimer;
let heartbeatInterval;
let leaderUrl;

function resetElectionTimer() {
  clearTimeout(electionTimer);
  const t = 500 + Math.random() * 300;
  electionTimer = setTimeout(startElection, t);
}

app.get('/status', (req, res) => {
  res.json({ state, currentTerm, leaderId, logLength: log.length, commitIndex });
});

app.post('/request-vote', (req, res) => {
  const { term, candidateId } = req.body;
  if (term > currentTerm) {
    currentTerm = term;
    state = 'follower';
    votedFor = null;
  }
  let voteGranted = false;
  if (term === currentTerm && (votedFor === null || votedFor === candidateId)) {
    voteGranted = true;
    votedFor = candidateId;
    resetElectionTimer();
  }
  res.json({ voteGranted, term: currentTerm });
});

async function startElection() {
  state = 'candidate';
  currentTerm += 1;
  votedFor = REPLICA_ID;
  let votes = 1;
  for (const peer of PEERS) {
    try {
      const res = await axios.post(peer + '/request-vote', { term: currentTerm, candidateId: REPLICA_ID });
      if (res.data.voteGranted) votes++;
    } catch {}
  }
  if (votes >= 2) {
    state = 'leader';
    startHeartbeats();
  } else {
    state = 'follower';
    resetElectionTimer();
  }
}

function startHeartbeats() {
  heartbeatInterval = setInterval(() => {
    if (state !== 'leader') {
      clearInterval(heartbeatInterval);
      return;
    }
    for (const peer of PEERS) {
      axios.post(peer + '/heartbeat', { term: currentTerm, leaderId: REPLICA_ID }).catch(() => {});
    }
  }, 150);
}

app.post('/heartbeat', (req, res) => {
  const { term, leaderId: incomingLeaderId } = req.body;
  if (term >= currentTerm) {
    currentTerm = term;
    state = 'follower';
    leaderId = incomingLeaderId;
    leaderUrl = PEERS.find(p => p.includes(incomingLeaderId));
    resetElectionTimer();
  }
  res.json({});
});

app.post('/append-entries', async (req, res) => {
  const { term, entry, leaderCommit } = req.body;
  if (term < currentTerm) {
    return res.json({ success: false, logLength: log.length });
  }
  currentTerm = term;
  state = 'follower';
  resetElectionTimer();
  if (entry) {
    if (entry.index > log.length) {
      try {
        const res = await axios.get(leaderUrl + '/sync-log?from=' + log.length);
        log.push(...res.data.entries);
      } catch {}
    } else {
      log.push(entry);
    }
  }
  if (leaderCommit > commitIndex) {
    commitIndex = leaderCommit;
  }
  res.json({ success: true, logLength: log.length });
});

app.post('/stroke', async (req, res) => {
  if (state !== 'leader') {
    return res.json({ error: 'not leader', leaderId });
  }
  const entry = { term: currentTerm, index: log.length, stroke: req.body };
  log.push(entry);
  let confirmations = 1;
  for (const peer of PEERS) {
    try {
      const r = await axios.post(peer + '/append-entries', { term: currentTerm, entry, leaderCommit: commitIndex });
      if (r.data.success) confirmations++;
    } catch {}
  }
  if (confirmations >= 2) {
    commitIndex = entry.index;
    axios.post('http://gateway:8080/broadcast', req.body);
  }
  res.json({});
});

app.get('/sync-log', (req, res) => {
  const fromIndex = parseInt(req.query.from || 0);
  res.json({ entries: log.slice(fromIndex), commitIndex });
});

app.listen(PORT, () => {
  resetElectionTimer();
});