// ═══════════════════════════════════════════════════════════════════
//  GATEWAY — server.js   COMPLETE IMPLEMENTATION
//  - WebSocket server for browsers
//  - Polls replicas every 500ms to find the leader
//  - Forwards strokes from browsers to the leader
//  - Broadcasts committed strokes back to all browsers
// ═══════════════════════════════════════════════════════════════════

const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const axios     = require('axios');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(express.json());

// CORS - allow browser requests
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  next();
});

// Serve the drawing board frontend
const path = require('path');
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT     = process.env.PORT || 8080;
const REPLICAS = [
  process.env.REPLICA1 || 'http://replica1:3001',
  process.env.REPLICA2 || 'http://replica2:3002',
  process.env.REPLICA3 || 'http://replica3:3003',
];

// All connected browser WebSocket clients
const clients = new Set();

// Current known leader URL
let currentLeader = null;

// ── LEADER DISCOVERY ───────────────────────────────────────────────
// Poll all replicas every 500ms, find who is state='leader'
async function discoverLeader() {
  for (const replicaUrl of REPLICAS) {
    try {
      const res = await axios.get(replicaUrl + '/status', { timeout: 400 });
      if (res.data.state === 'leader') {
        if (currentLeader !== replicaUrl) {
          console.log('[Gateway] Leader changed to ' + replicaUrl);
          currentLeader = replicaUrl;
        }
        return;
      }
    } catch { /* replica offline */ }
  }
  // No leader found yet (election in progress)
  currentLeader = null;
}

// Start polling
setInterval(discoverLeader, 500);
discoverLeader(); // run immediately on boot

// ── WEBSOCKET: browser connections ─────────────────────────────────
wss.on('connection', async (ws) => {
  clients.add(ws);
  console.log('[Gateway] Client connected. Total: ' + clients.size);

  // Send existing canvas state to the new client
  try {
    const leaderUrl = currentLeader || REPLICAS[0];
    const res       = await axios.get(leaderUrl + '/sync-log?from=0', { timeout: 1000 });
    const strokes   = res.data.entries.map(e => e.stroke);
    ws.send(JSON.stringify({ type: 'canvas_state', strokes }));
  } catch {
    ws.send(JSON.stringify({ type: 'canvas_state', strokes: [] }));
  }

  // Handle stroke messages from this browser
  ws.on('message', async (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type !== 'stroke') return;

      if (!currentLeader) {
        ws.send(JSON.stringify({ type: 'error', code: 'NO_LEADER',
          message: 'Election in progress — please retry in a moment' }));
        return;
      }

      try {
        await axios.post(currentLeader + '/stroke', msg.stroke, { timeout: 1000 });
        // Note: broadcast is triggered by the replica itself calling POST /broadcast
        // so we don't need to do anything else here
      } catch (err) {
        if (err.response && err.response.status === 403) {
          // Not the leader — re-discover and tell client to retry
          await discoverLeader();
          ws.send(JSON.stringify({ type: 'error', code: 'NO_LEADER',
            message: 'Leader changed — retrying' }));
        } else {
          ws.send(JSON.stringify({ type: 'error', code: 'CLUSTER_DOWN',
            message: 'Could not reach cluster' }));
        }
      }
    } catch {
      ws.send(JSON.stringify({ type: 'error', code: 'INVALID_MSG',
        message: 'Invalid message format' }));
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log('[Gateway] Client disconnected. Total: ' + clients.size);
  });

  ws.on('error', () => clients.delete(ws));
});

// ── POST /broadcast ────────────────────────────────────────────────
// Called by the LEADER replica after committing a stroke.
// Gateway fans it out to every connected browser.
app.post('/broadcast', (req, res) => {
  const stroke  = req.body;
  const message = JSON.stringify({ type: 'stroke', stroke });
  let sent      = 0;

  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
      sent++;
    }
  }

  res.json({ ok: true, clientsNotified: sent });
});

// ── GET /canvas-state ──────────────────────────────────────────────
// Browser can also fetch the full canvas over plain HTTP (on page load)
app.get('/canvas-state', async (req, res) => {
  try {
    const leaderUrl = currentLeader || REPLICAS[0];
    const response  = await axios.get(leaderUrl + '/sync-log?from=0', { timeout: 1000 });
    const strokes   = response.data.entries.map(e => e.stroke);
    res.json({ strokes, count: strokes.length });
  } catch {
    res.json({ strokes: [], count: 0 });
  }
});

// ── GET /status ────────────────────────────────────────────────────
app.get('/status', (req, res) => {
  res.json({ currentLeader, connectedClients: clients.size, replicas: REPLICAS });
});

// ── START ──────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log('[Gateway] Started on port ' + PORT);
  console.log('[Gateway] Replicas: ' + REPLICAS.join(', '));
});