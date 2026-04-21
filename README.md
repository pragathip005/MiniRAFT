# Distributed Real-Time Drawing Board

A distributed collaborative drawing board that uses a simplified **RAFT consensus protocol** to keep all replicas in sync. Multiple users can draw simultaneously, and every stroke is replicated across three replica nodes before being confirmed.

---

## Architecture

```
Browser  ──[WebSocket]──►  Gateway (port 8080)
                               │
                    ┌──────────┼──────────┐
                    ▼          ▼          ▼
               Replica 1   Replica 2  Replica 3
               (port 3001) (port 3002) (port 3003)
```

| Service    | Role                                                                 |
|------------|----------------------------------------------------------------------|
| **Frontend** | Browser canvas UI — draws strokes and receives live updates via WebSocket |
| **Gateway**  | WebSocket server — routes strokes to the leader, broadcasts committed strokes to all browsers |
| **Replica 1/2/3** | RAFT consensus nodes — run leader election, replicate strokes, maintain the commit log |

---

## How It Works

1. User draws on the canvas → browser sends a `stroke` message over WebSocket to the Gateway.
2. Gateway discovers the current leader via `GET /status` on replicas.
3. Gateway forwards the stroke to the leader via `POST /stroke`.
4. Leader replicates the stroke to both followers via `POST /append-entries`.
5. Once a majority (2 of 3) confirms, the leader **commits** the stroke and increments `commitIndex`.
6. Leader calls `POST /broadcast` on the Gateway.
7. Gateway fans the committed stroke out to all connected browser clients over WebSocket.

---

## RAFT Consensus (simplified)

Each replica can be in one of three states:

| State       | Description                                              |
|-------------|----------------------------------------------------------|
| `follower`  | Default state. Resets election timer on every heartbeat. |
| `candidate` | Election timeout fired. Requests votes from peers.       |
| `leader`    | Won majority vote. Sends heartbeats every 150 ms.        |

**Election timeout:** 500–800 ms (randomised to avoid split votes)  
**Heartbeat interval:** 150 ms  
**Majority required:** 2 out of 3 replicas

When a node restarts behind the leader, it calls `GET /sync-log?from=<index>` on the leader to catch up with all missing committed entries.

---

## Project Structure

```
.
├── docker-compose.yml        # Orchestrates all services
├── api-contract.md           # Full API specification
├── docs/
│   └── package-explanation.txt
├── replica/                  # Shared replica source (reference)
│   └── server.js             # Full RAFT implementation
├── replica1/                 # Deployed as container replica1
│   ├── server.js
│   ├── package.json
│   └── dockerfile
├── replica2/                 # Deployed as container replica2
├── replica3/                 # Deployed as container replica3
├── gateway/                  # WebSocket + HTTP gateway
└── frontend/                 # Browser canvas UI
```

---

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/) and Docker Compose

### Run the full stack

```bash
docker-compose up --build
```

This starts:
- Gateway on `http://localhost:8080`
- Replica 1 on `http://localhost:13001`
- Replica 2 on `http://localhost:13002`
- Replica 3 on `http://localhost:13003`

Open your browser at `http://localhost:8080` and start drawing.

### Useful debug commands

```bash
# Check which replica is the leader
curl http://localhost:13001/status
curl http://localhost:13002/status
curl http://localhost:13003/status

# See committed log on a replica
curl "http://localhost:13001/sync-log?from=0"
```

---

## API Overview

Full details are in [api-contract.md](api-contract.md).

### Replica Endpoints

| Method | Path              | Description                              |
|--------|-------------------|------------------------------------------|
| GET    | `/status`         | Current RAFT state (role, term, leader)  |
| POST   | `/request-vote`   | Vote request from a candidate            |
| POST   | `/heartbeat`      | Heartbeat from leader                    |
| POST   | `/append-entries` | Log replication from leader              |
| POST   | `/stroke`         | Submit a stroke (leader only)            |
| GET    | `/sync-log`       | Fetch missing committed entries          |

### Gateway Endpoints

| Method | Path            | Description                                  |
|--------|-----------------|----------------------------------------------|
| POST   | `/broadcast`    | Fan out a committed stroke to all WS clients |
| GET    | `/canvas-state` | Return all committed strokes for page load   |

### WebSocket (`ws://localhost:8080`)

| Direction        | Type           | Description                          |
|------------------|----------------|--------------------------------------|
| Browser → Server | `stroke`       | Send a new drawing stroke            |
| Server → Browser | `stroke`       | Broadcast a committed stroke         |
| Server → Browser | `canvas_state` | Full canvas replay on connection     |
| Server → Browser | `error`        | Error notification (no leader, etc.) |

---

## Environment Variables

Each replica container is configured via `docker-compose.yml`:

| Variable      | Example value                              | Description                    |
|---------------|--------------------------------------------|--------------------------------|
| `REPLICA_ID`  | `1`                                        | Unique ID of this replica      |
| `PORT`        | `3001`                                     | Port the replica listens on    |
| `PEERS`       | `http://replica2:3002,http://replica3:3003` | Comma-separated peer URLs      |
| `GATEWAY_URL` | `http://gateway:8080`                      | Gateway URL for broadcasting   |

---

## Tech Stack

| Layer    | Technology            |
|----------|-----------------------|
| Replicas | Node.js, Express, Axios |
| Gateway  | Node.js, Express, ws  |
| Frontend | HTML5 Canvas, WebSocket |
| Infra    | Docker, Docker Compose  |
