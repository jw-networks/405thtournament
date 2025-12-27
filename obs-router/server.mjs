import express from "express";
import http from "http";
import { WebSocketServer } from "ws";

const EXPORT_CSV_URL =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vRpG2Z5s4KvRw5iYM8AokvwxM6HEE5q1U2WWDOyOfyz3UBrAFnV80t7HVyK9iuA8SNxMS1bo0kj-e2Z/pub?gid=2053858318&single=true&output=csv";

// ---- tunables ----
const PORT = process.env.PORT ? Number(process.env.PORT) : 8787;
// Polling the sheet too frequently isn’t useful; Google may cache anyway.
// 1000–2000ms is a good practical range.
const POLL_MS = process.env.POLL_MS ? Number(process.env.POLL_MS) : 1250;

// Debounce: require the same change to appear twice before “committing” it.
// Helps avoid transient reads / mid-edit states.
const REQUIRE_STABLE_READS = process.env.REQUIRE_STABLE_READS
  ? Number(process.env.REQUIRE_STABLE_READS)
  : 2;

// -------------------

function parseCsvKeyValue(text) {
  // Assumes a 2-column CSV: key,value with a header row.
  // Handles simple quoted values; not a full CSV parser (good enough for your sheet format).
  const lines = text.trim().split(/\r?\n/);
  const out = {};
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    const comma = line.indexOf(",");
    if (comma === -1) continue;
    const k = line.slice(0, comma).replace(/^"|"$/g, "").trim();
    const v = line.slice(comma + 1).replace(/^"|"$/g, "").trim();
    if (k) out[k] = v;
  }
  return out;
}

function nowIso() {
  return new Date().toISOString();
}

const app = express();
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// last committed state (what we broadcast)
let state = {};
// staged candidates for debounce/stability
// { key: { value, count } }
let candidate = {};
let lastFetchOkAt = null;

function broadcast(msgObj) {
  const payload = JSON.stringify(msgObj);
  for (const client of wss.clients) {
    if (client.readyState === client.OPEN) {
      client.send(payload);
    }
  }
}

wss.on("connection", (ws) => {
  // Send full snapshot immediately on connect
  ws.send(
    JSON.stringify({
      op: "snapshot",
      ts: nowIso(),
      state
    })
  );
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    ts: nowIso(),
    lastFetchOkAt,
    keys: Object.keys(state).length
  });
});

// Useful for debugging: get a single key
app.get("/state/:key", (req, res) => {
  const key = req.params.key;
  res.json({
    key,
    value: state[key] ?? null
  });
});

// Useful for debugging: dump everything
app.get("/state", (req, res) => {
  res.json({ state });
});

// OPTIONAL: allow manual override from production tools if you want later
// POST /override { "key": "slot1_cam", "value": "https://vdo.ninja/?view=..." }
app.post("/override", (req, res) => {
  const { key, value } = req.body || {};
  if (!key || typeof key !== "string") return res.status(400).json({ error: "Missing key" });

  const v = (value ?? "").toString();
  const prev = state[key];

  if (prev !== v) {
    state[key] = v;
    broadcast({
      op: "patch",
      ts: nowIso(),
      changes: { [key]: v }
    });
  }

  res.json({ ok: true, key, value: state[key] });
});

async function pollOnce() {
  const url = EXPORT_CSV_URL + (EXPORT_CSV_URL.includes("?") ? "&" : "?") + "t=" + Date.now();
  const res = await fetch(url, {
    cache: "no-store",
    headers: {
      // extra nudge against caches
      "cache-control": "no-cache"
    }
  });

  if (!res.ok) throw new Error(`CSV fetch failed: ${res.status}`);
  const text = await res.text();
  const kv = parseCsvKeyValue(text);
  return kv;
}

function commitChange(key, value) {
  const prev = state[key];
  if (prev === value) return false;
  state[key] = value;
  return true;
}

async function loop() {
  try {
    const kv = await pollOnce();
    lastFetchOkAt = nowIso();

    const changes = {};

    // For every key we see in the CSV, stage/commit changes
    for (const [key, raw] of Object.entries(kv)) {
      const next = (raw ?? "").trim();

      const current = state[key];
      if (current === next) {
        // reset candidate tracking if it matches committed
        delete candidate[key];
        continue;
      }

      // stability logic
      const c = candidate[key];
      if (!c || c.value !== next) {
        candidate[key] = { value: next, count: 1 };
      } else {
        candidate[key].count += 1;
      }

      if (candidate[key].count >= REQUIRE_STABLE_READS) {
        delete candidate[key];
        if (commitChange(key, next)) changes[key] = next;
      }
    }

    // (Optional) If keys disappear from CSV, we keep last-good.
    // That prevents “blank flip” on transient read issues.

    if (Object.keys(changes).length > 0) {
      broadcast({
        op: "patch",
        ts: nowIso(),
        changes
      });
    }
  } catch (err) {
    // Don’t broadcast anything on errors; keep last-good state.
    // You can log if you want:
    // console.error("[poll error]", err);
  } finally {
    setTimeout(loop, POLL_MS);
  }
}

// start
server.listen(PORT, () => {
  console.log(`OBS Feed Router running on http://localhost:${PORT}`);
  console.log(`Polling CSV every ${POLL_MS}ms; stable reads required: ${REQUIRE_STABLE_READS}`);
  loop();
});
