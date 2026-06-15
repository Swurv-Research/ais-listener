import WebSocket from "ws";

const AISSTREAM_KEY = process.env.AISSTREAM_API_KEY;
const INGEST_URL = process.env.INGEST_URL;       // the edge function URL
const INGEST_SECRET = process.env.INGEST_SECRET; // must match x-ingest-secret in Lovable

// Coverage zones — each is [[swLat, swLng], [neLat, neLng]]
const BOUNDING_BOXES = [
  [[58.5, 22.0], [60.7, 30.5]],   // Gulf of Finland / Baltic
  [[49.5, -1.5], [51.5, 2.5]],     // English Channel
  [[51.0, 1.0], [54.0, 5.0]],      // Southern North Sea
  [[51.0, -6.0], [55.0, -2.5]],    // Irish Sea
  [[50.0, -11.0], [55.5, -6.0]],   // West of Ireland / Celtic Sea
];

let buffer = [];

async function flush() {
  if (buffer.length === 0) return;
  const batch = buffer;
  buffer = [];
  try {
    const res = await fetch(INGEST_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-ingest-secret": INGEST_SECRET
      },
      body: JSON.stringify({ positions: batch })
    });
    if (!res.ok) {
      console.error("ingest failed:", res.status, await res.text());
      // optional: re-queue batch on failure
    } else {
      const out = await res.json().catch(() => ({}));
      console.log(`sent ${batch.length}, written ${out.written ?? "?"}`);
    }
  } catch (e) {
    console.error("flush exception:", e.message);
  }
}

setInterval(flush, 2000); // batch every 2 seconds

function connect() {
  const ws = new WebSocket("wss://stream.aisstream.io/v0/stream");

  ws.on("open", () => {
    console.log("connected to AISStream");
    ws.send(JSON.stringify({
  APIKey: AISSTREAM_KEY,
  BoundingBoxes: BOUNDING_BOXES,
  FilterMessageTypes: ["PositionReport"]
}));
  });

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.MessageType !== "PositionReport") return;
      const pr = msg.Message.PositionReport;
      const meta = msg.MetaData || {};
      const mmsi = String(meta.MMSI || pr.UserID || "");
      if (!mmsi) return;

      buffer.push({
        mmsi,
        name: (meta.ShipName || "").trim() || null,
        latitude: pr.Latitude,
        longitude: pr.Longitude,
        heading: (pr.TrueHeading != null && pr.TrueHeading !== 511) ? pr.TrueHeading : null,
        speed: pr.Sog != null ? pr.Sog : null,
        recorded_at: meta.time_utc ? new Date(meta.time_utc).toISOString() : new Date().toISOString()
      });
    } catch (e) { /* ignore malformed */ }
  });

  ws.on("close", () => {
    console.log("disconnected, reconnecting in 3s");
    setTimeout(connect, 3000);
  });

  ws.on("error", (err) => {
    console.error("ws error:", err.message);
    ws.close();
  });
}

connect();
