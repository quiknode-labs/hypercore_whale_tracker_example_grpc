const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const path = require("path");

const GRPC_ENDPOINT = process.env.GRPC_ENDPOINT; // host:10000
const AUTH_TOKEN = process.env.AUTH_TOKEN;

if (!GRPC_ENDPOINT || !AUTH_TOKEN) {
  console.error('❌ Set GRPC_ENDPOINT="host:10000" and AUTH_TOKEN="token"');
  process.exit(1);
}

const COINS = ["BTC", "ETH"];

// Whale tiers
const TIER_WHALE = 50_000;
const TIER_MEGA = 250_000;
const TIER_LEVI = 1_000_000;

// Rolling window leaderboard
const WINDOW_MS = 30_000;
const PRINT_LEADERBOARD_EVERY_MS = 10_000;
const TOP_N = 5;

const pkgDef = protoLoader.loadSync(path.join(__dirname, "proto/streaming.proto"), {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const hyperliquid = grpc.loadPackageDefinition(pkgDef).hyperliquid;

function md() {
  const m = new grpc.Metadata();
  m.add("x-token", AUTH_TOKEN);
  return m;
}

function toNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : NaN;
}

function fmtUsd(x) {
  if (!Number.isFinite(x)) return "NaN";
  if (x >= 1e9) return `$${(x / 1e9).toFixed(2)}B`;
  if (x >= 1e6) return `$${(x / 1e6).toFixed(2)}M`;
  if (x >= 1e3) return `$${(x / 1e3).toFixed(2)}K`;
  return `$${x.toFixed(2)}`;
}

function whaleTier(notional) {
  if (notional >= TIER_LEVI) return { tag: "🦑 LEVIATHAN", min: TIER_LEVI };
  if (notional >= TIER_MEGA) return { tag: "🐳 MEGA WHALE", min: TIER_MEGA };
  if (notional >= TIER_WHALE) return { tag: "🐋 WHALE", min: TIER_WHALE };
  return null;
}

// Store recent whale prints for a rolling leaderboard
const recent = [];

function pruneOld(now) {
  while (recent.length && now - recent[0].t > WINDOW_MS) recent.shift();
}

function printLeaderboard() {
  const now = Date.now();
  pruneOld(now);

  if (recent.length === 0) return;

  const top = [...recent].sort((a, b) => b.notional - a.notional).slice(0, TOP_N);

  console.log(`\n🏆 Top ${TOP_N} whale prints (last ${Math.floor(WINDOW_MS / 1000)}s)`);
  for (let i = 0; i < top.length; i++) {
    const x = top[i];
    const dir = x.side === "B" ? "🟢 BUY " : "🔴 SELL";
    console.log(
      `${String(i + 1).padStart(2, " ")}  ${fmtUsd(x.notional).padStart(9, " ")}  ${x.coin} ${dir}  px=${x.px} sz=${x.sz}  tid=${x.tid}`
    );
  }
  console.log("");
}

const client = new hyperliquid.Streaming(
  GRPC_ENDPOINT,
  grpc.credentials.createSsl(),
  { "grpc.max_receive_message_length": 100 * 1024 * 1024 }
);

client.Ping({ count: 1 }, md(), (err, res) => {
  if (err) throw err;

  console.log("✅ Ping ok:", res);
  console.log(
    `🔄 Streaming TRADES for ${COINS.join(", ")} | tiers: ${fmtUsd(TIER_WHALE)} / ${fmtUsd(TIER_MEGA)} / ${fmtUsd(TIER_LEVI)} | taker-only`
  );

  const stream = client.StreamData(md());

  stream.on("data", (msg) => {
    if (msg.pong) return; // ignore pongs
    if (!msg.data?.data) return;

    let payload;
    try {
      payload = JSON.parse(msg.data.data);
    } catch {
      return;
    }

    const events = payload.events || [];
    if (!Array.isArray(events) || events.length === 0) return;

    for (const ev of events) {
      if (!Array.isArray(ev) || ev.length < 2) continue;

      const trade = ev[1];
      if (!trade || typeof trade !== "object") continue;

      // Only taker side to avoid double counting
      if (trade.crossed !== true) continue;

      const coin = trade.coin;
      if (!COINS.includes(coin)) continue;

      const px = toNum(trade.px);
      const sz = toNum(trade.sz);
      const notional = px * sz;
      if (!Number.isFinite(notional)) continue;

      const tier = whaleTier(notional);
      if (!tier) continue;

      const side = trade.side;
      const dir = side === "B" ? "🟢 BUY " : "🔴 SELL";
      const liq = trade.liquidation ? " 🚨LIQ" : "";

      console.log(`${tier.tag}${liq}  ${fmtUsd(notional)}  ${coin} ${dir} px=${trade.px} sz=${trade.sz} tid=${trade.tid}`);

      // Keep for leaderboard
      recent.push({
        t: Date.now(),
        coin,
        side,
        px: trade.px,
        sz: trade.sz,
        notional,
        tid: trade.tid,
      });

      pruneOld(Date.now());
    }
  });

  stream.on("error", (e) => console.error("❌ Stream error:", e.code, e.message));
  stream.on("end", () => console.log("Stream ended"));

  stream.write({
    subscribe: {
      stream_type: "TRADES",
      start_block: 0,
      filters: {
        coin: { values: COINS },
      },
    },
  });

  // keepalive ping
  setInterval(() => stream.write({ ping: { timestamp: Date.now() } }), 25000);

  // leaderboard print
  setInterval(printLeaderboard, PRINT_LEADERBOARD_EVERY_MS);

  const cleanup = () => {
    try { stream.end(); } catch {}
    try { client.close(); } catch {}
    process.exit(0);
  };
  process.on("SIGINT", cleanup);
  process.on("SIGTERM", cleanup);
});
