// Pump.fun Paper Sprint: Any Bot Can Win — an early-catch paper-trading bot.
//
// What the dev window taught me (1,222 universe coins, 24h of pump.fun data):
//   - Peaks come FAST: the median coin peaks 7 slots after creation, and only 3.8%
//     of coins still sit at 2x or more when the window ends. Holding to the end
//     gives the peak back, so this bot never holds: it exits on rules.
//   - By the time the 1%-of-real-reserve buy cap first allows a 0.02 SOL buy, the
//     price is already ~1.30x its creation price, yet the peak after that entry is
//     still a median 2.20x. So: enter as early as the cap allows, but never chase
//     a coin that already ran past 1.20x its creation price — the trace shows buys
//     at ~2x creation are almost always buying the top.
//   - Entry-time tape cannot tell runners from duds: at the cap's first reach the
//     typical coin has only 1-2 trades, and waiting for more buyers or more age
//     selects AGAINST the runners. So the bot takes every coin the filter passes.
//   - The equal-weight baseline only ever trades 5 coins: it never sells, so its 5
//     position slots fill up and never recycle. Exits are what buy breadth, and
//     breadth (the guardrail wants 20+ coins with a >=0.01 SOL round trip) is
//     mandatory. Every exit here is also a slot freed for a fresh coin.
//   - Fees are the enemy of small trades: ~2.5% curve fees plus 105,000 lamports
//     of tx fees per order. Stakes stay at the cap (up to 0.05 SOL) and exits are
//     kept tight so a round trip is never paid for a flat coin.
//
// The rules, in short: buy each coin once, on the first batch its cap reaches
// 0.02 SOL and its price is still within 1.20x of creation; sell the whole
// position on the first of a 1.20x take-profit, a 0.97x stop, a trailing stop
// once armed at 1.50x, or a 200-slot time limit while the position is flat.
// One entry per coin, no pyramiding, no re-entry.
//
// Deterministic by construction: no clock, no environment, no randomness;
// iteration only over insertion-ordered maps fed by the batch stream. The bot
// speaks agent-fun/replay/v2 on stdin/stdout through the vendored SDK shim.

import { runBot } from './vendor/makefun-bot.ts';

const LAMPORTS = 1e9;
const CURVE_FEE = 0.0125; // protocol 95 bps + creator 30 bps, on each side

// --- tuned on the dev window with the kit's own replay engine (~400 settings
// swept; this point sits on a flat plateau, not a sharp peak) ---
const MIN_CAP_SOL = 0.02; // buy when the 1% cap first reaches this many SOL
const STAKE_SOL = 0.05; // SOL per coin, capped by the cap itself
const MAX_ENTRY_MULT = 1.2; // never buy past this multiple of creation price
// Revision 1 (24 Nov, after the first grade-local): TAKE_PROFIT 1.20 -> 1.35.
// The dev window says the median peak after entry is 2.20x, so clipping every
// winner at 1.20x leaves most of the move on the table while the bot still pays
// full fees (~2.5% curve + tx) on every round trip. Letting winners run to
// 1.35x before the take-profit fires should raise the average win size; the
// trailing stop (armed at 1.50x) still protects any coin that runs further.
const TAKE_PROFIT = 1.35; // sell when mark value reaches this multiple of cost
const STOP_LOSS = 0.97; // sell when mark value falls to this multiple of cost
const TRAIL_ARM = 1.5; // arm the trailing stop at this multiple of cost
const TRAIL = 0.7; // once armed, sell below this share of the peak value
// Revision 2 (24 Nov, after the second grade-local): MAX_HOLD_SLOTS 200 -> 120.
// The dev window says the median coin peaks 7 slots after creation, so a coin
// still flat after 120 slots has almost certainly missed its run — but it keeps
// occupying a position slot while fresh coins wait. Cutting the flat-hold limit
// frees slots ~40% sooner for new entries, which is where breadth comes from.
const MAX_HOLD_SLOTS = 120; // sell a flat position after this many slots held
const HOLD_IF_UP = 1.1; // at the time limit, keep holding above this multiple
const SLIP_BPS = 10_000; // wide open: latency must never refuse a fill

// Per-coin bookkeeping, keyed by mint.
const decided = new Set<string>(); // coins already bought once (never re-entered)
const held = new Map<string, { opened: number; peakValue: number; cost: number }>();
const pendingSell = new Set<string>(); // mints with a sell already in flight
const createPrice = new Map<string, number>(); // mint -> vq/vt at its CreateEvent

// Mark-to-market value of a position in SOL, curve + tx fees deducted.
function markValue(state, mint: string, tokens: bigint): number {
  const curve = state.curves.get(mint);
  if (curve === undefined) return 0;
  const vq = Number(curve.virtual_quote_reserves);
  const vt = Number(curve.virtual_token_reserves);
  const t = Number(tokens);
  if (vt + t <= 0) return 0;
  const gross = (t * vq) / (vt + t) / LAMPORTS;
  return gross * (1 - CURVE_FEE) - Number(state.txFeeLamports()) / LAMPORTS;
}

await runBot({
  onBatch(state, batch) {
    const now = batch.slot;
    const orders = [];
    const cap = state.hello.caps.max_orders_per_batch;
    const tx = Number(state.txFeeLamports()) / LAMPORTS;

    // 1) Refresh bookkeeping from this batch's fills.
    for (const f of batch.fills) {
      if (f.client_id.startsWith('_')) continue; // harness closes, not ours
      if (f.side === 'buy' && f.status === 'filled') {
        const h = held.get(f.mint);
        if (h !== undefined) h.cost = Number(f.quote_lamports) / LAMPORTS + tx;
      }
      if (f.side === 'sell') pendingSell.delete(f.mint);
    }
    // Positions the harness closed (complete / flatten) drop out of state.
    for (const mint of [...held.keys()]) {
      if (
        !state.positions.has(mint) &&
        ![...state.pending.values()].some((po) => po.order.mint === mint)
      ) {
        held.delete(mint);
        pendingSell.delete(mint);
      }
    }

    // 2) Exits first: free slots before spending on entries.
    for (const [mint, pos] of state.positions) {
      const h = held.get(mint);
      if (h === undefined || pendingSell.has(mint)) continue;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      const value = markValue(state, mint, pos.token_amount);
      const cost = h.cost > 0 ? h.cost : Number(pos.cost_lamports) / LAMPORTS;
      if (value > h.peakValue) h.peakValue = value;
      let exit = false;
      if (value >= cost * TAKE_PROFIT) exit = true; // take profit
      else if (value <= cost * STOP_LOSS) exit = true; // stop loss
      else if (h.peakValue >= cost * TRAIL_ARM && value < h.peakValue * TRAIL) exit = true; // trailing stop
      else if (now - h.opened > MAX_HOLD_SLOTS && value < cost * HOLD_IF_UP) exit = true; // flat too long
      if (!exit) continue;
      if (orders.length >= cap) break;
      orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: SLIP_BPS });
      pendingSell.add(mint);
    }

    // 3) Entries: the first batch a coin's cap allows a meaningful buy.
    let free = Number(state.freeCashLamports()) / LAMPORTS;
    let slotsLeft = state.hello.caps.max_open_positions - state.committedPositionCount();
    const pendingBuys = new Set(
      [...state.pending.values()].filter((po) => po.order.side === 'buy').map((po) => po.order.mint),
    );
    for (const e of batch.events) {
      if (orders.length >= cap || slotsLeft <= 0) break;
      if (e.event === 'CreateEvent') {
        const vt = Number(e.virtual_token_reserves);
        if (vt > 0) createPrice.set(e.mint, Number(e.virtual_quote_reserves) / vt);
        continue;
      }
      if (e.event !== 'TradeEvent') continue;
      const mint = e.mint;
      if (decided.has(mint) || held.has(mint) || pendingBuys.has(mint)) continue;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      // Don't chase: skip coins that already ran past MAX_ENTRY_MULT of creation.
      const cp = createPrice.get(mint);
      if (cp !== undefined && cp > 0) {
        const priceNow = Number(curve.virtual_quote_reserves) / Number(curve.virtual_token_reserves);
        if (priceNow > cp * MAX_ENTRY_MULT) {
          decided.add(mint); // skip for good: the top is behind us
          continue;
        }
      }
      const capSol = Number(state.maxBuyLamports(mint)) / LAMPORTS;
      if (capSol < MIN_CAP_SOL) continue;
      let amount = Math.min(capSol, STAKE_SOL);
      if (amount > free - tx) amount = free - tx;
      if (amount < MIN_CAP_SOL) continue;
      orders.push({
        side: 'buy',
        mint,
        quote_lamports: BigInt(Math.floor(amount * LAMPORTS)),
        max_slippage_bps: SLIP_BPS,
      });
      decided.add(mint);
      held.set(mint, { opened: now, peakValue: 0, cost: amount + tx });
      pendingBuys.add(mint);
      free -= amount + tx;
      slotsLeft -= 1;
    }
    return orders;
  },
});
