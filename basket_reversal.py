"""
basket_reversal.py — Estrategia Fade Armónica v3

LÓGICA:
  Detección IDÉNTICA a basket.py (mismos filtros, mismo consenso FULL, mismo
  DIVERGENCE_MAX). La única diferencia: en vez de entrar al lado del gap,
  entramos al lado CONTRARIO.

  Cuando basket detectaría señal UP en SOL (SOL-UP está barato, gap >= 10bp):
    basket  → compra SOL-UP  @ ~0.65–0.70
    reversal → compra SOL-DOWN @ ~0.30–0.35  ← payout ~2.3x si resuelve DOWN

  Condiciones de entrada (iguales a basket + filtro de precio reversal):
    1. Consenso FULL: ambos pares > 0.80 en el lado del gap
    2. gap >= DIVERGENCE_THRESHOLD (0.10) y <= DIVERGENCE_MAX (0.14)
    3. Precio del lado contrario (reversal) entre 0.30 y 0.35
    4. Ventana de tiempo: 60s–85s restantes
"""

import asyncio
import os
import sys
import time
import json
import csv
import logging
import threading
from collections import deque
from datetime import datetime

from strategy_core_prod import (
    find_active_market,
    get_order_book_metrics,
    seconds_remaining,
    place_taker_buy,
    approve_conditional_token,
    get_usdc_balance,
)
from ws_client import MarketDataWS

mws = MarketDataWS()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("reversal")

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ═══════════════════════════════════════════════════════
#  PARÁMETROS — idénticos a basket.py salvo los de precio
# ═══════════════════════════════════════════════════════
POLL_INTERVAL        = 0.2
DIVERGENCE_THRESHOLD = 0.10    # gap mínimo 10bp (igual a basket pero más estricto para reversal)
DIVERGENCE_MAX       = 0.14    # gap máximo 14bp — igual que basket, descarta mercados rotos
WAKE_UP_SECS         = 90
ENTRY_WINDOW_SECS    = 85
ENTRY_OPEN_SECS      = 60
ENTRY_CLOSE_SECS     = 30

CAPITAL_TOTAL        = 100.0
ENTRY_USD            = 1.75    # $1.75 fijo — garantiza ≥5 shares con ask ≤0.35

RESOLVED_UP_THRESH   = 0.98
RESOLVED_DN_THRESH   = 0.02

# Consenso IDÉNTICO a basket — FULL: ambos pares > 0.80 en el lado del gap
CONSENSUS_FULL       = 0.80
CONSENSUS_SOFT       = 0.80    # igual que basket

# Precio del lado CONTRARIO (reversal) — aquí es donde diferimos de basket
ENTRY_MIN_PRICE      = 0.30    # piso: payout máximo razonable
ENTRY_MAX_PRICE      = 0.35    # techo: confirma que el gap es suficiente

MID_HISTORY_SIZE     = 3

LOG_FILE   = os.environ.get("LOG_FILE",   "/data/reversal_log.json")
CSV_FILE   = os.environ.get("CSV_FILE",   "/data/reversal_trades.csv")
STATE_FILE = os.environ.get("STATE_FILE", "/data/reversal_state.json")

# ═══════════════════════════════════════════════════════
#  ESTADO
# ═══════════════════════════════════════════════════════
SYMBOLS = ["ETH", "SOL", "BTC"]

markets = {
    s: {
        "info":      None,
        "up_bid":    0.0, "up_ask": 0.0, "up_mid": 0.0,
        "dn_bid":    0.0, "dn_ask": 0.0, "dn_mid": 0.0,
        "time_left": "N/A",
        "error":     None,
    }
    for s in SYMBOLS
}

mid_history: dict[str, deque] = {
    s: deque(maxlen=MID_HISTORY_SIZE) for s in SYMBOLS
}

bt = {
    "harm_up":            0.0,
    "harm_dn":            0.0,
    "signal_asset":       None,
    "signal_side":        None,
    "reversal_side":      None,
    "signal_div":         0.0,
    "entry_window":       False,
    "position":           None,
    "traded_this_cycle":  False,
    "capital":            CAPITAL_TOTAL,
    "total_pnl":          0.0,
    "peak_capital":       CAPITAL_TOTAL,
    "max_drawdown":       0.0,
    "wins":               0,
    "losses":             0,
    "consensus":          "NONE",
    "skipped":            0,
    "trades":             [],
    "cycle":              0,
    "phase":              "DURMIENDO",
    "next_wake":          "N/A",
}

recent_events = deque(maxlen=50)

CSV_COLUMNS = [
    "trade_id", "entry_ts", "exit_ts", "duration_s",
    "asset", "gap_side", "reversal_side", "consensus",
    "entry_ask", "entry_bid", "entry_mid", "entry_usd", "shares",
    "secs_left_entry", "harm_entry", "gap_pts",
    "peer1_sym", "peer1_side_mid", "peer1_opp_mid",
    "peer2_sym", "peer2_side_mid", "peer2_opp_mid",
    "exit_type", "exit_price", "resolved", "binary_win",
    "pnl_usd", "pnl_pct_entry", "max_possible_win", "outcome",
    "capital_before", "capital_after", "cumulative_pnl", "trade_number",
]


# ═══════════════════════════════════════════════════════
#  UTILIDADES
# ═══════════════════════════════════════════════════════

def log_event(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    recent_events.append(f"[{ts}] {msg}")
    log.info(msg)


def harmonic_mean(values: list) -> float:
    if not values or any(v <= 0 for v in values):
        return 0.0
    return len(values) / sum(1.0 / v for v in values)


def find_cheapest(mids: dict, h_avg: float):
    """Igual que basket — retorna el activo con mayor desviación negativa."""
    if h_avg == 0:
        return None, 0.0
    cheapest_name, cheapest_diff = None, 0.0
    for name, mid in mids.items():
        diff = mid - h_avg
        if diff < cheapest_diff:
            cheapest_diff, cheapest_name = diff, name
    return cheapest_name, cheapest_diff


def min_secs_remaining() -> float | None:
    result = None
    for sym in SYMBOLS:
        info = markets[sym]["info"]
        if info:
            secs = seconds_remaining(info)
            if secs is not None:
                result = secs if result is None else min(result, secs)
    return result


def update_drawdown():
    cap = bt["capital"]
    if cap > bt["peak_capital"]:
        bt["peak_capital"] = cap
    dd = bt["peak_capital"] - cap
    if dd > bt["max_drawdown"]:
        bt["max_drawdown"] = dd


# ═══════════════════════════════════════════════════════
#  RESOLUCIÓN FALLBACK — CLOB
# ═══════════════════════════════════════════════════════

def resolve_from_clob_history(sym: str) -> str:
    history = list(mid_history[sym])
    if not history:
        log_event(f"FALLBACK {sym}: sin historial CLOB — LOSS conservador")
        return "_UNKNOWN"
    avg = sum(history) / len(history)
    log_event(
        f"FALLBACK {sym}: up_mid_avg={avg:.4f} "
        f"({[round(v,4) for v in history]})"
    )
    if avg > 0.5:
        return "UP"
    elif avg < 0.5:
        return "DOWN"
    else:
        log_event(f"FALLBACK {sym}: empate técnico — LOSS conservador")
        return "_UNKNOWN"


# ═══════════════════════════════════════════════════════
#  ESCRITURA DE ESTADO
# ═══════════════════════════════════════════════════════

def write_state():
    total = bt["wins"] + bt["losses"]
    wr    = (bt["wins"] / total * 100) if total > 0 else 0.0
    roi   = (bt["capital"] - CAPITAL_TOTAL) / CAPITAL_TOTAL * 100

    state = {
        "strategy":       "REVERSAL",
        "ts":             datetime.now().isoformat(),
        "phase":          bt["phase"],
        "cycle":          bt["cycle"],
        "capital":        round(bt["capital"], 4),
        "total_pnl":      round(bt["total_pnl"], 4),
        "roi":            round(roi, 2),
        "peak_capital":   round(bt["peak_capital"], 4),
        "max_drawdown":   round(bt["max_drawdown"], 4),
        "wins":           bt["wins"],
        "losses":         bt["losses"],
        "win_rate":       round(wr, 1),
        "skipped":        bt["skipped"],
        "consensus":      bt["consensus"],
        "entry_window":   bt["entry_window"],
        "next_wake":      bt["next_wake"],
        "harm_up":        round(bt["harm_up"], 4),
        "harm_dn":        round(bt["harm_dn"], 4),
        "signal_asset":   bt["signal_asset"],
        "signal_side":    bt["signal_side"],
        "reversal_side":  bt["reversal_side"],
        "signal_div":     round(bt["signal_div"], 4),
        "position":       bt["position"],
        "markets": {
            sym: {
                "up_mid":    round(markets[sym]["up_mid"], 4),
                "dn_mid":    round(markets[sym]["dn_mid"], 4),
                "up_ask":    round(markets[sym]["up_ask"], 4),
                "dn_ask":    round(markets[sym]["dn_ask"], 4),
                "time_left": markets[sym]["time_left"],
                "error":     markets[sym]["error"],
            }
            for sym in SYMBOLS
        },
        "events":        list(recent_events)[-30:],
        "recent_trades": bt["trades"][-10:],
    }
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        log.warning(f"write_state error: {e}")


# ═══════════════════════════════════════════════════════
#  RESTAURAR ESTADO DESDE CSV
# ═══════════════════════════════════════════════════════

def sincronizar_capital_clob():
    balance = get_usdc_balance()
    if balance is not None and balance > 0:
        bt["capital"] = round(balance, 4)
        log_event(f"Capital sincronizado desde CLOB: ${bt['capital']:.4f}")
    else:
        log_event("No se pudo leer saldo CLOB — usando capital del CSV o default")


def restore_state_from_csv():
    if not os.path.isfile(CSV_FILE):
        log.info("No hay CSV previo — iniciando desde cero.")
        return
    try:
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        if not rows:
            return
        last = rows[-1]
        bt["capital"]   = float(last["capital_after"])
        bt["total_pnl"] = float(last["cumulative_pnl"])
        bt["wins"]      = sum(1 for r in rows if r["outcome"] == "WIN")
        bt["losses"]    = sum(1 for r in rows if r["outcome"] == "LOSS")
        bt["trades"]    = [dict(r) for r in rows]
        peak = CAPITAL_TOTAL
        for r in rows:
            cap = float(r["capital_after"])
            if cap > peak:
                peak = cap
            dd = peak - cap
            if dd > bt["max_drawdown"]:
                bt["max_drawdown"] = dd
        bt["peak_capital"] = peak
        total = bt["wins"] + bt["losses"]
        log.info(
            f"Estado restaurado — {total} trades | Capital: ${bt['capital']:.4f} | "
            f"PnL: ${bt['total_pnl']:+.4f} | W:{bt['wins']} L:{bt['losses']}"
        )
    except Exception as e:
        log.warning(f"No se pudo restaurar estado: {e}")


# ═══════════════════════════════════════════════════════
#  DISCOVERY Y FETCH
# ═══════════════════════════════════════════════════════

async def discover_all():
    loop = asyncio.get_event_loop()
    for sym in SYMBOLS:
        try:
            info = await loop.run_in_executor(None, find_active_market, sym)
            if info:
                markets[sym]["info"]  = info
                markets[sym]["error"] = None
                mid_history[sym].clear()
                log_event(f"{sym}: mercado → {info.get('question','')[:50]}")
            else:
                markets[sym]["info"]  = None
                markets[sym]["error"] = "sin mercado activo"
        except Exception as e:
            markets[sym]["info"]  = None
            markets[sym]["error"] = str(e)

    token_ids = []
    for sym in SYMBOLS:
        info = markets[sym]["info"]
        if info:
            token_ids.append(info["up_token_id"])
            token_ids.append(info["down_token_id"])
    if token_ids:
        mws.unsubscribe()
        mws.subscribe(token_ids)
        log_event(f"WS suscrito a {len(token_ids)} tokens")

    bt["traded_this_cycle"] = False
    write_state()


def _calc_mid(bid, ask):
    if bid > 0 and ask > 0:
        return round((bid + ask) / 2, 4)
    return round(bid or ask, 4)


def fetch_all_from_ws():
    for sym in SYMBOLS:
        info = markets[sym]["info"]
        if not info:
            continue
        up_m = mws.get_metrics(info["up_token_id"])
        dn_m = mws.get_metrics(info["down_token_id"])

        if not up_m or not dn_m:
            try:
                up_m, _ = get_order_book_metrics(info["up_token_id"])
                dn_m, _ = get_order_book_metrics(info["down_token_id"])
            except Exception:
                pass

        if up_m and dn_m:
            markets[sym]["up_bid"] = up_m["best_bid"]
            markets[sym]["up_ask"] = up_m["best_ask"]
            markets[sym]["dn_bid"] = dn_m["best_bid"]
            markets[sym]["dn_ask"] = dn_m["best_ask"]

            up_mid = _calc_mid(up_m["best_bid"], up_m["best_ask"])
            markets[sym]["up_mid"] = up_mid
            markets[sym]["dn_mid"] = _calc_mid(dn_m["best_bid"], dn_m["best_ask"])

            if up_mid > 0:
                mid_history[sym].append(up_mid)

            secs = seconds_remaining(info)
            if secs is not None:
                markets[sym]["time_left"] = f"{int(secs)}s"
                if secs <= 0:
                    markets[sym]["info"] = None
            else:
                markets[sym]["time_left"] = "N/A"
            markets[sym]["error"] = None
        else:
            markets[sym]["error"] = "sin datos WS"


# ═══════════════════════════════════════════════════════
#  SEÑALES — IDÉNTICO A BASKET, solo agrega reversal_side
# ═══════════════════════════════════════════════════════

def compute_signals():
    def normalized_up(s):
        mid = markets[s]["up_mid"]
        if mid >= RESOLVED_UP_THRESH: return 1.0
        if mid <= RESOLVED_DN_THRESH: return 0.0
        return mid

    def normalized_dn(s):
        mid = markets[s]["dn_mid"]
        if mid >= RESOLVED_UP_THRESH: return 1.0
        if mid <= RESOLVED_DN_THRESH: return 0.0
        return mid

    up_mids = {s: normalized_up(s) for s in SYMBOLS if markets[s]["up_mid"] > 0}
    dn_mids = {s: normalized_dn(s) for s in SYMBOLS if markets[s]["dn_mid"] > 0}

    if len(up_mids) < 2:
        bt["signal_asset"] = None
        return

    harm_up = harmonic_mean(list(up_mids.values()))
    harm_dn = harmonic_mean(list(dn_mids.values()))
    bt["harm_up"] = harm_up
    bt["harm_dn"] = harm_dn

    # find_cheapest idéntico a basket
    cheapest_up, div_up = find_cheapest(up_mids, harm_up)
    cheapest_dn, div_dn = find_cheapest(dn_mids, harm_dn)

    if abs(div_up) >= abs(div_dn) and cheapest_up:
        bt["signal_asset"]  = cheapest_up
        bt["signal_side"]   = "UP"      # lado con gap (el "caro" en basket, aquí también)
        bt["reversal_side"] = "DOWN"    # lado que COMPRAMOS nosotros
        bt["signal_div"]    = div_up
    elif cheapest_dn:
        bt["signal_asset"]  = cheapest_dn
        bt["signal_side"]   = "DOWN"
        bt["reversal_side"] = "UP"
        bt["signal_div"]    = div_dn
    else:
        bt["signal_asset"]  = None
        bt["reversal_side"] = None
        return

    # Consenso IDÉNTICO a basket — FULL requiere ambos pares > 0.80
    peers = [s for s in SYMBOLS if s != bt["signal_asset"]]
    if bt["signal_side"] == "UP":
        peer_vals = [markets[p]["up_mid"] for p in peers if markets[p]["up_mid"] > 0]
    else:
        peer_vals = [markets[p]["dn_mid"] for p in peers if markets[p]["dn_mid"] > 0]

    if len(peer_vals) == 2 and all(v > CONSENSUS_FULL for v in peer_vals):
        bt["consensus"] = "FULL"
    elif len(peer_vals) >= 1 and sum(1 for v in peer_vals if v > CONSENSUS_SOFT) >= 1:
        bt["consensus"] = "SOFT"
    else:
        bt["consensus"] = "NONE"


# ═══════════════════════════════════════════════════════
#  ENTRADA CONTRARIAN
# ═══════════════════════════════════════════════════════

def check_entry():
    if bt["traded_this_cycle"]:
        return
    if not bt["entry_window"]:
        return

    # Consenso FULL igual que basket
    if bt["consensus"] != "FULL":
        bt["skipped"] += 1
        return

    if not bt["signal_asset"] or not bt["reversal_side"]:
        return

    div_abs = abs(bt["signal_div"])

    # Gap mínimo — debe ser al menos 10bp
    if div_abs < DIVERGENCE_THRESHOLD:
        return

    # Sin gap máximo — gaps grandes (12, 14, 20bp...) son bienvenidos en reversal

    sym      = bt["signal_asset"]
    gap_side = bt["signal_side"]
    reversal = bt["reversal_side"]

    # Leemos precio del lado CONTRARIO (reversal), no del lado del gap
    if reversal == "UP":
        entry_ask = markets[sym]["up_ask"]
        entry_bid = markets[sym]["up_bid"]
        entry_mid = markets[sym]["up_mid"]
    else:
        entry_ask = markets[sym]["dn_ask"]
        entry_bid = markets[sym]["dn_bid"]
        entry_mid = markets[sym]["dn_mid"]

    if entry_ask <= 0 or entry_ask >= 1:
        return

    # Evitar activos ya resueltos — igual que basket
    up_mid = markets[sym]["up_mid"]
    dn_mid = markets[sym]["dn_mid"]
    if up_mid >= RESOLVED_UP_THRESH or up_mid <= RESOLVED_DN_THRESH or \
       dn_mid >= RESOLVED_UP_THRESH or dn_mid <= RESOLVED_DN_THRESH:
        log_event(f"SKIP {reversal} {sym} — activo ya resuelto (up={up_mid:.4f} dn={dn_mid:.4f})")
        bt["skipped"] += 1
        return

    # Filtro de precio del lado reversal: debe estar entre 0.30 y 0.35
    if entry_ask < ENTRY_MIN_PRICE:
        log_event(f"SKIP {reversal} {sym} — ask={entry_ask:.4f} bajo mínimo {ENTRY_MIN_PRICE}")
        bt["skipped"] += 1
        return

    if entry_ask > ENTRY_MAX_PRICE:
        log_event(f"SKIP {reversal} {sym} — ask={entry_ask:.4f} supera máximo {ENTRY_MAX_PRICE}")
        bt["skipped"] += 1
        return

    if bt["capital"] < ENTRY_USD:
        log_event(f"SKIP — capital insuficiente (${bt['capital']:.2f} < ${ENTRY_USD:.2f})")
        return

    shares_req     = round(ENTRY_USD / entry_ask, 2)
    secs           = min_secs_remaining() or 0
    peers          = [s for s in SYMBOLS if s != sym]
    peer_snaps     = {p: {"up_mid": markets[p]["up_mid"], "dn_mid": markets[p]["dn_mid"]} for p in peers}
    harm_entry     = bt["harm_up"] if gap_side == "UP" else bt["harm_dn"]
    gap_entry      = bt["signal_div"]
    capital_before = bt["capital"]
    token_id       = markets[sym]["info"]["up_token_id"] if reversal == "UP" else markets[sym]["info"]["down_token_id"]

    log_event(f"COMPRANDO {reversal} {sym} @ ask={entry_ask:.4f} | {shares_req} shares... (gap {gap_side} {gap_entry*100:+.1f}bp)")
    result = place_taker_buy(token_id, shares_req, entry_ask)

    if not result["success"] or result["shares_filled"] < shares_req * 0.5:
        log_event(f"FALLO compra {reversal} {sym}: {result['error']}")
        return

    shares_filled = result["shares_filled"]
    fill_price    = result.get("fill_price", entry_ask)
    usd_spent     = round(shares_filled * fill_price, 4)

    approve_conditional_token(token_id)

    bt["capital"]           -= usd_spent
    bt["traded_this_cycle"]  = True

    bt["position"] = {
        "asset":           sym,
        "side":            reversal,
        "gap_side":        gap_side,
        "token_id":        token_id,
        "entry_price":     fill_price,
        "entry_bid":       entry_bid,
        "entry_mid":       entry_mid,
        "entry_usd":       usd_spent,
        "shares":          shares_filled,
        "secs_left_entry": secs,
        "harm_entry":      harm_entry,
        "gap_entry":       gap_entry,
        "entry_ts":        datetime.now().isoformat(),
        "consensus_entry": bt["consensus"],
        "peer_snaps":      peer_snaps,
        "capital_before":  capital_before,
    }

    log_event(
        f"REVERSAL {reversal} {sym} @ fill={fill_price:.4f} | "
        f"gap {gap_side}={gap_entry*100:+.1f}bp | "
        f"harm={harm_entry:.4f} | shares={shares_filled} | "
        f"capital=${bt['capital']:.2f}"
    )
    write_state()


# ═══════════════════════════════════════════════════════
#  RESOLUCIÓN
# ═══════════════════════════════════════════════════════

def _apply_resolution(pos, resolved):
    sym  = pos["asset"]
    side = pos["side"]
    if resolved == side:
        pnl     = round(pos["shares"] - pos["entry_usd"], 6)
        outcome = "WIN"
        bt["wins"] += 1
    else:
        pnl     = -pos["entry_usd"]
        outcome = "LOSS"
        bt["losses"] += 1

    time.sleep(2.0)
    balance_real = get_usdc_balance()
    if balance_real is not None and balance_real > 0:
        bt["capital"]   = round(balance_real, 4)
        bt["total_pnl"] = round(bt["capital"] - CAPITAL_TOTAL, 4)
    else:
        bt["capital"]   += pos["entry_usd"] + pnl
        bt["total_pnl"] += pnl

    update_drawdown()
    log_event(
        f"RESOLUCIÓN {outcome} {side} {sym} → {resolved} | "
        f"PnL≈${pnl:+.4f} | Capital=${bt['capital']:.4f}"
    )
    _record_trade(pos, resolved, outcome, pnl)
    write_state()


def check_resolution():
    pos = bt["position"]
    if not pos:
        return
    sym    = pos["asset"]
    up_mid = markets[sym]["up_mid"]

    resolved = None
    if up_mid >= RESOLVED_UP_THRESH:
        resolved = "UP"
    elif up_mid <= RESOLVED_DN_THRESH:
        resolved = "DOWN"

    if resolved:
        _apply_resolution(pos, resolved)
        bt["position"] = None
        return

    if markets[sym]["info"] is None:
        resolved = resolve_from_clob_history(sym)
        if resolved == "_UNKNOWN":
            pnl = -pos["entry_usd"]
            bt["capital"]   += pos["entry_usd"] + pnl
            bt["total_pnl"] += pnl
            bt["losses"]    += 1
            update_drawdown()
            log_event(f"FALLBACK {sym}: LOSS conservador")
            _record_trade(pos, "UNKNOWN", "LOSS", pnl)
        else:
            _apply_resolution(pos, resolved)
        bt["position"] = None
        write_state()


# ═══════════════════════════════════════════════════════
#  PERSISTENCIA
# ═══════════════════════════════════════════════════════

def _build_trade_record(pos, exit_type, exit_price, resolved, outcome, pnl):
    exit_ts      = datetime.now().isoformat()
    duration_s   = round(
        (datetime.fromisoformat(exit_ts) - datetime.fromisoformat(pos["entry_ts"])).total_seconds(), 1
    )
    trade_number = bt["wins"] + bt["losses"]
    peers        = [s for s in SYMBOLS if s != pos["asset"]]
    peer_snaps   = pos.get("peer_snaps", {})

    def peer_mids(p):
        snap = peer_snaps.get(p, {})
        side = pos.get("gap_side", pos["side"])
        if side == "UP":
            return snap.get("up_mid", 0.0), snap.get("dn_mid", 0.0)
        return snap.get("dn_mid", 0.0), snap.get("up_mid", 0.0)

    p1_sm, p1_om = peer_mids(peers[0]) if peers else (0.0, 0.0)
    p2_sm, p2_om = peer_mids(peers[1]) if len(peers) > 1 else (0.0, 0.0)

    max_win    = round((1.0 - pos["entry_price"]) / pos["entry_price"] * pos["entry_usd"], 6)
    binary_win = (1 if outcome == "WIN" and exit_type == "RESOLUTION" else
                  0 if outcome == "LOSS" and exit_type == "RESOLUTION" else -1)

    return {
        "trade_id":         f"R{trade_number:04d}",
        "entry_ts":         pos["entry_ts"],
        "exit_ts":          exit_ts,
        "duration_s":       duration_s,
        "asset":            pos["asset"],
        "gap_side":         pos.get("gap_side", ""),
        "reversal_side":    pos["side"],
        "consensus":        pos["consensus_entry"],
        "entry_ask":        round(pos["entry_price"], 6),
        "entry_bid":        round(pos["entry_bid"], 6),
        "entry_mid":        round(pos["entry_mid"], 6),
        "entry_usd":        round(pos["entry_usd"], 4),
        "shares":           round(pos["shares"], 6),
        "secs_left_entry":  round(pos["secs_left_entry"], 1),
        "harm_entry":       round(pos["harm_entry"], 6),
        "gap_pts":          round(pos["gap_entry"] * 100, 2),
        "peer1_sym":        peers[0] if peers else "",
        "peer1_side_mid":   round(p1_sm, 6),
        "peer1_opp_mid":    round(p1_om, 6),
        "peer2_sym":        peers[1] if len(peers) > 1 else "",
        "peer2_side_mid":   round(p2_sm, 6),
        "peer2_opp_mid":    round(p2_om, 6),
        "exit_type":        exit_type,
        "exit_price":       round(exit_price, 6),
        "resolved":         resolved or "",
        "binary_win":       binary_win,
        "pnl_usd":          round(pnl, 6),
        "pnl_pct_entry":    round(pnl / pos["entry_usd"] * 100, 2),
        "max_possible_win": max_win,
        "outcome":          outcome,
        "capital_before":   round(pos["capital_before"], 4),
        "capital_after":    round(bt["capital"], 4),
        "cumulative_pnl":   round(bt["total_pnl"], 6),
        "trade_number":     trade_number,
    }


def _save_csv(record: dict):
    exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if not exists:
            writer.writeheader()
        writer.writerow(record)


def _record_trade(pos, resolved, outcome, pnl):
    exit_price = 1.0 if resolved == pos["side"] else 0.0
    record = _build_trade_record(pos, "RESOLUTION", exit_price, resolved, outcome, pnl)
    bt["trades"].append(record)
    _save_csv(record)
    _save_log()


def _save_log():
    total = bt["wins"] + bt["losses"]
    with open(LOG_FILE, "w") as f:
        json.dump({
            "strategy": "REVERSAL",
            "summary": {
                "capital_inicial":    CAPITAL_TOTAL,
                "capital_actual":     round(bt["capital"], 4),
                "total_pnl_usd":      round(bt["total_pnl"], 4),
                "roi_pct":            round((bt["capital"] - CAPITAL_TOTAL) / CAPITAL_TOTAL * 100, 2),
                "max_drawdown":       round(bt["max_drawdown"], 4),
                "wins":               bt["wins"],
                "losses":             bt["losses"],
                "win_rate":           round(bt["wins"] / total * 100, 1) if total else 0,
                "skipped":            bt["skipped"],
                "entry_usd":          ENTRY_USD,
                "divergence_threshold": DIVERGENCE_THRESHOLD * 100,
                "divergence_max":     DIVERGENCE_MAX * 100,
            },
            "trades": bt["trades"],
        }, f, indent=2)


# ═══════════════════════════════════════════════════════
#  LOOP PRINCIPAL
# ═══════════════════════════════════════════════════════

async def main_loop():
    log_event("basket_reversal.py iniciado — REVERSIÓN ARMÓNICA v3")
    log_event(f"Capital: ${CAPITAL_TOTAL:.0f} | Entrada: ${ENTRY_USD:.2f} fijo")
    log_event(f"Gap: {DIVERGENCE_THRESHOLD*100:.0f}–{DIVERGENCE_MAX*100:.0f}bp | Precio reversal: {ENTRY_MIN_PRICE}–{ENTRY_MAX_PRICE} | Ventana {ENTRY_OPEN_SECS}s–{ENTRY_WINDOW_SECS}s")

    restore_state_from_csv()
    sincronizar_capital_clob()

    bt["phase"] = "ACTIVO"
    write_state()
    await discover_all()

    while True:
        try:
            secs = min_secs_remaining()

            if secs is not None and secs > WAKE_UP_SECS and not bt["position"]:
                sleep_dur = secs - WAKE_UP_SECS
                wake_at   = datetime.fromtimestamp(time.time() + sleep_dur).strftime("%H:%M:%S")
                bt["phase"]        = "DURMIENDO"
                bt["entry_window"] = False
                slept = 0
                while slept < sleep_dur:
                    chunk = min(5.0, sleep_dur - slept)
                    await asyncio.sleep(chunk)
                    slept += chunk
                    bt["next_wake"] = f"{wake_at} (en {int(max(0, sleep_dur - slept))}s)"
                    write_state()
                bt["phase"] = "ACTIVO"
                log_event(f"Despertando — ~{WAKE_UP_SECS}s restantes")
                await discover_all()
                continue

            bt["phase"] = "ACTIVO"
            bt["cycle"] += 1
            fetch_all_from_ws()

            secs = min_secs_remaining()
            bt["entry_window"] = (
                secs is not None and
                ENTRY_CLOSE_SECS < secs <= ENTRY_WINDOW_SECS and
                secs >= ENTRY_OPEN_SECS
            )

            if bt["position"]:
                check_resolution()

            if all(markets[s]["info"] is None for s in SYMBOLS):
                if bt["position"]:
                    log_event("Mercado expirado con posición — resolviendo con CLOB...")
                    check_resolution()
                if not bt["position"]:
                    log_event("Ciclo expirado — buscando nuevo ciclo...")
                    await discover_all()
                continue

            if not bt["position"]:
                compute_signals()
                check_entry()

            write_state()

        except Exception as e:
            log_event(f"Error en loop: {e}")
            write_state()

        await asyncio.sleep(POLL_INTERVAL)


# ═══════════════════════════════════════════════════════
#  DASHBOARD EN HILO SECUNDARIO
# ═══════════════════════════════════════════════════════

def run_dashboard():
    import importlib.util
    spec = importlib.util.spec_from_file_location("dashboard", "dashboard.py")
    dash = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dash)
    port = int(os.environ.get("PORT", 5001))
    log.info(f"Dashboard iniciando en puerto {port}")
    dash.app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


# ═══════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("=" * 58)
    log.info("  BASKET REVERSAL — REVERSIÓN ARMÓNICA  v3")
    log.info(f"  Capital: ${CAPITAL_TOTAL:.0f}  |  Entrada: ${ENTRY_USD:.2f} fijo")
    log.info(f"  Gap: {DIVERGENCE_THRESHOLD*100:.0f}–{DIVERGENCE_MAX*100:.0f}bp  |  Precio reversal: {ENTRY_MIN_PRICE}–{ENTRY_MAX_PRICE}")
    log.info(f"  Consenso: FULL (ambos pares > {CONSENSUS_FULL})  |  Ventana: {ENTRY_OPEN_SECS}s–{ENTRY_WINDOW_SECS}s")
    log.info("  PRODUCCION — ORDENES REALES ACTIVAS")
    log.info("=" * 58)
    log.info(f"State -> {STATE_FILE} | Log -> {LOG_FILE}")

    t = threading.Thread(target=run_dashboard, daemon=True)
    t.start()

    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        log.info("Reversal bot detenido.")
        total = bt["wins"] + bt["losses"]
        roi   = (bt["capital"] - CAPITAL_TOTAL) / CAPITAL_TOTAL * 100
        log.info(f"Capital final: ${bt['capital']:.4f}  (ROI: {roi:+.2f}%)")
        log.info(f"P&L total: ${bt['total_pnl']:+.4f}")
        log.info(f"Trades: {total}  (WIN: {bt['wins']}  LOSS: {bt['losses']})")
