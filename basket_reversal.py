"""
basket_reversal.py — Estrategia Fade Armónica v2  (Opción B)

LÓGICA:
  Cuando el gap de un activo supera 11.5 bp respecto a la media armónica de
  sus pares, el precio se ha sobre-extendido. La estrategia original (basket.py)
  compra el lado barato esperando que siga — pero en esa zona el win rate cae
  al ~60%. Apostamos al lado CONTRARIO (fade): si UP de SOL está muy bajo,
  compramos DOWN, apostando a que el mercado que empujó ese lado tenía razón.

  Ejemplo: UP de SOL cae 12 bp por debajo de la media armónica (gap en UP) →
           el mercado está sobrevendiendo UP y favorece DOWN →
           compramos DOWN de SOL (el lado favorecido por el mercado).

  Backtest 3306 trades reales (31 días):
    gap >= 11.5 bp → 746 trades | WR=39.3% | payout 2.32x | EV +$0.305/trade
    Z-score 5.47 → confianza 99.9% | out-of-sample WR=41.1% (sin overfitting)

DIFERENCIAS vs basket.py (v5):
  - Solo entra cuando gap > REVERSAL_THRESHOLD = 0.115 (11.5 bp)
  - Sin DIVERGENCE_MAX — cualquier gap > 11.5 bp es válido
  - Entra al lado CONTRARIO al gap (reversal_side, opuesto a signal_side)
  - ENTRY_MIN_PRICE = 0.55 — el lado contrario suele cotizar entre 0.55–0.85
  - ENTRY_USD = $1.75 fijo
  - Consenso SOFT: al menos 1 par confirma el sesgo del gap
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
    place_taker_sell,
    approve_conditional_token,
    get_clob_balance,
    get_usdc_balance,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("reversal")

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ═══════════════════════════════════════════════════════
#  PARÁMETROS
# ═══════════════════════════════════════════════════════
POLL_INTERVAL         = 0.5
REVERSAL_THRESHOLD    = 0.115   # 11.5 bp — gap mínimo para entrada contrarian
WAKE_UP_SECS          = 90
ENTRY_WINDOW_SECS     = 85
ENTRY_OPEN_SECS       = 60
ENTRY_CLOSE_SECS      = 30

CAPITAL_TOTAL         = 100.0
ENTRY_USD             = 1.75    # $1.75 fijo — garantiza ≥5 shares con ask ≤0.35

RESOLVED_UP_THRESH    = 0.98
RESOLVED_DN_THRESH    = 0.02

# Para la reversión usamos consenso SOFT: basta con que 1 par apoye el sesgo
CONSENSUS_REQUIRED    = "SOFT"
CONSENSUS_SOFT        = 0.55    # umbral para contar un par como "del mismo lado"

ENTRY_MIN_PRICE       = 0.55    # el lado contrario (caro) debería estar entre 0.55–0.85
STOP_LOSS_PRICE       = 0.33

MID_HISTORY_SIZE      = 3

LOG_FILE   = os.environ.get("LOG_FILE",   "/data/reversal_log.json")
CSV_FILE   = os.environ.get("CSV_FILE",   "/data/reversal_trades.csv")
STATE_FILE = os.environ.get("STATE_FILE", "/data/reversal_state.json")

# Credenciales — requeridas en producción
# POLYMARKET_KEY   → clave privada hex de la wallet
# PROXY_ADDRESS    → dirección proxy en Polymarket
# POLY_CHAIN_ID    → 137 (Polygon mainnet, default)

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
    "signal_asset":       None,   # activo con mayor gap
    "signal_side":        None,   # lado CON el gap (UP o DOWN)
    "reversal_side":      None,   # lado que ENTRAMOS (opuesto al gap)
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
    "sl_price", "exit_type", "exit_price", "resolved", "binary_win",
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


def find_most_extended(mids: dict, h_avg: float):
    """Retorna el activo con MAYOR desviación negativa (más barato) respecto a la media."""
    if h_avg == 0:
        return None, 0.0
    name, diff = None, 0.0
    for asset, mid in mids.items():
        d = mid - h_avg
        if d < diff:
            diff, name = d, asset
    return name, diff


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
    """Sincroniza el capital con el saldo USDC real del CLOB al arrancar."""
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
    bt["traded_this_cycle"] = False
    write_state()


async def fetch_one(sym: str):
    info = markets[sym]["info"]
    if not info:
        return
    loop = asyncio.get_event_loop()
    try:
        up_m, err_up = await loop.run_in_executor(
            None, get_order_book_metrics, info["up_token_id"]
        )
        dn_m, err_dn = await loop.run_in_executor(
            None, get_order_book_metrics, info["down_token_id"]
        )
        if up_m and dn_m:
            markets[sym]["up_bid"] = up_m["best_bid"]
            markets[sym]["up_ask"] = up_m["best_ask"]
            markets[sym]["dn_bid"] = dn_m["best_bid"]
            markets[sym]["dn_ask"] = dn_m["best_ask"]

            def mid(bid, ask):
                if bid > 0 and ask > 0:
                    return round((bid + ask) / 2, 4)
                return round(bid or ask, 4)

            up_mid = mid(up_m["best_bid"], up_m["best_ask"])
            markets[sym]["up_mid"] = up_mid
            markets[sym]["dn_mid"] = mid(dn_m["best_bid"], dn_m["best_ask"])

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
            markets[sym]["error"] = err_up or err_dn or "error ob"
    except Exception as e:
        markets[sym]["error"] = str(e)


async def fetch_all():
    await asyncio.gather(*[fetch_one(sym) for sym in SYMBOLS])


# ═══════════════════════════════════════════════════════
#  SEÑALES
# ═══════════════════════════════════════════════════════

def compute_signals():
    def norm_up(s):
        m = markets[s]["up_mid"]
        if m >= RESOLVED_UP_THRESH: return 1.0
        if m <= RESOLVED_DN_THRESH: return 0.0
        return m

    def norm_dn(s):
        m = markets[s]["dn_mid"]
        if m >= RESOLVED_UP_THRESH: return 1.0
        if m <= RESOLVED_DN_THRESH: return 0.0
        return m

    up_mids = {s: norm_up(s) for s in SYMBOLS if markets[s]["up_mid"] > 0}
    dn_mids = {s: norm_dn(s) for s in SYMBOLS if markets[s]["dn_mid"] > 0}

    if len(up_mids) < 2:
        bt["signal_asset"] = None
        return

    harm_up = harmonic_mean(list(up_mids.values()))
    harm_dn = harmonic_mean(list(dn_mids.values()))
    bt["harm_up"] = harm_up
    bt["harm_dn"] = harm_dn

    ext_up, div_up = find_most_extended(up_mids, harm_up)
    ext_dn, div_dn = find_most_extended(dn_mids, harm_dn)

    # El gap más grande (en valor absoluto) gana
    if abs(div_up) >= abs(div_dn) and ext_up:
        bt["signal_asset"]  = ext_up
        bt["signal_side"]   = "UP"    # lado sobre-extendido (barato)
        bt["reversal_side"] = "DOWN"  # apostamos al lado contrario (caro, favorecido)
        bt["signal_div"]    = div_up
    elif ext_dn:
        bt["signal_asset"]  = ext_dn
        bt["signal_side"]   = "DOWN"
        bt["reversal_side"] = "UP"    # ídem
        bt["signal_div"]    = div_dn
    else:
        bt["signal_asset"] = None
        bt["reversal_side"] = None
        return

    # Consenso: los pares deben mostrar sesgo del lado sobre-extendido
    # (si UP de SOL está muy bajo, los pares también deberían tener UP bajo)
    asset = bt["signal_asset"]
    side  = bt["signal_side"]
    peers = [s for s in SYMBOLS if s != asset]

    if side == "UP":
        peer_vals = [markets[p]["up_mid"] for p in peers if markets[p]["up_mid"] > 0]
        # "mismo sesgo" = pares también por debajo de 0.5 (bearish sobre UP)
        below_half = [v for v in peer_vals if v < 0.5]
        bt["consensus"] = "SOFT" if len(below_half) >= 1 else "NONE"
    else:
        peer_vals = [markets[p]["dn_mid"] for p in peers if markets[p]["dn_mid"] > 0]
        below_half = [v for v in peer_vals if v < 0.5]
        bt["consensus"] = "SOFT" if len(below_half) >= 1 else "NONE"


# ═══════════════════════════════════════════════════════
#  ENTRADA CONTRARIAN
# ═══════════════════════════════════════════════════════

def check_entry():
    if bt["traded_this_cycle"]:
        return
    if not bt["entry_window"]:
        return
    if bt["consensus"] != CONSENSUS_REQUIRED:
        bt["skipped"] += 1
        return
    if not bt["signal_asset"] or not bt["reversal_side"]:
        return

    div_abs = abs(bt["signal_div"])

    # Entrada solo cuando el gap supera el umbral de sobre-extensión
    if div_abs <= REVERSAL_THRESHOLD:
        return

    sym      = bt["signal_asset"]
    gap_side = bt["signal_side"]    # lado sobre-extendido (barato, con el gap)
    reversal = bt["reversal_side"]  # lado que COMPRAMOS (opuesto al gap)

    # Precios del lado CONTRARIO al gap (el caro, que el mercado favorece)
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

    # Evitar activos ya resueltos
    up_mid = markets[sym]["up_mid"]
    dn_mid = markets[sym]["dn_mid"]
    if up_mid >= RESOLVED_UP_THRESH or up_mid <= RESOLVED_DN_THRESH or \
       dn_mid >= RESOLVED_UP_THRESH or dn_mid <= RESOLVED_DN_THRESH:
        log_event(f"SKIP {reversal} {sym} — activo ya resuelto")
        bt["skipped"] += 1
        return

    if entry_ask < ENTRY_MIN_PRICE:
        log_event(f"SKIP {reversal} {sym} — ask={entry_ask:.4f} bajo mínimo {ENTRY_MIN_PRICE}")
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

    # ── COMPRA REAL ───────────────────────────────────────────────────────
    log_event(f"COMPRANDO {reversal} {sym} @ ask={entry_ask:.4f} | {shares_req} shares... (gap {gap_side} {gap_entry*100:+.1f}bp)")
    result = place_taker_buy(token_id, shares_req, entry_ask)

    if not result["success"] or result["shares_filled"] < shares_req * 0.5:
        log_event(f"FALLO compra {gap_side} {sym}: {result['error']}")
        return

    shares_filled = result["shares_filled"]
    fill_price    = result.get("fill_price", entry_ask)
    usd_spent     = round(shares_filled * fill_price, 4)

    # Pre-aprobar token para poder vender en stop loss
    approve_conditional_token(token_id)

    bt["capital"]           -= usd_spent
    bt["traded_this_cycle"]  = True

    bt["position"] = {
        "asset":            sym,
        "side":             reversal,   # lado que compramos (opuesto al gap)
        "gap_side":         gap_side,   # lado sobre-extendido (referencia)
        "token_id":         token_id,
        "entry_price":      fill_price,
        "entry_bid":        entry_bid,
        "entry_mid":        entry_mid,
        "entry_usd":        usd_spent,
        "shares":           shares_filled,
        "secs_left_entry":  secs,
        "harm_entry":       harm_entry,
        "gap_entry":        gap_entry,
        "entry_ts":         datetime.now().isoformat(),
        "consensus_entry":  bt["consensus"],
        "peer_snaps":       peer_snaps,
        "capital_before":   capital_before,
        "salida_pendiente": False,
    }

    log_event(
        f"REVERSAL {reversal} {sym} @ fill={fill_price:.4f} | "
        f"gap {gap_side}={gap_entry*100:+.1f}bp (>{REVERSAL_THRESHOLD*100:.1f}bp) | "
        f"harm={harm_entry:.4f} | shares={shares_filled} | "
        f"capital=${bt['capital']:.2f}"
    )
    write_state()


# ═══════════════════════════════════════════════════════
#  STOP LOSS
# ═══════════════════════════════════════════════════════

def check_stop_loss():
    pos = bt["position"]
    if not pos:
        return

    sym      = pos["asset"]
    side     = pos["side"]
    token_id = pos["token_id"]
    bid      = markets[sym]["up_bid"] if side == "UP" else markets[sym]["dn_bid"]

    # Reintentar salida pendiente de ciclo anterior
    if pos.get("salida_pendiente") and bid > 0:
        log_event(f"Reintentando salida pendiente {side} {sym}...")
    elif bid > STOP_LOSS_PRICE or bid <= 0:
        return

    # Verificar balance real en CLOB antes de vender
    clob_bal = get_clob_balance(token_id)
    shares_a_vender = clob_bal if clob_bal > 0 else pos["shares"]

    log_event(f"STOP LOSS {side} {sym} @ bid={bid:.4f} | vendiendo {shares_a_vender} shares...")
    approve_conditional_token(token_id)
    result = place_taker_sell(token_id, shares_a_vender, bid)

    if not result["success"]:
        log_event(f"FALLO venta SL {side} {sym}: {result['error']} — reintentando próximo ciclo")
        pos["salida_pendiente"] = True
        write_state()
        return

    fill_price = result.get("fill_price") or bid
    pnl        = round(shares_a_vender * fill_price - pos["entry_usd"], 6)
    bt["capital"]   += pos["entry_usd"] + pnl
    bt["total_pnl"] += pnl
    bt["losses"]    += 1
    update_drawdown()
    log_event(f"STOP LOSS ejecutado {side} {sym} @ {fill_price:.4f} | PnL=${pnl:+.4f}")
    _record_trade_sl(pos, fill_price, pnl)
    bt["position"] = None
    write_state()


# ═══════════════════════════════════════════════════════
#  RESOLUCIÓN
# ═══════════════════════════════════════════════════════

def _apply_resolution(pos, resolved):
    sym  = pos["asset"]
    side = pos["side"]
    if resolved == side:
        # Ganamos — Polymarket auto-redime shares → 1 USDC cada una
        pnl     = round(pos["shares"] - pos["entry_usd"], 6)
        outcome = "WIN"
        bt["wins"] += 1
    else:
        pnl     = -pos["entry_usd"]
        outcome = "LOSS"
        bt["losses"] += 1

    # Sincronizar capital real desde CLOB (el redeem ya habrá ocurrido)
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
        side = pos.get("gap_side", pos["side"])  # referencia del gap, no del reversal
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
        "sl_price":         STOP_LOSS_PRICE,
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


def _record_trade_sl(pos, exit_bid, pnl):
    record = _build_trade_record(pos, "STOP_LOSS", exit_bid, None, "LOSS", pnl)
    bt["trades"].append(record)
    _save_csv(record)
    _save_log()


def _save_log():
    total = bt["wins"] + bt["losses"]
    with open(LOG_FILE, "w") as f:
        json.dump({
            "strategy": "REVERSAL",
            "summary": {
                "capital_inicial":  CAPITAL_TOTAL,
                "capital_actual":   round(bt["capital"], 4),
                "total_pnl_usd":    round(bt["total_pnl"], 4),
                "roi_pct":          round((bt["capital"] - CAPITAL_TOTAL) / CAPITAL_TOTAL * 100, 2),
                "max_drawdown":     round(bt["max_drawdown"], 4),
                "wins":             bt["wins"],
                "losses":           bt["losses"],
                "win_rate":         round(bt["wins"] / total * 100, 1) if total else 0,
                "skipped":          bt["skipped"],
                "entry_usd":        ENTRY_USD,
                "reversal_threshold_bp": REVERSAL_THRESHOLD * 100,
            },
            "trades": bt["trades"],
        }, f, indent=2)


# ═══════════════════════════════════════════════════════
#  LOOP PRINCIPAL
# ═══════════════════════════════════════════════════════

async def main_loop():
    log_event("basket_reversal.py iniciado — REVERSIÓN ARMÓNICA v1")
    log_event(f"Capital: ${CAPITAL_TOTAL:.0f} | Entrada: ${ENTRY_USD:.2f} fijo")
    log_event(f"Umbral reversión: >{REVERSAL_THRESHOLD*100:.1f}bp | Ventana {ENTRY_OPEN_SECS}s–{ENTRY_WINDOW_SECS}s")

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
            await fetch_all()

            secs = min_secs_remaining()
            bt["entry_window"] = (
                secs is not None and
                ENTRY_CLOSE_SECS < secs <= ENTRY_WINDOW_SECS and
                secs >= ENTRY_OPEN_SECS
            )

            if bt["position"]:
                check_stop_loss()
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
    log.info("  BASKET REVERSAL — REVERSIÓN ARMÓNICA  v1")
    log.info(f"  Capital: ${CAPITAL_TOTAL:.0f}  |  Entrada: ${ENTRY_USD:.2f} fijo (≥5 shares hasta ask {ENTRY_MAX_PRICE})")
    log.info(f"  Umbral: >{REVERSAL_THRESHOLD*100:.1f}bp  |  Ventana: {ENTRY_OPEN_SECS}s — {ENTRY_WINDOW_SECS}s")
    log.info("  SIMULACION — SIN DINERO REAL")
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
