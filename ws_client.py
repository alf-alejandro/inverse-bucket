"""
ws_client.py — WebSocket client para Polymarket CLOB

Dos componentes:
  FillWatcher   — detecta el fill de una orden específica en <100ms
  MarketDataWS  — suscripción continua al order book en tiempo real

Endpoints:
  wss://ws-subscriptions-clob.polymarket.com/ws/market  — order book / price feed
  wss://ws-subscriptions-clob.polymarket.com/ws/user    — fills / user orders
"""

import json
import threading
import logging
import time

import websocket

log = logging.getLogger("ws_client")

WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_USER_URL   = "wss://ws-subscriptions-clob.polymarket.com/ws/user"


# ── FillWatcher ───────────────────────────────────────────────────────────────

class FillWatcher:
    """
    Abre una conexión WebSocket temporal para detectar el fill de una orden.
    Mucho más rápido que polling REST (detección en <100ms vs ~250ms).

    Uso:
        watcher = FillWatcher(api_key, secret, passphrase, order_id, timeout=3.0)
        result  = watcher.wait()
        # result = {"filled": True, "size_matched": 7.14, "avg_price": 0.42}
    """

    def __init__(self, api_key: str, secret: str, passphrase: str,
                 order_id: str, timeout: float = 3.0):
        self.api_key    = api_key
        self.secret     = secret
        self.passphrase = passphrase
        self.order_id   = order_id
        self.timeout    = timeout
        self.result     = {"filled": False, "size_matched": 0.0, "avg_price": 0.0}
        self._done      = threading.Event()

    def wait(self) -> dict:
        ws = websocket.WebSocketApp(
            WS_USER_URL,
            on_open    = self._on_open,
            on_message = self._on_message,
            on_error   = lambda ws, e: log.debug(f"FillWS error: {e}"),
        )
        t = threading.Thread(target=lambda: ws.run_forever(), daemon=True)
        t.start()
        self._done.wait(timeout=self.timeout)
        ws.close()
        return self.result

    def _on_open(self, ws):
        ws.send(json.dumps({
            "type": "user",
            "auth": {
                "apiKey":     self.api_key,
                "secret":     self.secret,
                "passphrase": self.passphrase,
            },
        }))

    def _on_message(self, ws, message):
        try:
            events = json.loads(message)
            if not isinstance(events, list):
                events = [events]
            for ev in events:
                oid    = ev.get("id") or ev.get("order_id", "")
                status = (ev.get("status") or ev.get("event_type") or "").upper()
                if oid == self.order_id and status in ("MATCHED", "FILLED", "TRADE"):
                    self.result["filled"]       = True
                    self.result["size_matched"] = float(ev.get("size_matched") or ev.get("matched_amount") or 0)
                    self.result["avg_price"]    = float(ev.get("price") or ev.get("avg_price") or 0)
                    self._done.set()
                    ws.close()
        except Exception as e:
            log.debug(f"FillWS parse error: {e}")


# ── MarketDataWS ──────────────────────────────────────────────────────────────

class MarketDataWS:
    """
    Suscripción continua al order book vía WebSocket.
    Reemplaza el polling REST cada 0.2s — recibe updates en <50ms.

    Uso:
        mws = MarketDataWS()
        mws.subscribe([up_token_id, down_token_id])

        up_m = mws.get_metrics(up_token_id)
        dn_m = mws.get_metrics(down_token_id)

        mws.unsubscribe()
    """

    def __init__(self):
        self._books      = {}
        self._raw_books  = {}
        self._ws         = None
        self._thread     = None
        self._subscribed = set()
        self._lock       = threading.Lock()
        self._connected  = threading.Event()
        self._active     = False

    def subscribe(self, token_ids: list):
        self._active = True
        self._subscribed = set(token_ids)
        self._connect(token_ids)

    def unsubscribe(self):
        self._active = False
        self._subscribed.clear()
        with self._lock:
            self._books.clear()
            self._raw_books.clear()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def get_metrics(self, token_id: str) -> dict | None:
        with self._lock:
            return self._books.get(token_id)

    def is_ready(self, token_id: str) -> bool:
        with self._lock:
            return token_id in self._books

    def _connect(self, token_ids: list):
        # Nullify before closing so _on_close on the old ws doesn't trigger reconnect
        old_ws = self._ws
        self._ws = None
        if old_ws:
            try:
                old_ws.close()
            except Exception:
                pass

        if not self._active:
            return

        self._connected.clear()

        ws = websocket.WebSocketApp(
            WS_MARKET_URL,
            on_open    = lambda ws: self._on_open(ws, token_ids),
            on_message = self._on_message,
            on_error   = lambda ws, e: log.warning(f"MarketWS error: {e}"),
            on_close   = lambda ws, *a: self._on_close(ws),
        )
        self._ws = ws
        self._thread = threading.Thread(
            target=lambda: ws.run_forever(ping_interval=20, ping_timeout=10),
            daemon=True,
        )
        self._thread.start()
        self._connected.wait(timeout=5)

    def _on_open(self, ws, token_ids: list):
        ws.send(json.dumps({
            "type":      "market",
            "assets_ids": token_ids,
        }))
        self._connected.set()
        log.info(f"MarketWS conectado — {len(token_ids)} tokens")

    def _on_message(self, ws, message):
        try:
            events = json.loads(message)
            if not isinstance(events, list):
                events = [events]
            for ev in events:
                self._process(ev)
        except Exception as e:
            log.debug(f"MarketWS parse: {e}")

    def _on_close(self, closed_ws=None):
        # Ignore stale close callbacks from replaced connections
        if closed_ws is not None and closed_ws is not self._ws:
            return
        if self._active and self._subscribed:
            log.warning("MarketWS desconectado — reconectando en 2s...")
            time.sleep(2)
            self._connect(list(self._subscribed))

    def _process(self, ev: dict):
        event_type = ev.get("event_type", "")
        asset_id   = ev.get("asset_id", "")
        if not asset_id:
            return

        if event_type == "book":
            bids = {float(b["price"]): float(b["size"]) for b in ev.get("bids", [])}
            asks = {float(a["price"]): float(a["size"]) for a in ev.get("asks", [])}
            with self._lock:
                self._raw_books[asset_id] = {"bids": bids, "asks": asks}
                self._books[asset_id] = self._calc_metrics(bids, asks)

        elif event_type == "price_change":
            with self._lock:
                raw = self._raw_books.get(asset_id)
                if raw is None:
                    return
                for change in ev.get("changes", []):
                    price = float(change["price"])
                    size  = float(change["size"])
                    side  = change.get("side", "").upper()
                    book  = raw["bids"] if side == "BUY" else raw["asks"]
                    if size == 0:
                        book.pop(price, None)
                    else:
                        book[price] = size
                self._books[asset_id] = self._calc_metrics(raw["bids"], raw["asks"])

    def _calc_metrics(self, bids: dict, asks: dict) -> dict:
        bids_s = sorted(bids.items(), key=lambda x: x[0], reverse=True)
        asks_s = sorted(asks.items(), key=lambda x: x[0])

        bid_vol = sum(s for _, s in bids_s)
        ask_vol = sum(s for _, s in asks_s)
        total   = bid_vol + ask_vol
        obi     = (bid_vol - ask_vol) / total if total > 0 else 0.0

        best_bid = bids_s[0][0] if bids_s else 0.0
        best_ask = asks_s[0][0] if asks_s else 0.0

        if bid_vol > 0 and ask_vol > 0:
            bvwap    = sum(p * s for p, s in bids_s) / bid_vol
            avwap    = sum(p * s for p, s in asks_s) / ask_vol
            vwap_mid = (bvwap * bid_vol + avwap * ask_vol) / total
        else:
            vwap_mid = (best_bid + best_ask) / 2

        return {
            "bid_volume":   round(bid_vol, 2),
            "ask_volume":   round(ask_vol, 2),
            "total_volume": round(total, 2),
            "obi":          round(obi, 4),
            "best_bid":     round(best_bid, 4),
            "best_ask":     round(best_ask, 4),
            "spread":       round(best_ask - best_bid, 4),
            "vwap_mid":     round(vwap_mid, 4),
            "num_bids":     len(bids_s),
            "num_asks":     len(asks_s),
            "top_bids":     [(round(p, 4), round(s, 2)) for p, s in bids_s[:8]],
            "top_asks":     [(round(p, 4), round(s, 2)) for p, s in asks_s[:8]],
        }
