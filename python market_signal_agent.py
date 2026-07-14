import requests
import json
import time
import os
import re
from datetime import datetime, timezone, timedelta

# ============================================================
# CONFIG
# ============================================================
TWITTER_API_KEY = os.environ.get("TWITTER_API_KEY", "")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_IDS = os.environ.get("TELEGRAM_CHAT_IDS", "").split(",")
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")
ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "1or4ZytaOCJ_C6vWGG7PGIvs3iThpLfxO9FSomtTAv-I")

# Google credentials loaded from single JSON env variable
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "{}")

ACCOUNTS = [
    "elonmusk", "realDonaldTrump", "federalreserve", "jpmorgan",
    "GoldmanSachs", "michael_saylor", "WarrenBuffett",
    "nouriel", "RayDalio", "PeterSchiff",
    "VitalikButerin", "cz_binance", "SEC_News", "IMFNews",
    "business", "Reuters", "WSJ", "zerohedge",
    "thelonginvest", "unusual_whales"
]

ASSET_MAP = {
    "gold": "GLD", "oil": "USO", "wti": "USO", "crude": "USO",
    "silver": "SLV", "natural gas": "UNG", "bitcoin": "IBIT",
    "ethereum": "ETHE", "crypto": "IBIT"
}

TWEETS_PER_ACCOUNT = 3
MIN_SIGNAL_SCORE = 8
RUN_INTERVAL_HOURS = 1
BATCH_SIZE = 10
DEDUP_WINDOW_MINUTES = 45  # merge same symbol+direction signals whose source tweets fall within this window
ATR_PERIOD_DAYS = 14
ATR_TARGET_MULTIPLIER = 2.5   # target = entry ± ATR * this
ATR_STOP_MULTIPLIER = 1.5     # stop   = entry ± ATR * this  (~1.67:1 reward:risk)
RISK_PCT_PER_TRADE = 0.01     # max risk per trade as a fraction of current account capital
MAX_TOTAL_DEPLOYED_PCT = 0.25 # hard cap on total capital deployed across all open positions at once
SIM_STARTING_BALANCE = 10000  # local paper-sim starting balance — fully independent of the Alpaca account
SIM_SLIPPAGE_PCT = 0.001      # unfavorable slippage applied to simulated market-style fills (entries, stop/expiry exits)

_atr_cache = {}   # symbol -> ATR ($) for this run only; cleared at the top of run()

previous_signals = {}
last_seen_ids = {}   # username -> newest tweet id seen so far (high-water mark, in-memory for the process life)
previous_open_positions = set()   # symbols open at the previous cycle; in-memory only (resets on restart)
sheets_token = None
sheets_token_expiry = 0

# ============================================================
# GOOGLE SHEETS (raw REST API - no gspread)
# ============================================================
def get_sheets_token():
    global sheets_token, sheets_token_expiry
    now = time.time()
    if sheets_token and now < sheets_token_expiry - 60:
        return sheets_token
    try:
        import base64
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.backends import default_backend

        creds = json.loads(GOOGLE_CREDENTIALS_JSON)
        now_int = int(now)

        header = base64.urlsafe_b64encode(json.dumps({"alg":"RS256","typ":"JWT"}).encode()).rstrip(b"=").decode()
        payload = base64.urlsafe_b64encode(json.dumps({
            "iss": creds["client_email"],
            "scope": "https://www.googleapis.com/auth/spreadsheets",
            "aud": "https://oauth2.googleapis.com/token",
            "exp": now_int + 3600,
            "iat": now_int
        }).encode()).rstrip(b"=").decode()

        key = serialization.load_pem_private_key(
            creds["private_key"].encode(), password=None, backend=default_backend()
        )
        sig = key.sign(f"{header}.{payload}".encode(), padding.PKCS1v15(), hashes.SHA256())
        sig_b64 = base64.urlsafe_b64encode(sig).rstrip(b"=").decode()
        jwt = f"{header}.{payload}.{sig_b64}"

        r = requests.post("https://oauth2.googleapis.com/token", data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt
        }, timeout=10)
        data = r.json()
        sheets_token = data.get("access_token")
        sheets_token_expiry = now + data.get("expires_in", 3600)
        if sheets_token:
            print("  [SHEETS] Connected to Google Sheets")
        return sheets_token
    except Exception as e:
        print(f"  [ERROR] Sheets auth: {e}")
        return None

def sheets_append(sheet_name, row):
    try:
        token = get_sheets_token()
        if not token:
            return
        safe_row = [str(v) if v is not None else "" for v in row]
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/{sheet_name}!A1:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS"
        r = requests.post(url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"values": [safe_row]},
            timeout=15
        )
        if r.status_code == 200:
            print(f"  [SHEETS] Logged to {sheet_name}")
        else:
            print(f"  [ERROR] Sheets {sheet_name}: {r.status_code} {r.text[:100]}")
    except Exception as e:
        print(f"  [ERROR] Sheets append: {e}")

def init_sheets():
    try:
        token = get_sheets_token()
        if not token:
            return
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        
        for sheet_name, cols in [
            ("Signals", ["Timestamp","Account","Asset","Symbol","Direction","Confidence","Price","Target","Stop Loss","Time Horizon","Exit Trigger","Expiry","Signal Type","Conflict","Sentiment Shift","Ticker Confidence","Ticker Method","Source Tweet","Contributing Accounts","ATR","ATR Target %","ATR Stop %"]),
            ("Trades", ["Timestamp","Action","Symbol","Entry Price","Take Profit Price","Stop Loss Price","Target %","Stop %","USD Amount","Qty","Account","Asset","Order ID","Close Status","Exit Price","Exit Reason","P&L %","Closed At","Capital At Trade","Risk %"]),
            ("Outcomes", ["Signal Timestamp","Account","Symbol","Direction","Confidence","Entry Price","Target Price","Stop Price","Outcome","Price at Check","Actual % Move","Checked At"]),
            ("SimPositions", ["Opened At","Symbol","Direction","Entry Price","Target Price","Stop Price","Qty","USD Amount","Sim Capital At Trade","Contributing Accounts","Confidence","Status","Exit Price","Exit Reason","P&L $","P&L %","Closed At","Expiry"])
        ]:
            url = f"https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/{sheet_name}!A1"
            r = requests.get(url, headers=headers, timeout=10)
            if not r.json().get("values"):
                sheets_append(sheet_name, cols)

        print("  [SHEETS] Sheets initialized")
    except Exception as e:
        print(f"  [ERROR] Init sheets: {e}")

def sheets_read(sheet_name):
    try:
        token = get_sheets_token()
        if not token:
            return []
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/{sheet_name}"
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        if r.status_code == 200:
            return r.json().get("values", [])
        # A missing tab returns 400 — treat as no rows rather than raising.
        return []
    except Exception as e:
        print(f"  [ERROR] Sheets read {sheet_name}: {e}")
        return []

def sheets_update(a1_range, row):
    """Overwrite a specific A1 range (e.g. 'Trades!N5:R5') in place — used to update, not append."""
    try:
        token = get_sheets_token()
        if not token:
            return False
        safe_row = [str(v) if v is not None else "" for v in row]
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/{a1_range}?valueInputOption=USER_ENTERED"
        r = requests.put(url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"values": [safe_row]},
            timeout=15
        )
        if r.status_code == 200:
            print(f"  [SHEETS] Updated {a1_range}")
            return True
        print(f"  [ERROR] Sheets update {a1_range}: {r.status_code} {r.text[:100]}")
        return False
    except Exception as e:
        print(f"  [ERROR] Sheets update: {e}")
        return False

# ============================================================
# OUTCOME TRACKING
# ============================================================
def _parse_expiry(s):
    """Parse '2026-07-05 14:30 UTC' (and a couple looser variants) into an aware UTC datetime."""
    if not s or s.strip().upper() == "N/A":
        return None
    txt = s.replace("UTC", "").strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(txt, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None

def _parse_price(s):
    """Parse a logged price like '$408.96' into a float; None if missing/N/A."""
    if not s or s.strip().upper() == "N/A":
        return None
    try:
        return float(s.replace("$", "").replace(",", "").strip())
    except ValueError:
        return None

def _log_outcome(row):
    # Columns: Signal Timestamp, Account, Symbol, Direction, Confidence, Entry Price,
    #          Target Price, Stop Price, Outcome, Price at Check, Actual % Move, Checked At
    sheets_append("Outcomes", row)

def check_outcomes():
    """Classify past signals whose expiry has passed, and log results to the Outcomes tab.
    Wrapped in try/except so any failure here never blocks the main signal/trade flow."""
    try:
        signals = sheets_read("Signals")
        if not signals:
            return

        # Keys of outcomes already recorded: (signal timestamp, account, symbol, direction).
        done = set()
        for row in sheets_read("Outcomes"):
            if len(row) >= 4 and row[0].strip() != "Signal Timestamp":
                done.add((row[0].strip(), row[1].strip(), row[2].strip(), row[3].strip()))

        now = datetime.now(timezone.utc)
        checked = 0
        for row in signals:
            # Signals cols: 0 Timestamp,1 Account,2 Asset,3 Symbol,4 Direction,5 Confidence,
            #               6 Price,7 Target,8 Stop Loss,9 Time Horizon,10 Exit Trigger,11 Expiry,...
            if len(row) < 12 or row[0].strip() == "Timestamp":
                continue  # header or malformed
            ts, account, symbol, direction = row[0].strip(), row[1].strip(), row[3].strip(), row[4].strip()
            confidence, entry_str, target_str, stop_str, expiry_str = (
                row[5].strip(), row[6].strip(), row[7].strip(), row[8].strip(), row[11].strip()
            )

            key = (ts, account, symbol, direction)
            if key in done:
                continue

            # Only score signals whose expiry has actually passed.
            expiry_dt = _parse_expiry(expiry_str)
            if expiry_dt is None or now < expiry_dt:
                continue

            checked_at = now.strftime("%Y-%m-%d %H:%M UTC")

            # No resolvable symbol (or no entry price) — record UNRESOLVED, never guess.
            entry = _parse_price(entry_str)
            if not symbol or symbol.upper() == "N/A" or entry is None:
                _log_outcome([ts, account, symbol or "N/A", direction, confidence, entry_str or "N/A",
                              "N/A", "N/A", "UNRESOLVED", "N/A", "N/A", checked_at])
                done.add(key); checked += 1
                continue

            current = get_price(symbol)
            if current is None:
                continue  # can't classify right now — retry on a later cycle

            # Prefer ATR-derived target/stop % (cols 20/21) — what the bot actually traded on.
            # Fall back to the LLM's original suggestion (cols 7/8) for rows logged before this existed.
            target_pct = parse_pct(row[20].strip(), None) if len(row) > 20 and row[20].strip() else None
            stop_pct = parse_pct(row[21].strip(), None) if len(row) > 21 and row[21].strip() else None
            if target_pct is None:
                target_pct = parse_pct(target_str, 5.0)
            if stop_pct is None:
                stop_pct = parse_pct(stop_str, 3.0)

            # Derive target/stop PRICES from the logged entry + %, honouring direction.
            if direction == "bearish":
                target_price = round(entry * (1 - target_pct / 100), 2)
                stop_price = round(entry * (1 + stop_pct / 100), 2)
            else:  # bullish (neutral has no directional bet — treated as long for pricing)
                target_price = round(entry * (1 + target_pct / 100), 2)
                stop_price = round(entry * (1 - stop_pct / 100), 2)

            move_pct = (current - entry) / entry * 100  # signed move from entry to check

            if direction == "bullish":
                outcome = "WIN" if move_pct >= target_pct else "LOSS" if move_pct <= -stop_pct else "EXPIRED_FLAT"
            elif direction == "bearish":
                outcome = "WIN" if move_pct <= -target_pct else "LOSS" if move_pct >= stop_pct else "EXPIRED_FLAT"
            else:
                outcome = "EXPIRED_FLAT"  # no directional bet to score

            _log_outcome([ts, account, symbol, direction, confidence, f"${entry:.2f}",
                          f"${target_price:.2f}", f"${stop_price:.2f}", outcome,
                          f"${current:.2f}", f"{move_pct:+.2f}%", checked_at])
            done.add(key); checked += 1

        print(f"  [OUTCOMES] Checked {checked} newly-expired signal(s)")
    except Exception as e:
        print(f"  [ERROR] Outcomes: {e}")

def generate_confidence_report():
    """Correlate signal confidence against actual outcome, bucketed 1-10, using the Outcomes log.
    Informational only — NOT used for position sizing (sizing is risk-formula-driven, see #4).
    Rebuilds the whole tab each run since this is a derived snapshot, not an append-only log."""
    try:
        rows = sheets_read("Outcomes")
        buckets = {c: {"WIN": 0, "LOSS": 0, "EXPIRED_FLAT": 0, "UNRESOLVED": 0, "moves": []} for c in range(1, 11)}
        for row in (rows[1:] if rows else []):
            if len(row) < 11:
                continue
            try:
                confidence = int(float(row[4].strip()))
            except ValueError:
                continue
            if confidence not in buckets:
                continue
            outcome = row[8].strip()
            if outcome in buckets[confidence]:
                buckets[confidence][outcome] += 1
            try:
                buckets[confidence]["moves"].append(float(row[10].strip().replace("%", "").replace("+", "")))
            except ValueError:
                pass

        report_rows = [
            ["NOTE: informational only — NOT used for position sizing (sizing is risk-formula-driven, see item #4)"],
            ["Confidence", "Wins", "Losses", "Expired Flat", "Unresolved", "Scored Total", "Win Rate %", "Avg Move %"],
        ]
        for c in range(10, 0, -1):
            b = buckets[c]
            scored = b["WIN"] + b["LOSS"] + b["EXPIRED_FLAT"]
            win_rate = (b["WIN"] / scored * 100) if scored else 0.0
            avg_move = (sum(b["moves"]) / len(b["moves"])) if b["moves"] else 0.0
            report_rows.append([c, b["WIN"], b["LOSS"], b["EXPIRED_FLAT"], b["UNRESOLVED"], scored,
                                 f"{win_rate:.1f}%", f"{avg_move:+.2f}%"])

        token = get_sheets_token()
        if not token:
            return
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/ConfidenceReport!A1:H{len(report_rows)}?valueInputOption=USER_ENTERED"
        r = requests.put(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                          json={"values": report_rows}, timeout=15)
        if r.status_code == 200:
            print("  [REPORT] Confidence correlation report updated")
        else:
            print(f"  [ERROR] Confidence report: {r.status_code} {r.text[:120]}")
    except Exception as e:
        print(f"  [ERROR] Confidence report: {e}")

# ============================================================
# POSITION CLOSE DETECTION
# ============================================================
def _handle_closed_position(symbol):
    # (a) Pull recent closed orders for this symbol (most recent first).
    r = requests.get(
        f"{ALPACA_BASE_URL}/orders",
        headers=alpaca_headers(),
        params={"symbols": symbol, "status": "closed", "limit": 10, "direction": "desc"},
        timeout=10
    )
    if r.status_code != 200:
        print(f"  [ERROR] Close orders {symbol}: {r.status_code} {r.text[:120]}")
        return
    orders = r.json()

    # (b) Most recent FILLED sell bracket leg — the exit, not the original entry buy.
    exit_order = None
    for o in orders:
        if (o.get("side") == "sell" and o.get("order_class") == "bracket"
                and o.get("status") == "filled" and o.get("filled_avg_price")):
            exit_order = o
            break  # list is desc — first match is the most recent
    if not exit_order:
        print(f"  [CLOSE] {symbol} closed but no filled sell bracket leg found — skipping")
        return

    # (c) Exit reason from the order type.
    otype = exit_order.get("type", "")
    if otype == "limit":
        reason = "Take-Profit Hit"
    elif otype in ("stop", "stop_limit"):
        reason = "Stop-Loss Hit"
    else:
        reason = "Closed"
    exit_price = float(exit_order.get("filled_avg_price"))

    # (d) Match the open Trades row for this symbol (most recent one with no close status).
    row_num, entry_price = None, None
    trades = sheets_read("Trades")
    for i, t in enumerate(trades):
        if i == 0 or len(t) < 4:
            continue  # header / malformed
        # Trades col C(2)=Symbol, D(3)=Entry Price, N(13)=Close Status
        if t[2].strip().upper() == symbol.upper() and (len(t) <= 13 or not t[13].strip()):
            row_num = i + 1            # 1-based sheet row (row 1 is the header)
            entry_price = _parse_price(t[3])
    if row_num is None:
        print(f"  [CLOSE] {symbol} no open Trades row found — will notify without row update")

    # (e) P&L (long-only bot; short-side logic intentionally skipped).
    pnl = (exit_price - entry_price) / entry_price * 100 if entry_price else None
    pnl_str = f"{pnl:+.2f}%" if pnl is not None else "N/A"

    # (3) Telegram notification.
    send_telegram(f"✅ Position Closed: {symbol} | Exit: ${exit_price:.2f} | Reason: {reason} | P&L: {pnl_str}")

    # (4) Update the matching Trades row in place (Close Status, Exit Price, Exit Reason, P&L %, Closed At).
    if row_num is not None:
        closed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        sheets_update(f"Trades!N{row_num}:R{row_num}",
                      ["CLOSED", f"${exit_price:.2f}", reason, pnl_str, closed_at])
    print(f"  [CLOSE] {symbol} {reason} exit ${exit_price:.2f} P&L {pnl_str}")

def check_closed_positions(current_open):
    """Detect positions that closed since the previous cycle, notify, and update the Trades row.
    Wrapped so any failure never blocks the main signal/trade flow."""
    global previous_open_positions
    try:
        closed = previous_open_positions - set(current_open)
        if closed:
            print(f"  [CLOSE] Detected {len(closed)} closed position(s): {', '.join(sorted(closed))}")
        for symbol in sorted(closed):
            try:
                _handle_closed_position(symbol)
            except Exception as e:
                print(f"  [ERROR] Close handling {symbol}: {e}")
    except Exception as e:
        print(f"  [ERROR] Close detection: {e}")
    finally:
        # Reset the baseline for the next cycle. Done here (not literally at run() end) so an
        # early return in run() can't leave it stale and re-fire duplicate close alerts.
        previous_open_positions = set(current_open)

def log_signal_to_sheets(signal, symbol, current_price, ticker_confidence, ticker_method, raw_tweet_text, contributing_accounts, atr, atr_target_pct, atr_stop_pct):
    try:
        sheets_append("Signals", [
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            f"@{str(signal.get('account','')).lstrip('@')}",
            signal.get("asset_affected",""),
            symbol or "N/A",
            signal.get("direction",""),
            signal.get("confidence",""),
            f"${current_price:.2f}" if current_price else "N/A",
            signal.get("price_target",""),
            signal.get("stop_loss",""),
            signal.get("time_horizon",""),
            signal.get("exit_trigger",""),
            signal.get("expiry",""),
            signal.get("signal_type",""),
            "YES" if signal.get("conflicting") else "NO",
            "YES" if signal.get("sentiment_shift") else "NO",
            ticker_confidence,
            ticker_method,
            raw_tweet_text or "N/A",
            "; ".join(contributing_accounts) if contributing_accounts else "",
            f"${atr:.2f}" if atr else "N/A",
            f"{atr_target_pct:.2f}%" if atr_target_pct is not None else "N/A",
            f"{atr_stop_pct:.2f}%" if atr_stop_pct is not None else "N/A"
        ])
    except Exception as e:
        print(f"  [ERROR] Log signal: {e}")

def log_trade_to_sheets(symbol, entry_price, tp_price, sl_price, target_pct, stop_pct, usd_amount, qty, signal, order_id, capital, risk_pct):
    try:
        sheets_append("Trades", [
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "BUY_BRACKET",
            symbol,
            f"${entry_price:.2f}",
            f"${tp_price:.2f}",
            f"${sl_price:.2f}",
            f"+{target_pct}%",
            f"-{stop_pct}%",
            f"${usd_amount:.2f}",
            str(qty),
            f"@{str(signal.get('account','')).lstrip('@')}",
            signal.get("asset_affected",""),
            order_id or "",
            "", "", "", "", "",  # Close Status, Exit Price, Exit Reason, P&L %, Closed At — filled in on close
            f"${capital:.2f}",
            f"{risk_pct*100:.2f}%"
        ])
    except Exception as e:
        print(f"  [ERROR] Log trade: {e}")

# ============================================================
# LOCAL PAPER-TRADING SIMULATION
# Fully independent of Alpaca order placement — reads quotes only, never calls /orders.
# Runs in parallel with the live Alpaca-paper flow so the two can be compared directly.
# ============================================================
def get_quote(symbol):
    """(bid, ask) from Alpaca's latest quote — read-only market data, same feed get_price() uses."""
    try:
        url = f"https://data.alpaca.markets/v2/stocks/{symbol.upper()}/quotes/latest"
        r = requests.get(url, headers=alpaca_headers(), timeout=10)
        q = r.json().get("quote", {})
        bid, ask = q.get("bp"), q.get("ap")
        return (float(bid) if bid else None, float(ask) if ask else None)
    except Exception as e:
        print(f"  [ERROR] Quote {symbol}: {e}")
        return None, None

def simulate_entry_fill(symbol):
    """A simulated market buy fills at the ask plus extra unfavorable slippage —
    never at the exact signal-time price, to model real-world execution."""
    _, ask = get_quote(symbol)
    if not ask:
        return None
    return round(ask * (1 + SIM_SLIPPAGE_PCT), 2)

def get_sim_state():
    """Sim capital/deployed/open-symbols derived entirely from the SimPositions log —
    no separate mutable balance to drift or corrupt."""
    rows = sheets_read("SimPositions")
    capital = SIM_STARTING_BALANCE
    deployed = 0.0
    open_symbols = set()
    for i, row in enumerate(rows):
        if i == 0 or len(row) < 12:
            continue  # header or malformed
        status = row[11].strip()
        symbol = row[1].strip().upper()
        if status == "OPEN":
            deployed += _parse_price(row[7]) or 0.0
            open_symbols.add(symbol)
        elif len(row) > 14:
            try:
                capital += float(row[14].strip().replace("+", ""))
            except ValueError:
                pass
    return capital, deployed, open_symbols

def log_sim_position_open(symbol, direction, entry, target, stop, qty, usd_amount, capital_at_trade, accounts, confidence, expiry):
    try:
        sheets_append("SimPositions", [
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            symbol, direction, f"${entry:.2f}", f"${target:.2f}", f"${stop:.2f}",
            str(qty), f"${usd_amount:.2f}", f"${capital_at_trade:.2f}",
            "; ".join(accounts) if accounts else "",
            confidence, "OPEN", "", "", "", "", "", expiry or "N/A"
        ])
    except Exception as e:
        print(f"  [ERROR] Log sim position: {e}")

def check_sim_positions():
    """Mark OPEN sim positions as TARGET_HIT / STOP_HIT / EXPIRED by comparing current price
    against target/stop/expiry, and update the row in place. Wrapped so a failure here never
    blocks the main signal/trade flow."""
    try:
        rows = sheets_read("SimPositions")
        if not rows:
            return
        now = datetime.now(timezone.utc)
        closed = 0
        for i, row in enumerate(rows):
            if i == 0 or len(row) < 12 or row[11].strip() != "OPEN":
                continue  # header, malformed, or already closed

            symbol, direction = row[1].strip(), row[2].strip()
            entry = _parse_price(row[3])
            target_price, stop_price = _parse_price(row[4]), _parse_price(row[5])
            qty = float(row[6]) if row[6].strip() else 0.0
            usd_amount = _parse_price(row[7]) or 0.0
            expiry_dt = _parse_expiry(row[17]) if len(row) > 17 else None

            current = get_price(symbol)
            if current is None or entry is None or target_price is None or stop_price is None:
                continue  # can't classify right now — retry next cycle

            exit_price, exit_reason, market_exit = None, None, False
            if direction == "bearish":
                if current <= target_price:
                    exit_price, exit_reason = target_price, "Target Hit"
                elif current >= stop_price:
                    exit_price, exit_reason, market_exit = stop_price, "Stop Hit", True
            else:  # bullish / neutral treated as long
                if current >= target_price:
                    exit_price, exit_reason = target_price, "Target Hit"
                elif current <= stop_price:
                    exit_price, exit_reason, market_exit = stop_price, "Stop Hit", True

            if exit_price is None and expiry_dt and now >= expiry_dt:
                exit_price, exit_reason, market_exit = current, "Expired", True

            if exit_price is None:
                continue  # still open

            # Market-style exits (stop/expiry) slip against you; the target exit is a limit fill.
            if market_exit:
                exit_price = round(exit_price * (1 - SIM_SLIPPAGE_PCT if direction != "bearish" else 1 + SIM_SLIPPAGE_PCT), 2)

            pnl_dollars = (exit_price - entry) * qty if direction != "bearish" else (entry - exit_price) * qty
            pnl_pct = (pnl_dollars / usd_amount * 100) if usd_amount else 0.0
            status = exit_reason.upper().replace(" ", "_")
            closed_at = now.strftime("%Y-%m-%d %H:%M UTC")

            row_num = i + 1
            sheets_update(f"SimPositions!L{row_num}:Q{row_num}",
                          [status, f"${exit_price:.2f}", exit_reason, f"{pnl_dollars:+.2f}", f"{pnl_pct:+.2f}%", closed_at])
            closed += 1
        print(f"  [SIM] Closed {closed} simulated position(s)")
    except Exception as e:
        print(f"  [ERROR] Sim positions check: {e}")

# ============================================================
# ALPACA
# ============================================================
def alpaca_headers():
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        "Content-Type": "application/json"
    }

def get_price(symbol):
    try:
        url = f"https://data.alpaca.markets/v2/stocks/{symbol.upper()}/quotes/latest"
        r = requests.get(url, headers=alpaca_headers(), timeout=10)
        data = r.json()
        price = data.get("quote", {}).get("ap") or data.get("quote", {}).get("bp")
        return float(price) if price else None
    except Exception as e:
        print(f"  [ERROR] Price for {symbol}: {e}")
        return None

def get_open_positions():
    try:
        r = requests.get(f"{ALPACA_BASE_URL}/positions", headers=alpaca_headers(), timeout=10)
        if r.status_code == 200:
            return {p["symbol"].upper() for p in r.json()}
        return set()
    except Exception as e:
        print(f"  [ERROR] Get positions: {e}")
        return set()

def get_account_equity():
    """Total account value from Alpaca (paper by default, per ALPACA_BASE_URL) — the capital
    base for risk sizing. None on any failure; callers must skip trading rather than guess."""
    try:
        r = requests.get(f"{ALPACA_BASE_URL}/account", headers=alpaca_headers(), timeout=10)
        if r.status_code == 200:
            equity = r.json().get("equity")
            return float(equity) if equity is not None else None
        print(f"  [ERROR] Account: {r.status_code} {r.text[:120]}")
        return None
    except Exception as e:
        print(f"  [ERROR] Account: {e}")
        return None

def get_deployed_capital():
    """Sum of market value across all open positions — used against MAX_TOTAL_DEPLOYED_PCT."""
    try:
        r = requests.get(f"{ALPACA_BASE_URL}/positions", headers=alpaca_headers(), timeout=10)
        if r.status_code == 200:
            return sum(float(p.get("market_value", 0) or 0) for p in r.json())
        return 0.0
    except Exception as e:
        print(f"  [ERROR] Deployed capital: {e}")
        return 0.0

def calculate_position_size(capital, entry_price, stop_price, risk_pct=RISK_PCT_PER_TRADE):
    """Risk-based sizing: risk risk_pct of current capital on the entry-to-stop distance.
    Returns share quantity (not USD notional)."""
    per_share_risk = abs(entry_price - stop_price)
    if per_share_risk <= 0:
        return 0.0
    return (capital * risk_pct) / per_share_risk

def get_daily_bars(symbol, limit):
    try:
        url = f"https://data.alpaca.markets/v2/stocks/{symbol.upper()}/bars"
        r = requests.get(url, headers=alpaca_headers(),
                          params={"timeframe": "1Day", "limit": limit, "adjustment": "raw"}, timeout=10)
        if r.status_code != 200:
            print(f"  [ERROR] Bars {symbol}: {r.status_code} {r.text[:120]}")
            return []
        return r.json().get("bars", []) or []
    except Exception as e:
        print(f"  [ERROR] Bars {symbol}: {e}")
        return []

def get_atr(symbol, period=ATR_PERIOD_DAYS):
    """Average True Range over `period` daily bars. None if there isn't enough history —
    callers must treat that as 'can't size this trade', never guess a fixed % instead."""
    if symbol in _atr_cache:
        return _atr_cache[symbol]
    bars = get_daily_bars(symbol, period + 1)
    atr = None
    if len(bars) >= 2:
        trs = []
        for i in range(1, len(bars)):
            high, low, prev_close = bars[i]["h"], bars[i]["l"], bars[i-1]["c"]
            trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        trs = trs[-period:]
        if trs:
            atr = sum(trs) / len(trs)
    _atr_cache[symbol] = atr
    return atr

def atr_target_stop(entry_price, atr, direction):
    """Scale target/stop to this ticker's own recent volatility instead of a fixed %."""
    if direction == "bearish":
        target_price = round(entry_price - atr * ATR_TARGET_MULTIPLIER, 2)
        stop_price = round(entry_price + atr * ATR_STOP_MULTIPLIER, 2)
    else:  # bullish / neutral — neutral has no directional bet but priced long for consistency with check_outcomes
        target_price = round(entry_price + atr * ATR_TARGET_MULTIPLIER, 2)
        stop_price = round(entry_price - atr * ATR_STOP_MULTIPLIER, 2)
    target_pct = abs(target_price - entry_price) / entry_price * 100
    stop_pct = abs(stop_price - entry_price) / entry_price * 100
    return target_price, stop_price, target_pct, stop_pct

def parse_pct(pct_str, default=5.0):
    try:
        clean = str(pct_str).replace("%","").replace("+","").strip()
        if "-" in clean.lstrip("-"):
            parts = clean.lstrip("-").split("-")
            val = float(parts[0])
            if str(pct_str).strip().startswith("-"):
                val = -val
        else:
            val = float(clean)
        return abs(val)
    except:
        return default

def place_bracket_order(symbol, current_price, qty, target_pct, stop_pct):
    try:
        qty = round(qty, 6)
        if qty <= 0:
            return None, 0, 0, 0

        tp_price = round(current_price * (1 + target_pct / 100), 2)
        sl_price = round(current_price * (1 - stop_pct / 100), 2)

        # Re-check the live price before submitting — it may have moved since the quote.
        # If it has diverged more than 2% from the price we sized/priced the trade on,
        # skip entirely rather than force-fitting TP/SL around a stale entry.
        live_price = get_price(symbol)
        if not live_price:
            print(f"  [SKIP] no live price {symbol}")
            return None, 0, 0, 0
        gap = abs(live_price - current_price) / current_price
        if gap > 0.02:
            print(f"  [SKIP] price mismatch too large {symbol}: quote ${current_price} vs live ${live_price} ({gap*100:.1f}%)")
            return None, 0, 0, 0

        # Alpaca rejects a bracket take-profit not strictly above / stop not strictly below the
        # base price; nudge each clear of the floor after the (small, <=2%) drift check above.
        if tp_price <= live_price + 0.01:
            tp_price = round(live_price + 0.02, 2)
        if sl_price >= live_price - 0.01:
            sl_price = round(live_price - 0.02, 2)

        print(f"  [BRACKET] {symbol} qty={qty} @ ${current_price} | TP=${tp_price} | SL=${sl_price}")

        body = {
            "symbol": symbol.upper(),
            "qty": str(qty),
            "side": "buy",
            "type": "market",
            "time_in_force": "day",
            "order_class": "bracket",
            "take_profit": {"limit_price": str(tp_price)},
            "stop_loss": {"stop_price": str(sl_price)}
        }

        r = requests.post(f"{ALPACA_BASE_URL}/orders", headers=alpaca_headers(), json=body, timeout=10)
        data = r.json()

        if r.status_code in [200, 201]:
            print(f"  [TRADE OK] {symbol} bracket order — ID: {data.get('id')}")
            return data, tp_price, sl_price, qty
        else:
            print(f"  [ERROR] Bracket failed {symbol}: {data}")
            return None, tp_price, sl_price, qty
    except Exception as e:
        print(f"  [ERROR] Bracket: {e}")
        return None, 0, 0, 0

# ============================================================
# SYMBOL RESOLVER
# ============================================================
def resolve_symbol(asset_affected):
    """Resolve a ticker only when the tweet explicitly names it.
    Returns (symbol_or_None, ticker_confidence 0-10, method)."""
    paren = re.search(r'\(([^)]*)\)', asset_affected)
    if paren:
        inside = paren.group(1).strip()
        # A dot means a non-US exchange suffix (CON.DE, RIO.L, SHOP.TO, ...) — don't trade it,
        # and don't fall through to keyword matching either: an explicit foreign ticker means
        # the intent was clear, just not tradable here.
        if "." in inside:
            return None, 0, "non_us_exchange"
        # A bare 1-5 letter US ticker is the most explicit signal — trust it outright.
        if re.fullmatch(r'[A-Z]{1,5}', inside):
            return inside, 10, "explicit_ticker"
        # Parenthetical present but not a valid ticker (e.g. "Apple (tech)") — fall through
        # to the keyword check below rather than dropping the signal.

    # No usable parenthetical ticker: only a whole-word commodity/asset match, never a
    # substring (this is what silently mapped "spoil"/"boiling"/"turmoil" to USO before).
    asset_lower = asset_affected.lower()
    for keyword, etf in ASSET_MAP.items():
        if re.search(rf'\b{re.escape(keyword)}\b', asset_lower):
            return etf, 6, "keyword_map"

    return None, 0, "no_explicit_ticker"

def _parse_tweet_dt(s):
    """Best-effort parse of a tweet's created_at into an aware UTC datetime. None if unparseable
    (an unparseable timestamp just means that signal won't be merged with anything — safe failure)."""
    if not s:
        return None
    s = s.strip()
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    for fmt in ("%a %b %d %H:%M:%S %z %Y", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None

def _safe_int(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default

def _merge_cluster(cluster):
    """Pick the highest-confidence signal as primary; attach a clean list of every
    contributing account instead of concatenating them into the account field."""
    primary = max(cluster, key=lambda s: (_safe_int(s.get("confidence")), s["_dt"] or datetime.min.replace(tzinfo=timezone.utc)))
    accounts, seen = [], set()
    for s in cluster:
        acct = str(s.get("account", "")).lstrip("@")
        if acct and acct not in seen:
            seen.add(acct)
            accounts.append(acct)
    merged = dict(primary)
    merged["_contributing_accounts"] = accounts
    return merged

def resolve_and_dedup_signals(all_signals):
    """Resolve each signal's ticker, then collapse same-symbol+direction signals whose
    source tweets fall within DEDUP_WINDOW_MINUTES of each other into one."""
    for s in all_signals:
        symbol, ticker_confidence, ticker_method = resolve_symbol(s.get("asset_affected", ""))
        s["_symbol"] = symbol
        s["_ticker_confidence"] = ticker_confidence
        s["_ticker_method"] = ticker_method
        s["_dt"] = _parse_tweet_dt(s.get("_source_created_at"))

    # Only resolved symbols are dedup candidates — N/A signals pass through untouched.
    groups, passthrough = {}, []
    for s in all_signals:
        if not s["_symbol"]:
            passthrough.append(s)
            continue
        groups.setdefault((s["_symbol"], s.get("direction")), []).append(s)

    deduped = []
    for sigs in groups.values():
        sigs.sort(key=lambda s: s["_dt"] or datetime.min.replace(tzinfo=timezone.utc))
        cluster = [sigs[0]]
        for s in sigs[1:]:
            anchor, cur = cluster[0]["_dt"], s["_dt"]
            if anchor and cur and (cur - anchor) <= timedelta(minutes=DEDUP_WINDOW_MINUTES):
                cluster.append(s)
            else:
                deduped.append(_merge_cluster(cluster))
                cluster = [s]
        deduped.append(_merge_cluster(cluster))
    return deduped + passthrough

# ============================================================
# FETCH TWEETS
# ============================================================
def fetch_tweets(username):
    try:
        r = requests.get(
            "https://api.twitterapi.io/twitter/user/last_tweets",
            headers={"X-API-Key": TWITTER_API_KEY},
            params={"userName": username, "count": TWEETS_PER_ACCOUNT},
            timeout=10
        )
        if r.status_code in (401, 403):
            print(f"  [ERROR] @{username}: auth failed HTTP {r.status_code} {r.text[:200]}")
            return []
        raw = r.json().get("data", {}).get("tweets", [])
        tweets = [{"id": t.get("id",""), "text": t.get("text",""), "created_at": t.get("createdAt",""), "username": username} for t in raw]
        if not tweets:
            return []

        # Twitter IDs are snowflake integers (monotonically increasing), so the largest id is the
        # newest. Only keep tweets strictly newer than the high-water mark from the previous run.
        def as_int(tid):
            try:
                return int(tid)
            except (TypeError, ValueError):
                return -1

        last_seen = last_seen_ids.get(username)
        newest_id = max((t["id"] for t in tweets), key=as_int)

        if last_seen is None:
            new_tweets = tweets  # first time this account is seen this process — everything is new
        else:
            new_tweets = [t for t in tweets if as_int(t["id"]) > as_int(last_seen)]

        if newest_id:
            last_seen_ids[username] = newest_id
        return new_tweets
    except Exception as e:
        print(f"  [ERROR] @{username}: {e}")
        return []

# ============================================================
# ANALYSE WITH CLAUDE
# ============================================================
# Static instruction text — byte-identical across every batch call so it can be cached.
# Kept out of analyse_batch() so nothing volatile (tweets, timestamps) leaks into the prefix.
SIGNAL_INSTRUCTIONS = f"""You are an expert financial signal analyst. Analyse these tweets and identify market-moving signals.

STRICT RULES - only flag signals that are DIRECTLY market-moving:
INCLUDE: earnings beats/misses, CEO changes, mergers/acquisitions, central bank decisions, war escalation with specific supply impact, regulatory actions, major macro data releases
EXCLUDE: retweets of general news, social commentary, opinion posts, product demos, general AI hype, vague geopolitical commentary, SpaceX launches

Each tweet below is numbered like "[3] @user (...):". Return a JSON array where each item has:
- "source_index" (the exact [N] of the ONE tweet that generated this signal),
- "account", "tweet_summary",
- "asset_affected" (ONLY include a parenthetical ticker, e.g. "Apple (AAPL)", "Gold (GLD)", if the tweet EXPLICITLY names that specific company, index, or commodity/ETF. If the tweet describes a market-moving event without naming a specific tradable instrument, describe the asset with NO parenthetical ticker — never guess or infer one.),
- "signal_type", "direction" (bullish/bearish/neutral), "confidence" (1-10),
- "price_target" (e.g. "+5%"), "stop_loss" (e.g. "-3%"), "time_horizon" (e.g. "2-3 days"),
- "exit_trigger", "expiry", "conflicting" (true/false), "conflict_note",
- "sentiment_shift" (true/false), "sentiment_note"

Calculate "expiry" as an absolute date/time (e.g. "2026-07-05 14:30 UTC") measured from THAT tweet's own created_at timestamp shown in parentheses next to it, by adding the time_horizon. Do NOT use today's date or any arbitrary date.

Only include confidence >= {MIN_SIGNAL_SCORE}. Be conservative. Return [] if none. JSON only, no extra text."""

def _attach_raw_text(signals, tweets_batch):
    """Attach ground-truth raw tweet text via Claude's source_index — never trust the
    model's own paraphrase for the audit trail."""
    if not isinstance(signals, list):
        return signals
    by_account = {}
    for t in tweets_batch:
        by_account.setdefault(t["username"], []).append(t)
    for s in signals:
        tweet = None
        try:
            idx = int(s.get("source_index"))
            if 1 <= idx <= len(tweets_batch):
                tweet = tweets_batch[idx - 1]
        except (TypeError, ValueError):
            pass
        if tweet is None:
            candidates = by_account.get(str(s.get("account", "")).lstrip("@"), [])
            if len(candidates) == 1:
                tweet = candidates[0]
        s["_raw_tweet_text"] = tweet["text"] if tweet else ""
        s["_source_created_at"] = tweet["created_at"] if tweet else ""
    return signals

def analyse_batch(tweets_batch, prev_signals):
    tweets_text = "\n".join([f"\n[{i+1}] @{t['username']} ({t['created_at']}):\n{t['text']}" for i, t in enumerate(tweets_batch)])
    prev_text = ""
    if prev_signals:
        prev_text = "\n\nPREVIOUS SIGNALS:\n" + "\n".join([f"- {a}: {i['direction']} ({i['confidence']}/10)" for a, i in prev_signals.items()])

    # Only the volatile tweet content goes in the user turn; the cached instructions live in `system`.
    user_content = f"TWEETS:{tweets_text}{prev_text}"

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": CLAUDE_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 1500,
                "system": [
                    {"type": "text", "text": SIGNAL_INSTRUCTIONS, "cache_control": {"type": "ephemeral"}}
                ],
                "messages": [{"role": "user", "content": user_content}]
            },
            timeout=45
        )
        data = r.json()
        if "content" not in data:
            print(f"  [ERROR] Claude API: {data}")
            return []
        raw = data["content"][0]["text"].strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()
        try:
            return _attach_raw_text(json.loads(raw), tweets_batch)
        except:
            start = raw.find("[")
            if start == -1:
                return []
            depth = 0
            for i, c in enumerate(raw[start:], start):
                if c == "[":
                    depth += 1
                elif c == "]":
                    depth -= 1
                    if depth == 0:
                        try:
                            return _attach_raw_text(json.loads(raw[start:i+1]), tweets_batch)
                        except:
                            return []
            return []
    except Exception as e:
        print(f"  [ERROR] Claude batch: {e}")
        return []

# ============================================================
# TELEGRAM
# ============================================================
def send_telegram(message):
    for chat_id in TELEGRAM_CHAT_IDS:
        chat_id = chat_id.strip()
        if not chat_id:
            continue
        try:
            safe_msg = message.replace("&", "and").replace("<", "").replace(">", "")
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": safe_msg},
                timeout=10
            )
            print(f"  [OK] Telegram -> {chat_id}" if r.status_code == 200 else f"  [ERROR] Telegram {chat_id}: {r.text}")
        except Exception as e:
            print(f"  [ERROR] Telegram: {e}")

# ============================================================
# FORMAT ALERT
# ============================================================
def format_signal_alert(signal, symbol, current_price, tp_price=None, sl_price=None, qty=0, traded=False, is_conflict=False):
    emoji = {"bullish": "🟢", "bearish": "🔴", "neutral": "🟡"}.get(signal.get("direction","neutral"), "⚪")
    header = "⚠️ CONFLICTING SIGNAL - NO TRADE" if is_conflict else "🚨 Market Signal Alert"
    price_str = f"${current_price:.2f}" if current_price else "N/A"

    trade_line = ""
    if traded and tp_price and sl_price:
        usd_amount = qty * current_price
        trade_line = f"\n📈 Trade: BUY ${usd_amount:.2f} ({qty} sh) of {symbol} @ {price_str}\n🎯 Take Profit: ${tp_price} | 🛑 Stop Loss: ${sl_price}"

    account = str(signal.get('account','?')).lstrip('@')
    contributors = signal.get("_contributing_accounts") or [account]
    accounts_line = ", ".join(f"@{a}" for a in contributors)
    if len(contributors) > 1:
        accounts_line += f" ({len(contributors)} accounts)"
    msg = (
        f"{header}\n\n"
        f"👤 {accounts_line}\n"
        f"📌 Asset: {signal.get('asset_affected','?')}\n"
        f"🔤 Symbol: {symbol or 'N/A'}\n"
        f"💰 Price: {price_str}\n"
        f"{emoji} Direction: {signal.get('direction','?').upper()}\n"
        f"📝 Signal: {signal.get('tweet_summary','?')}\n\n"
        f"🎯 LLM-Suggested Target: {signal.get('price_target','N/A')}\n"
        f"🛑 LLM-Suggested Stop: {signal.get('stop_loss','N/A')}\n"
        f"⏱ Hold: {signal.get('time_horizon','N/A')}\n"
        f"🚪 Exit: {signal.get('exit_trigger','N/A')}\n"
        f"⌛ Expires: {signal.get('expiry','N/A')}\n"
        f"💡 Confidence: {signal.get('confidence','?')}/10"
        f"{trade_line}"
    )
    if signal.get("conflicting"):
        msg += f"\n⚠️ Conflict: {signal.get('conflict_note','')}"
    if signal.get("sentiment_shift"):
        msg += f"\n🔄 Shift: {signal.get('sentiment_note','')}"
    msg += f"\n🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    return msg

# ============================================================
# MAIN RUN
# ============================================================
def run():
    global previous_signals
    _atr_cache.clear()
    print(f"\n{'='*50}\nMarket Signal Agent - {datetime.now().strftime('%Y-%m-%d %H:%M')}\n{'='*50}")

    print("\n[0/3] Checking outcomes of expired signals...")
    check_outcomes()
    generate_confidence_report()

    open_symbols = get_open_positions()
    print(f"\n  Open positions: {open_symbols or 'None'}")
    check_closed_positions(open_symbols)

    print("  Checking simulated paper positions...")
    check_sim_positions()

    all_tweets = []
    active, skipped = 0, 0
    print("\n[1/3] Fetching tweets...")
    for username in ACCOUNTS:
        print(f"  Fetching @{username}...")
        new_tweets = fetch_tweets(username)
        if new_tweets:
            active += 1
            all_tweets.extend(new_tweets)
        else:
            skipped += 1  # no new tweets since last run (or fetch error) — not sent to Claude
        time.sleep(0.5)

    print(f"\n  Accounts with new tweets: {active} | skipped (no new / error): {skipped}")
    print(f"  Total tweets: {len(all_tweets)}")
    if not all_tweets:
        print("  No tweets fetched.")
        return

    print(f"\n[2/3] Analysing with Claude (batches of {BATCH_SIZE})...")
    all_signals = []
    batch_count = -(-len(all_tweets) // (BATCH_SIZE * TWEETS_PER_ACCOUNT))
    for i in range(0, len(all_tweets), BATCH_SIZE * TWEETS_PER_ACCOUNT):
        batch = all_tweets[i:i + BATCH_SIZE * TWEETS_PER_ACCOUNT]
        print(f"  Batch {i//(BATCH_SIZE*TWEETS_PER_ACCOUNT)+1}/{batch_count}...")
        signals = analyse_batch(batch, previous_signals)
        all_signals.extend(signals)
        time.sleep(1)

    print(f"  Total signals: {len(all_signals)}")
    if not all_signals:
        print("  No significant signals.")
        return

    # Detect conflicts
    conflicts = set()
    asset_dirs = {}
    for s in all_signals:
        a = s.get("asset_affected","")
        asset_dirs.setdefault(a, []).append(s.get("direction",""))
    for a, dirs in asset_dirs.items():
        if "bullish" in dirs and "bearish" in dirs:
            conflicts.add(a)

    print("\n[3/3] Processing signals...")

    for asset in conflicts:
        sigs = [s for s in all_signals if s.get("asset_affected") == asset]
        accounts = ", ".join([f"@{str(s.get('account','')).lstrip('@')}" for s in sigs])
        send_telegram(f"⚠️ CONFLICTING SIGNALS - DO NOT TRADE\n\nAsset: {asset}\nFrom: {accounts}\nRecommendation: Wait for clarity\n🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    pre_dedup_count = len(all_signals)
    all_signals = resolve_and_dedup_signals(all_signals)
    print(f"  Deduplicated {pre_dedup_count} -> {len(all_signals)} signal(s) (window={DEDUP_WINDOW_MINUTES}m)")

    capital = get_account_equity()
    if capital is None or capital <= 0:
        print("  [ERROR] Could not read account equity — skipping all live trading this cycle (signals still logged/alerted).")
        deployed, max_deployed = 0.0, 0.0
    else:
        deployed = get_deployed_capital()
        max_deployed = capital * MAX_TOTAL_DEPLOYED_PCT
        print(f"  Capital: ${capital:.2f} | Deployed: ${deployed:.2f} | Cap: ${max_deployed:.2f} ({MAX_TOTAL_DEPLOYED_PCT*100:.0f}%)")

    sim_capital, sim_deployed, sim_open_symbols = get_sim_state()
    sim_max_deployed = sim_capital * MAX_TOTAL_DEPLOYED_PCT
    print(f"  Sim Capital: ${sim_capital:.2f} | Sim Deployed: ${sim_deployed:.2f} | Sim Cap: ${sim_max_deployed:.2f}")

    traded_symbols = set()
    sim_traded_symbols = set()
    for signal in all_signals:
        asset = signal.get("asset_affected","")
        is_conflict = asset in conflicts
        symbol = signal.get("_symbol")
        ticker_confidence = signal.get("_ticker_confidence", 0)
        ticker_method = signal.get("_ticker_method", "no_explicit_ticker")
        current_price = get_price(symbol) if symbol else None
        contributing_accounts = signal.get("_contributing_accounts") or [str(signal.get("account","")).lstrip("@")]
        direction = signal.get("direction","neutral")

        atr = get_atr(symbol) if symbol and current_price else None
        if atr:
            _, atr_stop_price, atr_target_pct, atr_stop_pct = atr_target_stop(current_price, atr, direction)
        else:
            atr_stop_price = atr_target_pct = atr_stop_pct = None

        # Log every signal to Google Sheets
        log_signal_to_sheets(signal, symbol, current_price, ticker_confidence, ticker_method, signal.get("_raw_tweet_text", ""),
                              contributing_accounts, atr, atr_target_pct, atr_stop_pct)

        order, tp_price, sl_price, qty = None, None, None, 0
        can_trade = symbol and current_price and atr and not is_conflict and direction == "bullish"

        if can_trade and capital and symbol not in open_symbols and symbol not in traded_symbols:
            sized_qty = calculate_position_size(capital, current_price, atr_stop_price, RISK_PCT_PER_TRADE)
            trade_usd = sized_qty * current_price
            if sized_qty <= 0:
                print(f"  [SKIP] {symbol} position size computed as 0")
            elif deployed + trade_usd > max_deployed:
                print(f"  [SKIP] {symbol} would breach total deployed-capital cap: "
                      f"${deployed:.2f} + ${trade_usd:.2f} > ${max_deployed:.2f}")
            else:
                order, tp_price, sl_price, qty = place_bracket_order(
                    symbol, current_price, sized_qty, atr_target_pct, atr_stop_pct
                )
                if order:
                    traded_symbols.add(symbol)
                    deployed += qty * current_price  # reserve for subsequent signals this run
                    log_trade_to_sheets(symbol, current_price, tp_price, sl_price,
                                       round(atr_target_pct, 2), round(atr_stop_pct, 2),
                                       qty * current_price, qty, signal, order.get("id"),
                                       capital, RISK_PCT_PER_TRADE)
        elif can_trade and symbol in open_symbols:
            print(f"  [SKIP] {symbol} already open")
        elif can_trade and symbol in traded_symbols:
            print(f"  [SKIP] {symbol} already traded this run")
        elif symbol and current_price and not atr and not is_conflict and direction == "bullish":
            print(f"  [SKIP] {symbol} no ATR data — can't size a volatility-scaled stop")
        elif can_trade and not capital:
            print(f"  [SKIP] {symbol} no account equity available — can't size live position")

        # Local paper-trading simulation — independent ledger, no Alpaca order calls.
        if can_trade and symbol not in sim_open_symbols and symbol not in sim_traded_symbols:
            sim_entry = simulate_entry_fill(symbol)
            if sim_entry:
                sim_target_price, sim_stop_price, _, _ = atr_target_stop(sim_entry, atr, direction)
                sim_qty = round(calculate_position_size(sim_capital, sim_entry, sim_stop_price, RISK_PCT_PER_TRADE), 6)
                sim_usd = sim_qty * sim_entry
                if sim_qty <= 0:
                    print(f"  [SIM][SKIP] {symbol} position size computed as 0")
                elif sim_deployed + sim_usd > sim_max_deployed:
                    print(f"  [SIM][SKIP] {symbol} would breach sim deployed-capital cap: "
                          f"${sim_deployed:.2f} + ${sim_usd:.2f} > ${sim_max_deployed:.2f}")
                else:
                    log_sim_position_open(symbol, direction, sim_entry, sim_target_price, sim_stop_price,
                                          sim_qty, sim_usd, sim_capital, contributing_accounts,
                                          signal.get("confidence",""), signal.get("expiry",""))
                    sim_traded_symbols.add(symbol)
                    sim_deployed += sim_usd
            else:
                print(f"  [SIM][SKIP] {symbol} no quote available for simulated fill")

        price_str = f"${current_price:.2f}" if current_price else "N/A"
        print(f"\n  -> {asset} | {signal.get('direction')} | {signal.get('confidence')}/10 | {symbol} @ {price_str}")
        send_telegram(format_signal_alert(signal, symbol, current_price, tp_price, sl_price, qty, order is not None, is_conflict))

    previous_signals = {
        s.get("asset_affected",""): {"direction": s.get("direction"), "confidence": s.get("confidence")}
        for s in all_signals
    }

    print(f"\n{'='*50}\nDone.")

if __name__ == "__main__":
    init_sheets()
    while True:
        run()
        print(f"\nSleeping {RUN_INTERVAL_HOURS} hour(s)...\n")
        time.sleep(RUN_INTERVAL_HOURS * 60 * 60)
