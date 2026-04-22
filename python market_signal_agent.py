import requests
import json
import time
import os
import re
from datetime import datetime, timezone

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
PAPER_TRADE_SIZE = 1000
BATCH_SIZE = 5

previous_signals = {}
gspread_client = None

# ============================================================
# GOOGLE SHEETS (using gspread)
# ============================================================
def get_sheets_client():
    global gspread_client
    if gspread_client:
        return gspread_client
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gspread_client = gspread.authorize(creds)
        print("  [SHEETS] Connected to Google Sheets")
        return gspread_client
    except Exception as e:
        print(f"  [ERROR] Google Sheets client: {e}")
        return None

def sheets_append(sheet_name, row):
    try:
        client = get_sheets_client()
        if not client:
            return
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sheet.worksheet(sheet_name)
        worksheet.append_row(row)
        print(f"  [SHEETS] Logged to {sheet_name}")
    except Exception as e:
        print(f"  [ERROR] Sheets append: {e}")

def init_sheets():
    try:
        client = get_sheets_client()
        if not client:
            return
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        
        # Init Signals sheet
        try:
            ws = sheet.worksheet("Signals")
        except:
            ws = sheet.add_worksheet("Signals", 1000, 20)
        if not ws.row_values(1):
            ws.append_row(["Timestamp","Account","Asset","Symbol","Direction","Confidence","Price","Target","Stop Loss","Time Horizon","Exit Trigger","Expiry","Signal Type","Conflict","Sentiment Shift"])

        # Init Trades sheet
        try:
            ws2 = sheet.worksheet("Trades")
        except:
            ws2 = sheet.add_worksheet("Trades", 1000, 20)
        if not ws2.row_values(1):
            ws2.append_row(["Timestamp","Action","Symbol","Entry Price","Take Profit Price","Stop Loss Price","Target %","Stop %","USD Amount","Qty","Account","Asset","Order ID"])
        
        print("  [SHEETS] Sheets initialized")
    except Exception as e:
        print(f"  [ERROR] Init sheets: {e}")

def log_signal_to_sheets(signal, symbol, current_price):
    try:
        sheets_append("Signals", [
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            f"@{signal.get('account','')}",
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
            "YES" if signal.get("sentiment_shift") else "NO"
        ])
    except Exception as e:
        print(f"  [ERROR] Log signal: {e}")

def log_trade_to_sheets(symbol, entry_price, tp_price, sl_price, target_pct, stop_pct, qty, signal, order_id):
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
            f"${PAPER_TRADE_SIZE}",
            str(qty),
            f"@{signal.get('account','')}",
            signal.get("asset_affected",""),
            order_id or ""
        ])
    except Exception as e:
        print(f"  [ERROR] Log trade: {e}")

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

def place_bracket_order(symbol, current_price, usd_amount, target_pct_str, stop_pct_str):
    try:
        qty = round(usd_amount / current_price, 6)
        if qty <= 0:
            return None, 0, 0, 0

        target_pct = parse_pct(target_pct_str, 5.0)
        stop_pct = parse_pct(stop_pct_str, 3.0)
        tp_price = round(current_price * (1 + target_pct / 100), 2)
        sl_price = round(current_price * (1 - stop_pct / 100), 2)

        print(f"  [BRACKET] {symbol} qty={qty} @ ${current_price} | TP=${tp_price} | SL=${sl_price}")

        body = {
            "symbol": symbol.upper(),
            "qty": str(qty),
            "side": "buy",
            "type": "market",
            "time_in_force": "gtc",
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
    asset_lower = asset_affected.lower()
    for keyword, etf in ASSET_MAP.items():
        if keyword in asset_lower:
            return etf
    match = re.search(r'\(([A-Z]{1,5})\)', asset_affected)
    if match:
        return match.group(1)
    for word in asset_affected.split():
        clean = word.strip("().,")
        if clean.isupper() and 1 <= len(clean) <= 5:
            return clean
    return None

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
        tweets = r.json().get("data", {}).get("tweets", [])
        return [{"id": t.get("id",""), "text": t.get("text",""), "created_at": t.get("createdAt",""), "username": username} for t in tweets]
    except Exception as e:
        print(f"  [ERROR] @{username}: {e}")
        return []

# ============================================================
# ANALYSE WITH CLAUDE
# ============================================================
def analyse_batch(tweets_batch, prev_signals):
    tweets_text = "\n".join([f"\n@{t['username']} ({t['created_at']}):\n{t['text']}" for t in tweets_batch])
    prev_text = ""
    if prev_signals:
        prev_text = "\n\nPREVIOUS SIGNALS:\n" + "\n".join([f"- {a}: {i['direction']} ({i['confidence']}/10)" for a, i in prev_signals.items()])

    prompt = f"""You are an expert financial signal analyst. Analyse these tweets and identify market-moving signals.

STRICT RULES - only flag signals that are DIRECTLY market-moving:
INCLUDE: earnings beats/misses, CEO changes, mergers/acquisitions, central bank decisions, war escalation with specific supply impact, regulatory actions, major macro data releases
EXCLUDE: retweets of general news, social commentary, opinion posts, product demos, general AI hype, vague geopolitical commentary, SpaceX launches

Return a JSON array where each item has:
- "account", "tweet_summary", "asset_affected" (specific ticker e.g. "Apple (AAPL)", "Gold (GLD)"),
- "signal_type", "direction" (bullish/bearish/neutral), "confidence" (1-10),
- "price_target" (e.g. "+5%"), "stop_loss" (e.g. "-3%"), "time_horizon" (e.g. "2-3 days"),
- "exit_trigger", "expiry", "conflicting" (true/false), "conflict_note",
- "sentiment_shift" (true/false), "sentiment_note"

Only include confidence >= {MIN_SIGNAL_SCORE}. Be conservative. Return [] if none. JSON only, no extra text.

TWEETS:{tweets_text}{prev_text}"""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": CLAUDE_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 1500, "messages": [{"role": "user", "content": prompt}]},
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
            return json.loads(raw)
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
                            return json.loads(raw[start:i+1])
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
def format_signal_alert(signal, symbol, current_price, tp_price=None, sl_price=None, traded=False, is_conflict=False):
    emoji = {"bullish": "🟢", "bearish": "🔴", "neutral": "🟡"}.get(signal.get("direction","neutral"), "⚪")
    header = "⚠️ CONFLICTING SIGNAL - NO TRADE" if is_conflict else "🚨 Market Signal Alert"
    price_str = f"${current_price:.2f}" if current_price else "N/A"

    trade_line = ""
    if traded and tp_price and sl_price:
        trade_line = f"\n📈 Paper Trade: BUY ${PAPER_TRADE_SIZE} of {symbol} @ {price_str}\n🎯 Take Profit: ${tp_price} | 🛑 Stop Loss: ${sl_price}"

    msg = (
        f"{header}\n\n"
        f"👤 @{signal.get('account','?')}\n"
        f"📌 Asset: {signal.get('asset_affected','?')}\n"
        f"🔤 Symbol: {symbol or 'N/A'}\n"
        f"💰 Price: {price_str}\n"
        f"{emoji} Direction: {signal.get('direction','?').upper()}\n"
        f"📝 Signal: {signal.get('tweet_summary','?')}\n\n"
        f"🎯 Target: {signal.get('price_target','N/A')}\n"
        f"🛑 Stop Loss: {signal.get('stop_loss','N/A')}\n"
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
    print(f"\n{'='*50}\nMarket Signal Agent - {datetime.now().strftime('%Y-%m-%d %H:%M')}\n{'='*50}")

    open_symbols = get_open_positions()
    print(f"\n  Open positions: {open_symbols or 'None'}")

    all_tweets = []
    print("\n[1/3] Fetching tweets...")
    for username in ACCOUNTS:
        print(f"  Fetching @{username}...")
        all_tweets.extend(fetch_tweets(username))
        time.sleep(0.5)

    print(f"\n  Total tweets: {len(all_tweets)}")
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
        accounts = ", ".join([f"@{s.get('account')}" for s in sigs])
        send_telegram(f"⚠️ CONFLICTING SIGNALS - DO NOT TRADE\n\nAsset: {asset}\nFrom: {accounts}\nRecommendation: Wait for clarity\n🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    traded_symbols = set()
    for signal in all_signals:
        asset = signal.get("asset_affected","")
        is_conflict = asset in conflicts
        symbol = resolve_symbol(asset)
        current_price = get_price(symbol) if symbol else None

        # Log every signal to Google Sheets
        log_signal_to_sheets(signal, symbol, current_price)

        order, tp_price, sl_price, qty = None, None, None, 0

        if symbol and current_price and not is_conflict:
            direction = signal.get("direction","neutral")
            if direction == "bullish" and symbol not in open_symbols and symbol not in traded_symbols:
                target_pct_str = signal.get("price_target", "+5%")
                stop_pct_str = signal.get("stop_loss", "-3%")
                order, tp_price, sl_price, qty = place_bracket_order(
                    symbol, current_price, PAPER_TRADE_SIZE, target_pct_str, stop_pct_str
                )
                if order:
                    traded_symbols.add(symbol)
                    log_trade_to_sheets(symbol, current_price, tp_price, sl_price,
                                       parse_pct(target_pct_str), parse_pct(stop_pct_str),
                                       qty, signal, order.get("id"))
            elif symbol in open_symbols:
                print(f"  [SKIP] {symbol} already open")
            elif symbol in traded_symbols:
                print(f"  [SKIP] {symbol} already traded this run")

        price_str = f"${current_price:.2f}" if current_price else "N/A"
        print(f"\n  -> {asset} | {signal.get('direction')} | {signal.get('confidence')}/10 | {symbol} @ {price_str}")
        send_telegram(format_signal_alert(signal, symbol, current_price, tp_price, sl_price, order is not None, is_conflict))

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
