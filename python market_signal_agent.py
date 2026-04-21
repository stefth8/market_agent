import requests
import json
import time
import os
import csv
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
MIN_SIGNAL_SCORE = 6
RUN_INTERVAL_HOURS = 1
PAPER_TRADE_SIZE = 1000
LOG_FILE = "/tmp/trades_log.csv"
BATCH_SIZE = 5  # accounts per Claude call

previous_signals = {}
open_positions = {}

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

def place_paper_trade(symbol, side, usd_amount):
    try:
        url = f"{ALPACA_BASE_URL}/orders"
        body = {"symbol": symbol.upper(), "notional": str(usd_amount), "side": side, "type": "market", "time_in_force": "day"}
        r = requests.post(url, headers=alpaca_headers(), json=body, timeout=10)
        data = r.json()
        if r.status_code in [200, 201]:
            print(f"  [TRADE] {side.upper()} ${usd_amount} of {symbol} — ID: {data.get('id')}")
            return data
        else:
            print(f"  [ERROR] Trade failed {symbol}: {data}")
            return None
    except Exception as e:
        print(f"  [ERROR] Trade: {e}")
        return None

def close_position(symbol):
    try:
        r = requests.delete(f"{ALPACA_BASE_URL}/positions/{symbol.upper()}", headers=alpaca_headers(), timeout=10)
        if r.status_code in [200, 201, 204]:
            print(f"  [CLOSE] {symbol} closed")
            return True
        print(f"  [ERROR] Close {symbol}: {r.text}")
        return False
    except Exception as e:
        print(f"  [ERROR] Close: {e}")
        return False

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
# TRADE LOGGER
# ============================================================
def log_trade(action, symbol, price, signal, result=None):
    file_exists = os.path.exists(LOG_FILE)
    with open(LOG_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp","action","symbol","price","direction","confidence","price_target","stop_loss","time_horizon","account","result"])
        writer.writerow([
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            action, symbol, price,
            signal.get("direction",""), signal.get("confidence",""),
            signal.get("price_target",""), signal.get("stop_loss",""),
            signal.get("time_horizon",""), signal.get("account",""),
            result or ""
        ])

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
# ANALYSE BATCH WITH CLAUDE
# ============================================================
def analyse_batch(tweets_batch, prev_signals):
    tweets_text = "\n".join([f"\n@{t['username']} ({t['created_at']}):\n{t['text']}" for t in tweets_batch])

    prev_text = ""
    if prev_signals:
        prev_text = "\n\nPREVIOUS SIGNALS:\n" + "\n".join([f"- {a}: {i['direction']} ({i['confidence']}/10)" for a, i in prev_signals.items()])

    prompt = f"""You are an expert financial signal analyst. Analyse these tweets and identify market-moving signals.

Return a JSON array where each item has:
- "account", "tweet_summary", "asset_affected" (include ticker e.g. "Apple (AAPL)", "Gold (GLD)"),
- "signal_type", "direction" (bullish/bearish/neutral), "confidence" (1-10),
- "price_target" (e.g. "+5%"), "stop_loss" (e.g. "-3%"), "time_horizon" (e.g. "2-3 days"),
- "exit_trigger", "expiry", "conflicting" (true/false), "conflict_note",
- "sentiment_shift" (true/false), "sentiment_note"

Only include confidence >= {MIN_SIGNAL_SCORE}. Return [] if none. JSON only, no extra text.

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
        return json.loads(raw.strip())
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
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"},
                timeout=10
            )
            print(f"  [OK] Telegram → {chat_id}" if r.status_code == 200 else f"  [ERROR] Telegram {chat_id}: {r.text}")
        except Exception as e:
            print(f"  [ERROR] Telegram: {e}")

# ============================================================
# FORMAT ALERTS
# ============================================================
def format_signal_alert(signal, symbol, current_price, order=None, is_conflict=False):
    emoji = {"bullish": "🟢", "bearish": "🔴", "neutral": "🟡"}.get(signal.get("direction","neutral"), "⚪")
    header = "⚠️ *CONFLICTING SIGNAL — NO TRADE*" if is_conflict else "🚨 *Market Signal Alert*"
    price_str = f"${current_price:.2f}" if current_price else "N/A"
    trade_line = ""
    if order and not is_conflict:
        side = "BUY" if signal.get("direction") == "bullish" else "SELL"
        trade_line = f"\n📈 *Paper Trade:* {side} ${PAPER_TRADE_SIZE} of {symbol} @ {price_str}"

    msg = (
        f"{header}\n\n"
        f"👤 @{signal.get('account','?')}\n"
        f"📌 *Asset:* {signal.get('asset_affected','?')}\n"
        f"🔤 *Symbol:* {symbol or 'N/A'}\n"
        f"💰 *Price:* {price_str}\n"
        f"{emoji} *Direction:* {signal.get('direction','?').upper()}\n"
        f"📝 *Signal:* {signal.get('tweet_summary','?')}\n\n"
        f"🎯 *Target:* {signal.get('price_target','N/A')}\n"
        f"🛑 *Stop Loss:* {signal.get('stop_loss','N/A')}\n"
        f"⏱ *Hold:* {signal.get('time_horizon','N/A')}\n"
        f"🚪 *Exit:* {signal.get('exit_trigger','N/A')}\n"
        f"⌛ *Expires:* {signal.get('expiry','N/A')}\n"
        f"💡 *Confidence:* {signal.get('confidence','?')}/10"
        f"{trade_line}"
    )
    if signal.get("conflicting"):
        msg += f"\n⚠️ *Conflict:* {signal.get('conflict_note','')}"
    if signal.get("sentiment_shift"):
        msg += f"\n🔄 *Shift:* {signal.get('sentiment_note','')}"
    msg += f"\n🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    return msg

def format_exit_alert(symbol, entry, exit_price, reason, pnl):
    emoji = "✅" if pnl > 0 else "❌"
    return (
        f"{emoji} *Position Closed*\n\n"
        f"🔤 *Symbol:* {symbol}\n"
        f"📥 *Entry:* ${entry:.2f}\n"
        f"📤 *Exit:* ${exit_price:.2f}\n"
        f"📊 *P&L:* {pnl:+.2f}%\n"
        f"💡 *Reason:* {reason}\n"
        f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )

# ============================================================
# CHECK OPEN POSITIONS
# ============================================================
def check_open_positions():
    if not open_positions:
        return
    print(f"\n[POSITIONS] Checking {len(open_positions)} open position(s)...")
    for symbol, pos in list(open_positions.items()):
        current_price = get_price(symbol)
        if not current_price:
            continue
        entry = pos["entry_price"]
        pnl = ((current_price - entry) / entry) * 100
        if pos["direction"] == "bearish":
            pnl = -pnl
        try:
            stop = -abs(float(pos["stop_loss_pct"].replace("%","").replace("-","")))
            target = abs(float(pos["target_pct"].replace("%","").replace("+","")))
        except:
            stop, target = -3.0, 5.0

        reason = None
        if pnl <= stop:
            reason = f"Stop loss hit ({pnl:+.2f}%)"
        elif pnl >= target:
            reason = f"Target reached ({pnl:+.2f}%)"

        if reason:
            if close_position(symbol):
                send_telegram(format_exit_alert(symbol, entry, current_price, reason, pnl))
                log_trade("EXIT", symbol, current_price, pos.get("signal", {}), result=reason)
                del open_positions[symbol]

# ============================================================
# MAIN RUN
# ============================================================
def run():
    global previous_signals, open_positions
    print(f"\n{'='*50}\nMarket Signal Agent — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n{'='*50}")

    check_open_positions()

    # Fetch tweets
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

    # Analyse in batches
    print(f"\n[2/3] Analysing with Claude (batch size: {BATCH_SIZE} accounts)...")
    all_signals = []
    for i in range(0, len(all_tweets), BATCH_SIZE * TWEETS_PER_ACCOUNT):
        batch = all_tweets[i:i + BATCH_SIZE * TWEETS_PER_ACCOUNT]
        print(f"  Batch {i // (BATCH_SIZE * TWEETS_PER_ACCOUNT) + 1}/{-(-len(all_tweets) // (BATCH_SIZE * TWEETS_PER_ACCOUNT))}...")
        signals = analyse_batch(batch, previous_signals)
        all_signals.extend(signals)
        time.sleep(1)  # avoid rate limits

    print(f"  Total signals found: {len(all_signals)}")
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

    # Send conflict warnings
    for asset in conflicts:
        sigs = [s for s in all_signals if s.get("asset_affected") == asset]
        accounts = ", ".join([f"@{s.get('account')}" for s in sigs])
        send_telegram(
            f"⚠️ *CONFLICTING SIGNALS — DO NOT TRADE*\n\n"
            f"📌 *Asset:* {asset}\n"
            f"👥 *From:* {accounts}\n"
            f"💡 Wait for clarity\n"
            f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
        )
        print(f"  ⚠️ CONFLICT: {asset}")

    # Process signals
    for signal in all_signals:
        asset = signal.get("asset_affected","")
        is_conflict = asset in conflicts
        symbol = resolve_symbol(asset)
        current_price = get_price(symbol) if symbol else None
        order = None

        if symbol and current_price and not is_conflict:
            direction = signal.get("direction","neutral")
            if direction == "bullish":
                order = place_paper_trade(symbol, "buy", PAPER_TRADE_SIZE)
                if order:
                    open_positions[symbol] = {
                        "entry_price": current_price,
                        "stop_loss_pct": signal.get("stop_loss", "-3%"),
                        "target_pct": signal.get("price_target", "+5%"),
                        "direction": direction,
                        "signal": signal
                    }
                    log_trade("ENTRY", symbol, current_price, signal)
            elif direction == "bearish":
                log_trade("SIGNAL_BEARISH", symbol, current_price, signal)

        price_str = f"${current_price:.2f}" if current_price else "N/A"
        print(f"\n  → {asset} | {signal.get('direction')} | {signal.get('confidence')}/10 | {symbol} @ {price_str}")
        send_telegram(format_signal_alert(signal, symbol, current_price, order, is_conflict))

    # Update previous signals
    previous_signals = {
        s.get("asset_affected",""): {"direction": s.get("direction"), "confidence": s.get("confidence")}
        for s in all_signals
    }

    print(f"\n{'='*50}\nDone.")

if __name__ == "__main__":
    while True:
        run()
        print(f"\nSleeping {RUN_INTERVAL_HOURS} hour(s)...\n")
        time.sleep(RUN_INTERVAL_HOURS * 60 * 60)
