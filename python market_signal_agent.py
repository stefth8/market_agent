import requests
import json
import time
import os
import csv
from datetime import datetime, timezone

# ============================================================
# CONFIG — loaded from environment variables (set in Railway)
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

# Commodity/non-stock ETF proxies
ASSET_MAP = {
    "gold": "GLD", "oil": "USO", "wti": "USO", "crude": "USO",
    "silver": "SLV", "natural gas": "UNG", "bitcoin": "IBIT",
    "ethereum": "ETHE", "crypto": "IBIT"
}

TWEETS_PER_ACCOUNT = 3
MIN_SIGNAL_SCORE = 6
RUN_INTERVAL_HOURS = 1
PAPER_TRADE_SIZE = 1000  # USD per trade
LOG_FILE = "/tmp/trades_log.csv"

previous_signals = {}
open_positions = {}  # {symbol: {entry_price, stop_loss_pct, target_pct, direction, time_horizon}}

# ============================================================
# ALPACA HELPERS
# ============================================================
def alpaca_headers():
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        "Content-Type": "application/json"
    }

def get_price(symbol):
    """Get latest price from Alpaca"""
    symbol = symbol.upper().strip()
    try:
        url = f"https://data.alpaca.markets/v2/stocks/{symbol}/quotes/latest"
        r = requests.get(url, headers=alpaca_headers(), timeout=10)
        data = r.json()
        price = data.get("quote", {}).get("ap", 0)  # ask price
        if not price:
            price = data.get("quote", {}).get("bp", 0)  # fallback to bid
        return float(price) if price else None
    except Exception as e:
        print(f"  [ERROR] Getting price for {symbol}: {e}")
        return None

def place_paper_trade(symbol, side, usd_amount):
    """Place a paper trade on Alpaca"""
    symbol = symbol.upper().strip()
    try:
        url = f"{ALPACA_BASE_URL}/orders"
        body = {
            "symbol": symbol,
            "notional": str(usd_amount),
            "side": side,
            "type": "market",
            "time_in_force": "day"
        }
        r = requests.post(url, headers=alpaca_headers(), json=body, timeout=10)
        data = r.json()
        if r.status_code in [200, 201]:
            print(f"  [TRADE] {side.upper()} ${usd_amount} of {symbol} — Order ID: {data.get('id')}")
            return data
        else:
            print(f"  [ERROR] Trade failed for {symbol}: {data}")
            return None
    except Exception as e:
        print(f"  [ERROR] Place trade: {e}")
        return None

def close_position(symbol):
    """Close an open position"""
    symbol = symbol.upper().strip()
    try:
        url = f"{ALPACA_BASE_URL}/positions/{symbol}"
        r = requests.delete(url, headers=alpaca_headers(), timeout=10)
        if r.status_code in [200, 201, 204]:
            print(f"  [CLOSE] Position closed for {symbol}")
            return True
        else:
            print(f"  [ERROR] Close position failed for {symbol}: {r.text}")
            return False
    except Exception as e:
        print(f"  [ERROR] Close position: {e}")
        return False

# ============================================================
# SYMBOL RESOLVER
# ============================================================
def resolve_symbol(asset_affected):
    """Extract a tradeable symbol from asset description"""
    asset_lower = asset_affected.lower()
    
    # Check commodity map
    for keyword, etf in ASSET_MAP.items():
        if keyword in asset_lower:
            return etf
    
    # Extract ticker in parentheses e.g. "Apple (AAPL)"
    import re
    match = re.search(r'\(([A-Z]{1,5})\)', asset_affected)
    if match:
        return match.group(1)
    
    # If all caps word, treat as ticker
    words = asset_affected.split()
    for word in words:
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
            writer.writerow(["timestamp", "action", "symbol", "price", "direction",
                           "confidence", "price_target", "stop_loss", "time_horizon",
                           "account", "result", "pnl_pct"])
        writer.writerow([
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            action, symbol, price,
            signal.get("direction", ""),
            signal.get("confidence", ""),
            signal.get("price_target", ""),
            signal.get("stop_loss", ""),
            signal.get("time_horizon", ""),
            signal.get("account", ""),
            result or "",
            ""
        ])

# ============================================================
# FETCH TWEETS
# ============================================================
def fetch_tweets(username):
    url = "https://api.twitterapi.io/twitter/user/last_tweets"
    headers = {"X-API-Key": TWITTER_API_KEY}
    params = {"userName": username, "count": TWEETS_PER_ACCOUNT}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        data = r.json()
        tweets = data.get("data", {}).get("tweets", [])
        results = []
        for t in tweets:
            results.append({
                "id": t.get("id", ""),
                "text": t.get("text", ""),
                "created_at": t.get("createdAt", ""),
                "username": username
            })
        return results
    except Exception as e:
        print(f"  [ERROR] Fetching @{username}: {e}")
        return []

# ============================================================
# ANALYSE WITH CLAUDE
# ============================================================
def analyse_tweets(tweets_bundle, prev_signals):
    tweets_text = ""
    for t in tweets_bundle:
        tweets_text += f"\n@{t['username']} ({t['created_at']}):\n{t['text']}\n"

    prev_text = ""
    if prev_signals:
        prev_text = "\n\nPREVIOUS SIGNALS FROM LAST RUN:\n"
        for asset, info in prev_signals.items():
            prev_text += f"- {asset}: was {info['direction']} (confidence {info['confidence']}/10)\n"

    prompt = f"""You are an expert financial signal analyst. Analyse the following tweets and identify market-moving signals.

For each significant signal, return a JSON array where each item has:
- "account": Twitter username
- "tweet_summary": one sentence summary
- "asset_affected": specific asset with ticker in parentheses where possible (e.g. "Apple (AAPL)", "Bitcoin (BTC)", "Gold (GLD)", "Oil (USO)")
- "signal_type": one of: "earnings_beat", "earnings_miss", "leadership_change", "macro_bearish", "macro_bullish", "safe_haven", "crypto_signal", "sector_upgrade", "sector_downgrade", "regulatory", "conflict_signal"
- "direction": "bullish", "bearish", or "neutral"
- "confidence": 1-10
- "price_target": short-term % move expected (e.g. "+5%" or "-8%")
- "stop_loss": suggested stop loss % (e.g. "-3%")
- "time_horizon": how long to hold (e.g. "4 hours", "2-3 days", "1 week")
- "exit_trigger": specific condition to exit
- "expiry": when this signal expires
- "conflicting": true/false
- "conflict_note": explanation if conflicting
- "sentiment_shift": true/false
- "sentiment_note": explanation if sentiment shifted

Only include signals with confidence >= {MIN_SIGNAL_SCORE}.
If no signals found, return: []
Return valid JSON only, no extra text.

TWEETS:
{tweets_text}
{prev_text}
"""

    headers = {
        "x-api-key": CLAUDE_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    body = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 2000,
        "messages": [{"role": "user", "content": prompt}]
    }
    try:
        r = requests.post("https://api.anthropic.com/v1/messages", headers=headers, json=body, timeout=45)
        data = r.json()
        raw = data["content"][0]["text"].strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except Exception as e:
        print(f"  [ERROR] Claude analysis: {e}")
        return []

# ============================================================
# CONFLICT DETECTION
# ============================================================
def detect_conflicts(signals):
    asset_directions = {}
    for s in signals:
        asset = s.get("asset_affected", "")
        direction = s.get("direction", "")
        if asset not in asset_directions:
            asset_directions[asset] = []
        asset_directions[asset].append(direction)
    conflicts = set()
    for asset, directions in asset_directions.items():
        if "bullish" in directions and "bearish" in directions:
            conflicts.add(asset)
    return conflicts

# ============================================================
# TELEGRAM
# ============================================================
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for chat_id in TELEGRAM_CHAT_IDS:
        chat_id = chat_id.strip()
        if not chat_id:
            continue
        try:
            r = requests.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}, timeout=10)
            if r.status_code == 200:
                print(f"  [OK] Alert sent to {chat_id}")
            else:
                print(f"  [ERROR] Telegram {chat_id}: {r.text}")
        except Exception as e:
            print(f"  [ERROR] Telegram: {e}")

# ============================================================
# FORMAT ALERTS
# ============================================================
def format_signal_alert(signal, symbol, current_price, order=None, is_conflict=False):
    emoji = {"bullish": "🟢", "bearish": "🔴", "neutral": "🟡"}.get(signal.get("direction", "neutral"), "⚪")
    header = "⚠️ *CONFLICTING SIGNAL — NO TRADE*" if is_conflict else "🚨 *Market Signal Alert*"
    trade_line = ""
    if order and not is_conflict:
        side = "BUY" if signal.get("direction") == "bullish" else "SELL"
        trade_line = f"\n📈 *Paper Trade:* {side} ${PAPER_TRADE_SIZE} of {symbol} @ ${current_price:.2f}"

    return (
        f"{header}\n\n"
        f"👤 @{signal.get('account','?')}\n"
        f"📌 *Asset:* {signal.get('asset_affected','?')}\n"
        f"🔤 *Symbol:* {symbol or 'N/A'}\n"
        f"💰 *Current Price:* ${current_price:.2f}" if current_price else f"💰 *Current Price:* N/A"
        f"\n{emoji} *Direction:* {signal.get('direction','?').upper()}\n"
        f"📝 *Signal:* {signal.get('tweet_summary','?')}\n\n"
        f"🎯 *Price Target:* {signal.get('price_target','N/A')}\n"
        f"🛑 *Stop Loss:* {signal.get('stop_loss','N/A')}\n"
        f"⏱ *Time Horizon:* {signal.get('time_horizon','N/A')}\n"
        f"🚪 *Exit When:* {signal.get('exit_trigger','N/A')}\n"
        f"⌛ *Signal Expires:* {signal.get('expiry','N/A')}\n"
        f"💡 *Confidence:* {signal.get('confidence','?')}/10"
        f"{trade_line}\n"
        + (f"\n⚠️ *Conflict:* {signal.get('conflict_note','')}\n" if signal.get("conflicting") else "")
        + (f"\n🔄 *Sentiment Shift:* {signal.get('sentiment_note','')}\n" if signal.get("sentiment_shift") else "")
        + f"\n🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )

def format_conflict_warning(asset, signals_for_asset):
    accounts = ", ".join([f"@{s.get('account')}" for s in signals_for_asset])
    directions = ", ".join([s.get("direction","?") for s in signals_for_asset])
    return (
        f"⚠️ *CONFLICTING SIGNALS — DO NOT TRADE*\n\n"
        f"📌 *Asset:* {asset}\n"
        f"🔀 *Conflict:* {directions}\n"
        f"👥 *From:* {accounts}\n"
        f"💡 *Recommendation:* Wait for clarity\n"
        f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )

def format_exit_alert(symbol, entry_price, exit_price, reason, pnl_pct):
    emoji = "✅" if pnl_pct > 0 else "❌"
    return (
        f"{emoji} *Position Closed*\n\n"
        f"🔤 *Symbol:* {symbol}\n"
        f"📥 *Entry:* ${entry_price:.2f}\n"
        f"📤 *Exit:* ${exit_price:.2f}\n"
        f"📊 *P&L:* {pnl_pct:+.2f}%\n"
        f"💡 *Reason:* {reason}\n"
        f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )

# ============================================================
# CHECK & EXIT OPEN POSITIONS
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
        pnl_pct = ((current_price - entry) / entry) * 100
        if pos["direction"] == "bearish":
            pnl_pct = -pnl_pct

        stop_pct = float(pos["stop_loss_pct"].replace("%","").replace("-","")) * -1
        target_pct = float(pos["target_pct"].replace("%","").replace("+",""))

        reason = None
        if pnl_pct <= stop_pct:
            reason = f"Stop loss hit ({pnl_pct:+.2f}%)"
        elif pnl_pct >= target_pct:
            reason = f"Target reached ({pnl_pct:+.2f}%)"

        if reason:
            closed = close_position(symbol)
            if closed:
                msg = format_exit_alert(symbol, entry, current_price, reason, pnl_pct)
                send_telegram(msg)
                log_trade("EXIT", symbol, current_price, pos.get("signal", {}), result=reason)
                del open_positions[symbol]
                print(f"  [EXIT] {symbol} — {reason}")

# ============================================================
# MAIN RUN
# ============================================================
def run():
    global previous_signals, open_positions

    print(f"\n{'='*50}")
    print(f"Market Signal Agent — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}")

    # Check existing positions first
    check_open_positions()

    all_tweets = []
    print("\n[1/3] Fetching tweets...")
    for username in ACCOUNTS:
        print(f"  Fetching @{username}...")
        tweets = fetch_tweets(username)
        all_tweets.extend(tweets)
        time.sleep(0.5)

    print(f"\n  Total tweets fetched: {len(all_tweets)}")
    if not all_tweets:
        print("  No tweets fetched.")
        return

    print("\n[2/3] Analysing with Claude...")
    signals = analyse_tweets(all_tweets, previous_signals)
    print(f"  Signals found: {len(signals)}")

    if not signals:
        print("  No significant signals this run.")
        return

    conflicts = detect_conflicts(signals)
    asset_signals = {}
    for s in signals:
        asset = s.get("asset_affected", "")
        if asset not in asset_signals:
            asset_signals[asset] = []
        asset_signals[asset].append(s)

    print("\n[3/3] Processing signals...")

    # Send conflict warnings
    for asset in conflicts:
        msg = format_conflict_warning(asset, asset_signals[asset])
        send_telegram(msg)
        print(f"  ⚠️ CONFLICT on {asset} — no trade placed")

    # Process individual signals
    for signal in signals:
        asset = signal.get("asset_affected", "")
        is_conflict = asset in conflicts
        symbol = resolve_symbol(asset)
        current_price = get_price(symbol) if symbol else None
        order = None

        if symbol and current_price and not is_conflict:
            direction = signal.get("direction", "neutral")
            if direction == "bullish":
                order = place_paper_trade(symbol, "buy", PAPER_TRADE_SIZE)
                if order:
                    open_positions[symbol] = {
                        "entry_price": current_price,
                        "stop_loss_pct": signal.get("stop_loss", "-3%"),
                        "target_pct": signal.get("price_target", "+5%"),
                        "direction": direction,
                        "time_horizon": signal.get("time_horizon", ""),
                        "signal": signal
                    }
                    log_trade("ENTRY", symbol, current_price, signal)
            elif direction == "bearish" and symbol not in open_positions:
                # For bearish, we skip short selling for now — just log and alert
                log_trade("SIGNAL_BEARISH", symbol, current_price or 0, signal)

        msg = format_signal_alert(signal, symbol, current_price or 0, order, is_conflict)
        print(f"\n  → {asset} | {signal.get('direction')} | {signal.get('confidence')}/10 | {symbol} @ ${current_price:.2f if current_price else 'N/A'}")
        send_telegram(msg)

    # Update previous signals
    previous_signals = {}
    for s in signals:
        asset = s.get("asset_affected", "")
        previous_signals[asset] = {
            "direction": s.get("direction"),
            "confidence": s.get("confidence")
        }

    print(f"\n{'='*50}\nDone.")

if __name__ == "__main__":
    while True:
        run()
        print(f"\nSleeping {RUN_INTERVAL_HOURS} hour(s)...\n")
        time.sleep(RUN_INTERVAL_HOURS * 60 * 60)
