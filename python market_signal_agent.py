import requests
import json
import time
import os
from datetime import datetime, timezone

# ============================================================
# CONFIG — loaded from environment variables (set in Railway)
# ============================================================
TWITTER_API_KEY = os.environ.get("TWITTER_API_KEY", "")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_IDS = os.environ.get("TELEGRAM_CHAT_IDS", "").split(",")

ACCOUNTS = [
    "elonmusk", "realDonaldTrump", "federalreserve", "jpmorgan",
    "GoldmanSachs", "michael_saylor", "WarrenBuffett",
    "nouriel", "RayDalio", "PeterSchiff",
    "VitalikButerin", "cz_binance", "SEC_News", "IMFNews",
    "business", "Reuters", "WSJ", "zerohedge",
    "thelonginvest", "unusual_whales"
]

TWEETS_PER_ACCOUNT = 3
MIN_SIGNAL_SCORE = 6
RUN_INTERVAL_HOURS = 1

# Tracks signals from previous run to detect follow-up changes
previous_signals = {}

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

def analyse_tweets(tweets_bundle, prev_signals):
    tweets_text = ""
    for t in tweets_bundle:
        tweets_text += f"\n@{t['username']} ({t['created_at']}):\n{t['text']}\n"

    prev_text = ""
    if prev_signals:
        prev_text = "\n\nPREVIOUS SIGNALS FROM LAST RUN (check if sentiment has shifted):\n"
        for asset, info in prev_signals.items():
            prev_text += f"- {asset}: was {info['direction']} (confidence {info['confidence']}/10)\n"

    prompt = f"""You are an expert financial signal analyst. Analyse the following tweets and identify market-moving signals.

For each significant signal, return a JSON array where each item has:
- "account": Twitter username
- "tweet_summary": one sentence summary
- "asset_affected": specific asset (e.g. "Tesla (TSLA)", "Bitcoin (BTC)", "Gold", "S&P 500")
- "signal_type": one of: "earnings_beat", "earnings_miss", "leadership_change", "macro_bearish", "macro_bullish", "safe_haven", "crypto_signal", "sector_upgrade", "sector_downgrade", "regulatory", "conflict_signal"
- "direction": "bullish", "bearish", or "neutral"
- "confidence": 1-10
- "price_target": short-term % move expected (e.g. "+5%" or "-8%")
- "stop_loss": suggested stop loss % (e.g. "-3%")
- "time_horizon": how long to hold (e.g. "4 hours", "2-3 days", "1 week")
- "exit_trigger": specific condition to exit (e.g. "Exit if price drops 3% from entry", "Exit after 5 days", "Exit when Fed contradicts")
- "expiry": when this signal expires and should be ignored (e.g. "4 hours", "24 hours", "end of week")
- "conflicting": true/false — is there a conflicting signal from another account on same asset?
- "conflict_note": if conflicting=true, explain the conflict briefly
- "sentiment_shift": true/false — has sentiment shifted vs previous run on this asset?
- "sentiment_note": if sentiment_shift=true, explain what changed

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

def detect_conflicts(signals):
    """Flag assets where multiple signals conflict"""
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

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for chat_id in TELEGRAM_CHAT_IDS:
        chat_id = chat_id.strip()
        if not chat_id:
            continue
        try:
            r = requests.post(url, json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown"
            }, timeout=10)
            if r.status_code == 200:
                print(f"  [OK] Alert sent to {chat_id}")
            else:
                print(f"  [ERROR] Telegram {chat_id}: {r.text}")
        except Exception as e:
            print(f"  [ERROR] Telegram: {e}")

def format_alert(signal, is_conflict=False, is_warning=False):
    dir_emoji = {"bullish": "🟢", "bearish": "🔴", "neutral": "🟡"}.get(signal.get("direction", "neutral"), "⚪")
    header = "⚠️ *CONFLICTING SIGNAL WARNING*" if is_conflict else "🚨 *Market Signal Alert*"

    msg = (
        f"{header}\n\n"
        f"👤 @{signal.get('account','?')}\n"
        f"📌 *Asset:* {signal.get('asset_affected','?')}\n"
        f"{dir_emoji} *Direction:* {signal.get('direction','?').upper()}\n"
        f"📝 *Signal:* {signal.get('tweet_summary','?')}\n\n"
        f"🎯 *Price Target:* {signal.get('price_target','N/A')}\n"
        f"🛑 *Stop Loss:* {signal.get('stop_loss','N/A')}\n"
        f"⏱ *Time Horizon:* {signal.get('time_horizon','N/A')}\n"
        f"🚪 *Exit When:* {signal.get('exit_trigger','N/A')}\n"
        f"⌛ *Signal Expires:* {signal.get('expiry','N/A')}\n\n"
        f"💡 *Confidence:* {signal.get('confidence','?')}/10\n"
    )

    if signal.get("conflicting"):
        msg += f"\n⚠️ *Conflict:* {signal.get('conflict_note','Conflicting signals detected')}\n"

    if signal.get("sentiment_shift"):
        msg += f"\n🔄 *Sentiment Shift:* {signal.get('sentiment_note','Sentiment changed vs last run')}\n"

    msg += f"\n🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    return msg

def format_conflict_warning(asset, signals_for_asset):
    accounts = ", ".join([f"@{s.get('account')}" for s in signals_for_asset])
    directions = ", ".join([s.get("direction","?") for s in signals_for_asset])
    return (
        f"⚠️ *CONFLICTING SIGNALS — DO NOT TRADE*\n\n"
        f"📌 *Asset:* {asset}\n"
        f"🔀 *Conflict:* {directions}\n"
        f"👥 *From:* {accounts}\n"
        f"💡 *Recommendation:* Wait for clarity before acting\n"
        f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )

def run():
    global previous_signals

    print(f"\n{'='*50}")
    print(f"Market Signal Agent — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}")

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

    # Detect conflicts
    conflicts = detect_conflicts(signals)

    # Group signals by asset for conflict warnings
    asset_signals = {}
    for s in signals:
        asset = s.get("asset_affected", "")
        if asset not in asset_signals:
            asset_signals[asset] = []
        asset_signals[asset].append(s)

    print("\n[3/3] Sending alerts...")

    # Send conflict warnings first
    for asset in conflicts:
        msg = format_conflict_warning(asset, asset_signals[asset])
        print(f"\n  ⚠️ CONFLICT on {asset}")
        send_telegram(msg)

    # Send individual signals
    for signal in signals:
        asset = signal.get("asset_affected", "")
        is_conflict = asset in conflicts
        msg = format_alert(signal, is_conflict=is_conflict)
        print(f"\n  → {asset} | {signal.get('direction')} | {signal.get('confidence')}/10 | target: {signal.get('price_target')} | exit: {signal.get('exit_trigger')}")
        send_telegram(msg)

    # Update previous signals for next run
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
