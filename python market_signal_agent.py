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
    "GoldmanSachs", "CathieDWood", "WarrenBuffett",
    "nouriel", "RayDalio", "jimcramer", "PeterSchiff",
    "VitalikButerin", "cz_binance", "SEC_News", "IMFNews",
    "business", "Reuters", "WSJ", "zerohedge",
    "thelonginvest", "unusual_whales"
]

TWEETS_PER_ACCOUNT = 3
MIN_SIGNAL_SCORE = 6
RUN_INTERVAL_HOURS = 1

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

def analyse_tweets(tweets_bundle):
    tweets_text = ""
    for t in tweets_bundle:
        tweets_text += f"\n@{t['username']} ({t['created_at']}):\n{t['text']}\n"

    prompt = f"""You are a financial signal analyst. Analyse the following tweets from influential accounts and identify any that could impact stocks, crypto, commodities, currencies, or macro markets.

For each significant signal found, respond with a JSON array. Each item should have:
- "account": Twitter username
- "tweet_summary": one sentence summary of the tweet
- "asset_affected": what stock/crypto/commodity/sector is affected
- "direction": "bullish", "bearish", or "neutral"
- "reasoning": 1-2 sentence explanation
- "confidence": score 1-10

Only include tweets with confidence >= {MIN_SIGNAL_SCORE}.
If no significant signals found, return: []
Respond with valid JSON only, no extra text.

TWEETS:
{tweets_text}
"""

    headers = {
        "x-api-key": CLAUDE_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    body = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1000,
        "messages": [{"role": "user", "content": prompt}]
    }
    try:
        r = requests.post("https://api.anthropic.com/v1/messages", headers=headers, json=body, timeout=30)
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

def format_alert(signal):
    emoji = {"bullish": "🟢", "bearish": "🔴", "neutral": "🟡"}.get(signal.get("direction", "neutral"), "⚪")
    return (
        f"🚨 *Market Signal Alert*\n\n"
        f"👤 @{signal['account']}\n"
        f"📌 *Asset:* {signal['asset_affected']}\n"
        f"{emoji} *Direction:* {signal['direction'].upper()}\n"
        f"📝 *Tweet:* {signal['tweet_summary']}\n"
        f"💡 *Why it matters:* {signal['reasoning']}\n"
        f"📊 *Confidence:* {signal['confidence']}/10\n"
        f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )

def run():
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
    signals = analyse_tweets(all_tweets)
    print(f"  Signals found: {len(signals)}")

    print("\n[3/3] Sending alerts...")
    if not signals:
        print("  No significant signals this run.")
    else:
        for signal in signals:
            print(f"\n  → {signal.get('asset_affected')} | {signal.get('direction')} | {signal.get('confidence')}/10")
            send_telegram(format_alert(signal))

    print(f"\n{'='*50}\nDone.")

if __name__ == "__main__":
    while True:
        run()
        print(f"\nSleeping {RUN_INTERVAL_HOURS} hour(s)...\n")
        time.sleep(RUN_INTERVAL_HOURS * 60 * 60)
