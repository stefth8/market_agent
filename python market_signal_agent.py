import requests
import json
import time
from datetime import datetime, timezone

# ============================================================
# CONFIG — fill in your keys here
# ============================================================
TWITTER_API_KEY = "new1_80aa4e06ab134dbc971e5abe67d010b0"
CLAUDE_API_KEY = "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"
TELEGRAM_BOT_TOKEN = "8723253437:AAF-x49eCC_X01NPY0K__fXgMlraVqBtnfc"
TELEGRAM_CHAT_IDS = ["8208069287"]  # Add more IDs here later e.g. ["123", "456"]

# ============================================================
# 20 MOST MARKET-INFLUENTIAL ACCOUNTS
# ============================================================
ACCOUNTS = [
    "elonmusk", "realDonaldTrump", "federalreserve", "jpmorgan",
    "GoldmanSachs", "CathieDWood", "michael_saylor", "WarrenBuffett",
    "nouriel", "RayDalio", "jimcramer", "PeterSchiff",
    "VitalikButerin", "cz_binance", "SEC_News", "IMFNews",
    "business", "Reuters", "WSJ", "zerohedge"
]

# How many recent tweets to fetch per account
TWEETS_PER_ACCOUNT = 3

# Only alert if Claude confidence score >= this (1-10)
MIN_SIGNAL_SCORE = 6

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
                "created_at": t.get("created_at", ""),
                "username": username
            })
        return results
    except Exception as e:
        print(f"  [ERROR] Fetching @{username}: {e}")
        return []

# ============================================================
# ANALYSE WITH CLAUDE
# ============================================================
def analyse_tweets(tweets_bundle):
    tweets_text = ""
    for t in tweets_bundle:
        tweets_text += f"\n@{t['username']} ({t['created_at']}):\n{t['text']}\n"

    prompt = f"""You are a financial signal analyst. Analyse the following tweets from influential accounts and identify any that could impact stocks, crypto, commodities, currencies, or macro markets.

For each significant signal found, respond with a JSON array. Each item should have:
- "account": Twitter username
- "tweet_summary": one sentence summary of the tweet
- "asset_affected": what stock/crypto/commodity/sector is affected (be specific, e.g. "Tesla (TSLA)", "Bitcoin (BTC)", "Gold", "USD")
- "direction": "bullish", "bearish", or "neutral"
- "reasoning": 1-2 sentence explanation of why this matters
- "confidence": score 1-10 (10 = very high impact signal)

Only include tweets with confidence >= {MIN_SIGNAL_SCORE}.
If no significant signals found, return an empty array: []
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
        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        signals = json.loads(raw.strip())
        return signals
    except Exception as e:
        print(f"  [ERROR] Claude analysis: {e}")
        return []

# ============================================================
# SEND TELEGRAM ALERT
# ============================================================
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for chat_id in TELEGRAM_CHAT_IDS:
        try:
            r = requests.post(url, json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown"
            }, timeout=10)
            if r.status_code == 200:
                print(f"  [✓] Alert sent to {chat_id}")
            else:
                print(f"  [ERROR] Telegram {chat_id}: {r.text}")
        except Exception as e:
            print(f"  [ERROR] Telegram send: {e}")

# ============================================================
# FORMAT ALERT MESSAGE
# ============================================================
def format_alert(signal):
    direction_emoji = {"bullish": "🟢", "bearish": "🔴", "neutral": "🟡"}.get(signal.get("direction", "neutral"), "⚪")
    return (
        f"🚨 *Market Signal Alert*\n\n"
        f"👤 @{signal['account']}\n"
        f"📌 *Asset:* {signal['asset_affected']}\n"
        f"{direction_emoji} *Direction:* {signal['direction'].upper()}\n"
        f"📝 *Tweet:* {signal['tweet_summary']}\n"
        f"💡 *Why it matters:* {signal['reasoning']}\n"
        f"📊 *Confidence:* {signal['confidence']}/10\n"
        f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )

# ============================================================
# MAIN RUN
# ============================================================
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
        time.sleep(0.5)  # be polite to the API

    print(f"\n  Total tweets fetched: {len(all_tweets)}")

    if not all_tweets:
        print("  No tweets fetched. Check your twitterapi.io key.")
        return

    print("\n[2/3] Analysing with Claude...")
    # Send all tweets in one batch to save Claude API credits
    signals = analyse_tweets(all_tweets)
    print(f"  Signals found: {len(signals)}")

    print("\n[3/3] Sending alerts...")
    if not signals:
        print("  No significant signals this run.")
        # Optional: send a "no signal" summary
        # send_telegram("✅ Market Signal Agent ran — no significant signals found.")
    else:
        for signal in signals:
            message = format_alert(signal)
            print(f"\n  → Signal: {signal.get('asset_affected')} | {signal.get('direction')} | Score: {signal.get('confidence')}")
            send_telegram(message)

    print(f"\n{'='*50}")
    print("Done.")

if __name__ == "__main__":
    run()
