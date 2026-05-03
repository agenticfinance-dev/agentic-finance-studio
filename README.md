Agentic Finance Studio 🚀

Institutional Sentiment → Actionable Crypto Signals

Agentic Finance Studio is an AI-powered Telegram bot that helps traders turn market sentiment into clear, actionable insights.

Instead of reacting to noise, the bot delivers structured intelligence for assets like BTC and XRP, combining sentiment data, price tracking, and smart alerts.

🎯 Project Vision

Retail traders often struggle with:

- Information overload
- Late entries
- Lack of clear signals

Agentic Finance Studio solves this by combining:

1. Institutional Sentiment
   Data from SoSoValue (when available)

2. Market Data Intelligence
   Multi-source pricing (SoSoValue → CoinGecko → Binance)

3. Actionable Alerts
   Simple insights delivered via Telegram in real-time
   
🏗️ Modular Architecture (The Stack)

To ensure high availability and professional-grade performance, the studio is built on a decoupled, three-tier stack:
 1. Intelligence Layer: Utilizes the SoSoValue API for real-time institutional sentiment indices and news filtering.
 2. Signal Engine: Powered by Asynchronous Python for non-blocking technical analysis (Quant TA) and institutional Whale Alert monitoring.
 3. Execution Layer: Integrated
   
🛠️ Tech Stack

- Backend: Python (Async)
- Bot Framework: python-telegram-bot
- Hosting: JustRunMy.App
- APIs:
  - SoSoValue (primary)
  - CoinGecko (fallback)
  - Binance (backup)
- Security:
  - Environment variables ("TELEGRAM_TOKEN", "SOSO_API_KEY")
  - No hardcoded credentials

🚀 Features

- 📊 Live Price Tracking
  BTC, ETH, XRP, SOL via simple commands

- 🔁 Multi-API Fallback System
  Ensures price data is always available

- 🐳 Whale Alerts (MVP)
  XRP/BTC movement detection (expandable)

- ⚡ Fast Telegram Interface
  Clean and instant responses

📦 Installation
# Clone the repository
git clone https://github.com/fxscalpersignals/agentic-finance-studio.git

# Enter the directory
cd agentic-finance-studio

# Install dependencies
pip install -r requirements.txt


🔐 Environment Variables

Set these in your hosting platform or ".env" file:

TELEGRAM_TOKEN=your_telegram_bot_token
SOSO_API_KEY=your_sosovalue_api_key

▶️ Run the Bot

python main.py

💬 Telegram Commands

/start        - Show menu
/test         - Check bot status
/btc          - Bitcoin price
/eth          - Ethereum price
/xrp          - XRP price
/sol          - Solana price
/price <sym>  - Custom coin
/whale        - Whale alert (MVP)

📈 Roadmap: The 3 Waves

🌊 Wave 1: The Foundation (Active)

Goal: Deploy a functional, real-time signal engine for a "One-Person Finance Business."
- Core Engine: Asynchronous Python Telegram bot running 24/7 on JustRunMy.App
- Market Data: Live price tracking with multi-API fallback (SoSoValue → CoinGecko → Binance)
- Assets Covered: BTC, XRP, SOL
- Whale Monitoring (MVP): Price-action-based detection of abnormal movements (expandable to on-chain tracking)
- Security: Environment Variables for all API keys (no hardcoded secrets)

🌊 Wave 2: Institutional Intelligence

Goal: Integrate SoSoValue data to enable sentiment-driven trading insights.
- SSI Integration: Real-time SoSoValue Sentiment Index (Fear vs Greed layer)
- Sector Intelligence: Monitoring sectors like PayFi, AI, and RWA via SoSoValue indices
- Signal Layer: Combine sentiment + price action to generate Entry, TP, and SL signals
- Smart Alerts: Automated Telegram notifications (no command required)

🌊 Wave 3: The Execution Layer (Final Goal)

Goal: Enable seamless transition from signal → execution using SoDEX.
- On-Chain Trading: Direct trade execution via SoDEX SDK
- Yield Intelligence: Alerts for opportunities like USSI and MAG7.ssi
- Automation: End-to-end pipeline (Signal → Decision → Execution)
- Monetization: Telegram Stars integration for premium features

🧠 Long-Term Vision

To evolve Agentic Finance Studio into a fully autonomous trading assistant that:
- Understands market sentiment
- Generates high-probability signals
- Executes trades non-custodially
- Operates as a personal financial agent

  
⚙️ Testing Instructions

For optimal performance, please allow 5–10 seconds between commands.
Agentic Finance Studio uses a tiered data architecture to ensure reliability and continuous uptime:
Primary: SoSoValue (Institutional sentiment and market intelligence)
Secondary: CoinGecko (Global price data fallback)
Tertiary: Binance (Market data redundancy and failover)
This design ensures that even if one data source is temporarily unavailable or rate-limited, the system continues to deliver accurate and timely results.

⚠️ Disclaimer

This project is for educational purposes only.
Not financial advice.

🤝 Links

- Telegram Bot: https://t.me/AgenticFinanceBot
- Developer: fxscalpersignals
- Buildathon: SoSoValue & SoDEX
