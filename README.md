🚀 Agentic Finance Studio

Institutional Sentiment → Actionable Crypto Signals

Agentic Finance Studio is an AI-powered Telegram bot that helps traders turn market sentiment into clear, actionable insights.  
Instead of reacting to noise, the bot delivers structured intelligence for assets like BTC, ETH, XRP, and SOL — combining price tracking and smart signals.

🎯 Project Vision

Retail traders often struggle with:  
- Information overload  
- Late entries  
- Lack of clear signals

Agentic Finance Studio solves this by combining:  
- Institutional Sentiment from SoSoValue  
- Market Data Intelligence with multi-source pricing  
- Actionable Alerts delivered in real-time via Telegram

🏗️ Modular Architecture

The system is built on a decoupled three-tier architecture:

1. Intelligence Layer — SoSoValue API for institutional data and pricing  
2. Signal Engine — Asynchronous Python for fast technical analysis and whale monitoring  
3. Delivery Layer — Clean, mobile-first Telegram interface with full button navigation

🛠️ Tech Stack  
- Backend: Python (Asynchronous)  
- Framework: python-telegram-bot  
- Hosting: JustRunMy.app (24/7)  
- Primary API: SoSoValue  
- Fallbacks: CoinGecko + Binance  
- Security: Environment variables only

🚀 Key Features (Wave 1)

- Real-time price tracking for BTC, ETH, XRP, and SOL  
- Smart Signal Engine with Entry, TP, and SL levels  
- Whale Activity Reports  
- Sector Intelligence Overview  
- Background Smart Alerts  
- Robust multi-API fallback system

📦 Installation & Setup

git clone https://github.com/fxscalpersignals/agentic-finance-studio.git  
cd agentic-finance-studio  
pip install -r requirements.txt  
python main.py

Environment Variables:

TELEGRAM_TOKEN=your_telegram_bot_token  
SOSO_API_KEY=your_sosovalue_api_key

CHAT_ID=your_telegram_chat_id for auto scanner alerts

Roadmap: The 3 Waves

🌊 Wave 1: The Foundation (Completed)

• Asynchronous Telegram bot running 24/7  
• Multi-source price engine (SoSoValue Primary → CoinGecko → Binance)  
• Assets: BTC, ETH, XRP, SOL  
• Whale Monitoring (MVP)  
• Secure environment-based configuration

🚀 Key Features (Wave 2)

- Real-time ATR-based signals with intelligent pullback entries, TP & SL
- EMA trend filtering (EMA20/EMA50)
- Live Sector Intelligence Map (AI, PAYFI, RWA, DEFI)
- Whale Radar for extreme momentum detection
- ETF Flow Intelligence & institutional rotation analysis
- Dynamic SSI (Sentiment Index) with fallback
- Fully button-driven Telegram interface
- Production-ready: Global Session, Cooldown, Graceful Shutdown, Safe Rate Limiting
- Paper Trading Portfolio with PnL tracking
- Auto Scanner with smart alerts
  
🌊 Wave 2: Institutional Intelligence (Completed)

• SSI Integration (SoSoValue Sentiment Index)  
• Sector Intelligence (AI Agents, PayFi, RWA)  
• Advanced Signal Engine (Sentiment + Price Action)  
• Smart Alerts (automated)

🌊 Wave 3: The Execution Layer (Future Goal)

• On-chain trading via SoDEX SDK  
• Yield Intelligence alerts  
• Full automation pipeline  
• Monetization via Telegram Stars

⚙️ Testing Instructions  
Allow 5–10 seconds between commands for best performance.  
The bot uses a tiered fallback system to ensure reliability.

⚠️ Disclaimer

This project is for educational and demonstration purposes only.  
Not financial advice. Trade at your own risk.

Live Bot: https://t.me/AgenticFinanceBot  
Demo Video: https://www.youtube.com/watch?v=2WVZ96wgUTU
GitHub: https://github.com/fxscalpersignals/agentic-finance-studio

Built by fxscalpersignals (Solo Developer) for the SoSoValue Buildathon.
