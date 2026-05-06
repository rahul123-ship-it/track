# EMA Tracker - Cryptocurrency Trading Signal System

A real-time cryptocurrency trading signal bot that monitors **Binance Futures** for EMA crossovers and sends alerts via **Telegram** and **Windows notifications**.

---

## Folder Structure

```
Ema_Tracker/
├── main.js                  ⭐ Production entry point
├── package.json
├── settings.json            Runtime config (auto-saved by bot)
├── ecosystem.config.js      PM2 process manager config
├── Dockerfile               Container deployment
├── Procfile                 Railway / Render process file
├── render.yaml              Render.com deployment config
├── fly.toml                 Fly.io deployment config
├── .env.example             Environment variable template
├── .gitignore
│
├── src/                     Core source modules
│   ├── indicators.js        RSI, MACD, Bollinger Bands, ATR
│   └── ml/                  Machine Learning components
│       ├── model.js         Brain.js neural network (train + predict)
│       ├── alternative.js   Alternative ML approach
│       ├── collector.js     Data collection helpers
│       └── os_module.js     OS-level ML utilities
│
├── variants/                Alternative bot implementations
│   ├── node_simple.js       Simplified — Telegram polling, REST-only
│   ├── node_minimal.js      Minimal — no color deps, direct HTTP
│   └── node_builtin.js      Pure Node.js built-ins only
│
├── scripts/                 Utility scripts
│   ├── setup.js             First-run setup wizard
│   └── data_sync.js         Data sync utility
│
├── logs/                    (auto-created) Daily log files
├── ml_data/                 (auto-created) ML training data
├── ml_models/               (auto-created) Saved model weights
├── csv_data/                (auto-created) CSV exports
└── models/                  (auto-created) Model storage
```

---

## System Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    INITIALIZATION                         │
│  loadSettings() → initializeTerminal() → sendStartup()   │
│  → checkEMACross() → setupAllWebSockets()                 │
└────────────────────┬─────────────────────────────────────┘
                     │
        ┌────────────┴────────────┐
        ▼                         ▼
┌───────────────┐       ┌─────────────────────┐
│  REST Polling │       │ WebSocket Streams    │
│  (Backup)     │       │ (Real-Time Primary)  │
│  Every 5 min  │       │ Per-symbol kline_XY  │
└───────┬───────┘       └──────────┬──────────┘
        │                          │
        ▼                          ▼
┌─────────────────────────────────────────────┐
│           CROSSOVER DETECTION               │
│                                             │
│  Mode A: Price vs Single EMA (50/100/200)   │
│    - Price crosses above EMA → Bullish 🟢   │
│    - Price crosses below EMA → Bearish 🔴   │
│                                             │
│  Mode B: Dual EMA 9/15 Crossover            │
│    - EMA(9) crosses above EMA(15) → Bull 🟢 │
│    - EMA(9) crosses below EMA(15) → Bear 🔴 │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│          ALERT SYSTEM                       │
│  shouldAlert() → cooldown check → send:     │
│    • Telegram message with Delta Exchange link │
│    • Windows toast with full signal info    │
│    • Console + file log                     │
└─────────────────────────────────────────────┘
```

---

## EMA Crossover Modes

### Mode A: Price vs Single EMA (Default)
Detects when the closing price crosses above/below a single EMA line.
- Select **EMA 50**, **EMA 100**, or **EMA 200** in settings.
- Best for trend-following on longer timeframes.

### Mode B: Dual EMA 9/15 Crossover
Detects when the fast EMA(9) crosses the slow EMA(15).
- Select **EMA 9/15** in settings to activate this mode.
- More responsive to short-term momentum shifts.
- When active, single EMA options (50/100/200) are ignored.
- Only EMA-to-EMA crossovers trigger alerts — price position is irrelevant.

---

## Telegram Bot Commands

| Command | Description |
|---------|-------------|
| `/menu` | Main menu with all options |
| `/status` | Current bot status and configuration |
| `/settings` | Change EMA, timeframe, volume, ML, crossover mode |
| `/top` | Top gainers, losers, highest volume |
| `/refresh` | Reconnect all WebSocket streams |
| `/mlstatus` | ML model performance stats |
| `/train` | Manually trigger ML model training |
| `/collectdata` | Collect historical data for ML |
| `/exportcsv` | Export training data to CSV |
| `/help` | Help message |

---

## Configuration (via Settings or Environment Variables)

| Setting | Default | Description |
|---------|---------|-------------|
| `TELEGRAM_BOT_TOKEN` | — | Your bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | — | Your Telegram user/chat ID |
| `EMA_PERIOD` | 200 | Single EMA period (50, 100, or 200) |
| `TIMEFRAME` | 5m | Candlestick interval (1m, 5m, 15m, 1h, 4h) |
| `VOLUME_THRESHOLD` | 100M | Minimum 24h volume to track a pair |
| `DUAL_EMA_MODE` | false | Enable EMA 9/15 crossover mode |
| `CHECK_INTERVAL` | 5 min | Backup REST polling interval |
| `ALERT_COOLDOWN` | 15 min | Minimum time between alerts per symbol |
| `ML_ENABLED` | false | Enable ML-enhanced predictions |

---

## Quick Start (Local)

```bash
# 1. Install dependencies
npm install

# 2. Set your tokens (or set as environment variables)
#    Edit settings.json OR use environment variables

# 3. Run the production bot
npm start

# 4. Or run with PM2 (keeps alive after terminal close)
npx pm2 start ecosystem.config.js
npx pm2 logs ema-tracker
```

---

## 🚀 Free 24/7 Deployment (Run When Laptop is Off)

### Option 1 — Railway ⭐ EASIEST

**Railway** automatically deploys from GitHub and keeps your bot running forever.
Free tier includes **$5 credit/month** — enough for this lightweight bot.

**Steps:**
1. Push your code to GitHub
2. Go to [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub Repo**
3. Select your repo
4. Click **Variables** and add:
   - `TELEGRAM_BOT_TOKEN` = your token
   - `TELEGRAM_CHAT_ID` = your chat ID
5. Railway detects the `Procfile` automatically and starts `node main.js`
6. Done — your bot is live 24/7 ✅

```bash
# Push to GitHub first:
git init
git add .
git commit -m "Initial deploy"
git remote add origin https://github.com/YOUR_USERNAME/ema-tracker.git
git push -u origin main
```

---

### Option 2 — Render (Free Background Worker)

**Render** Background Workers run continuously (750 hours/month free — enough for 24/7).

**Steps:**
1. Push your code to GitHub
2. Go to [render.com](https://render.com) → **New** → **Background Worker**
3. Connect your GitHub repo
4. Render detects `render.yaml` automatically
5. Add your secret environment variables in the dashboard:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
6. Click **Deploy** — your bot runs forever ✅

---

### Option 3 — Fly.io (Free Shared VMs)

**Fly.io** gives you 3 free shared VMs that run 24/7. Needs Docker.

```bash
# Install flyctl CLI
npm install -g flyctl

# Login
fly auth login

# Set your secrets (never commit these!)
fly secrets set TELEGRAM_BOT_TOKEN=your-token TELEGRAM_CHAT_ID=your-id

# Deploy
fly deploy
```

---

### Option 4 — Oracle Cloud Always Free ♾️ FOREVER FREE

**Oracle Cloud** gives you 4 ARM CPU cores + 24 GB RAM VMs that are **permanently free**. Best for serious long-term use.

1. Sign up at [cloud.oracle.com](https://cloud.oracle.com) (free, needs credit card for verification only — never charged)
2. Create a free **ARM Ampere VM** instance (Ubuntu 22.04)
3. SSH into the VM and run:

```bash
# Install Node.js 18
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt-get install -y nodejs

# Clone your repo
git clone https://github.com/YOUR_USERNAME/ema-tracker.git
cd ema-tracker
npm install

# Set environment variables
export TELEGRAM_BOT_TOKEN=your-token
export TELEGRAM_CHAT_ID=your-id

# Install PM2 to keep it running forever
sudo npm install -g pm2
pm2 start ecosystem.config.js
pm2 save
pm2 startup    # follow the printed command to auto-start on reboot
```

---

## Data Flow

1. **Startup**: Load saved settings → fetch all Binance Futures pairs above volume threshold
2. **Initial Check**: REST API fetches klines for each pair → calculates EMA → checks for crossovers
3. **Real-Time**: WebSocket streams receive closed candles → updates kline cache → recalculates EMA → checks crossovers
4. **Alert**: On crossover detection → cooldown check → Telegram message + Windows toast notification + log
5. **Periodic**: Every 5 min backup REST check; every 1 min WebSocket heartbeat; every 12h ML training

---

## EMA Crossover Modes

### Mode A: Price vs Single EMA (Default)
Detects when the closing price crosses above/below a single EMA line.
- Select **EMA 50**, **EMA 100**, or **EMA 200** in settings.
- Best for trend-following on longer timeframes.

### Mode B: Dual EMA 9/15 Crossover
Detects when the fast EMA(9) crosses the slow EMA(15).
- Select **EMA 9/15** in settings to activate this mode.
- More responsive to short-term momentum shifts.
- When active, single EMA options (50/100/200) are ignored.
- Only EMA-to-EMA crossovers trigger alerts — price position is irrelevant.

---

## Telegram Bot Commands

| Command | Description |
|---------|-------------|
| `/menu` | Main menu with all options |
| `/status` | Current bot status and configuration |
| `/settings` | Change EMA, timeframe, volume, ML, crossover mode |
| `/top` | Top gainers, losers, highest volume |
| `/refresh` | Reconnect all WebSocket streams |
| `/mlstatus` | ML model performance stats |
| `/train` | Manually trigger ML model training |
| `/collectdata` | Collect historical data for ML |
| `/exportcsv` | Export training data to CSV |
| `/help` | Help message |

---

## Configuration (via Settings or Environment Variables)

| Setting | Default | Description |
|---------|---------|-------------|
| `EMA_PERIOD` | 200 | Single EMA period (50, 100, or 200) |
| `TIMEFRAME` | 5m | Candlestick interval (1m, 5m, 15m, 1h, 4h) |
| `VOLUME_THRESHOLD` | 100M | Minimum 24h volume to track a pair |
| `DUAL_EMA_MODE` | false | Enable EMA 9/15 crossover mode |
| `CHECK_INTERVAL` | 5 min | Backup REST polling interval |
| `ALERT_COOLDOWN` | 15 min | Minimum time between alerts per symbol |
| `ML_ENABLED` | false | Enable ML-enhanced predictions |

---

## Quick Start

```bash
# Install dependencies
npm install

# Run the production bot
npm start

# Or run the simplified version
node node_simple.js

# PM2 deployment
npx pm2 start ecosystem.config.js
```

---

## Data Flow

1. **Startup**: Load saved settings → fetch all Binance Futures pairs above volume threshold
2. **Initial Check**: REST API fetches klines for each pair → calculates EMA → checks for crossovers
3. **Real-Time**: WebSocket streams receive closed candles → updates kline cache → recalculates EMA → checks crossovers
4. **Alert**: On crossover detection → cooldown check → Telegram message + desktop notification + log
5. **Periodic**: Every 5 min backup REST check; every 1 min WebSocket heartbeat; every 12h ML training
