require('dotenv').config();
const axios = require('axios');
const colors = require('colors');
const figlet = require('figlet');
const path = require('path');
const fs = require('fs');
const { exec, execFile } = require('child_process');
const net = require('net');
const TelegramBot = require('node-telegram-bot-api');
const notifier = require('node-notifier');
const WebSocket = require('ws');
const http = require('http');
const { createMLRuntime } = require('./src/ml/runtime');
const DELTA_REST_BASE_URL = 'https://api.india.delta.exchange/v2';
const DELTA_PUBLIC_WS_URL = 'wss://public-socket.india.delta.exchange';

let initialLoadComplete = false;
let lastCandleTime = null; // Timestamp of the most recently processed closed candle
// ML configuration
let ML_ENABLED = false;

// Dual EMA crossover mode: when true, alerts on EMA(9) vs EMA(15) crossover instead of price vs single EMA
let DUAL_EMA_MODE = false;
// Optional fast timeframe addon for dual mode: include 1m+3m alongside 5m+15m
let INCLUDE_FAST_TFS = (process.env.INCLUDE_FAST_TFS || 'true').toLowerCase() === 'true';
const FORCE_ALL_DUAL_TFS = (process.env.FORCE_ALL_DUAL_TFS || 'true').toLowerCase() === 'true';

// Configuration
let EMA_PERIOD = parseInt(process.env.EMA_PERIOD, 10) || 200;
let TIMEFRAME = process.env.TIMEFRAME || '15m';
let VOLUME_THRESHOLD = parseInt(process.env.VOLUME_THRESHOLD, 10) || 100_000_000;
const CHECK_INTERVAL = parseInt(process.env.CHECK_INTERVAL, 10) || 5 * 60 * 1000; // 5 minutes
const ALERT_COOLDOWN = parseInt(process.env.ALERT_COOLDOWN, 10) || 1 * 60 * 1000; // 1 minute cooldown for alerts

// Flat EMA filter configuration - suppresses alerts during sideways markets
const FLAT_EMA_PERIODS = 5; // Number of candles to look back for slope calculation
const FLAT_EMA_HYSTERESIS = Math.max(1, parseInt(process.env.FLAT_EMA_HYSTERESIS || '1', 10));
const FLAT_EMA_THRESHOLD_SCALE = (() => {
    const parsed = parseFloat(process.env.FLAT_EMA_THRESHOLD_SCALE || '0.2');
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0.2;
})();
const DUAL_EMA_FLAT_FILTER = (process.env.DUAL_EMA_FLAT_FILTER || 'false').toLowerCase() === 'true';
// Per-timeframe thresholds: faster TFs need smaller thresholds (in percent)
const FLAT_EMA_THRESHOLDS = {
    '1m': 0.005,   // 0.005% for 1m (5 candles = 5 minutes)
    '3m': 0.008,   // 0.008% for 3m (5 candles = 15 minutes)
    '5m': 0.012,   // 0.012% for 5m (5 candles = 25 minutes)
    '15m': 0.02,   // 0.02% for 15m (5 candles = 75 minutes)
    '1h': 0.04,    // 0.04% for 1h (5 candles = 5 hours)
    '4h': 0.08,    // 0.08% for 4h
    '1d': 0.15     // 0.15% for 1d
};

// Telegram configuration — must be provided via environment variables
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;
if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.error('FATAL: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set as environment variables.');
    console.error('Create a .env file or set them in your deployment platform, then restart.');
    process.exit(1);
}
if (!/^-?[0-9]+$/.test(TELEGRAM_CHAT_ID)) {
    console.error('FATAL: TELEGRAM_CHAT_ID must be a numeric string (digits with optional leading minus).');
    process.exit(1);
}
if (!/^\d+:[A-Za-z0-9_-]{35,}$/.test(TELEGRAM_BOT_TOKEN)) {
    console.error('FATAL: TELEGRAM_BOT_TOKEN format is invalid. Expected format: <digits>:<35+ alphanumeric chars>');
    process.exit(1);
}

// Initialize Telegram bot with polling enabled
// const bot = new TelegramBot(TELEGRAM_BOT_TOKEN, { polling: true });
// Initialize Telegram bot with better error handling
const bot = new TelegramBot(TELEGRAM_BOT_TOKEN, { 
    polling: {
      params: { timeout: 30, limit: 100 },
      retryTimeout: 5000 // Wait 5 seconds before retrying on error
    }
  });

  // Add specific error handlers
bot.on('polling_error', (error) => {
    log(`Telegram polling error: ${error.message}`, 'error');
    // Skip auto-reconnect while a graceful reconnect is running
    if (isReconnecting) return;

    // Restart polling after a delay if connection was reset
    if (error.code === 'ECONNRESET' || error.code === 'EFATAL') {
      log('Connection reset, restarting polling in 10 seconds...', 'warning');
      isReconnecting = true;
      setTimeout(() => {
        try {
          bot.stopPolling();
          setTimeout(() => {
            bot.startPolling();
            isReconnecting = false;
            log('Telegram polling restarted successfully', 'success');
          }, 1000);
        } catch (e) {
          isReconnecting = false;
          log(`Failed to restart polling: ${e.message}`, 'error');
        }
      }, 10000);
    }
  });


// Store last alert times and states for each symbol
const lastAlerts = new Map();
const coinStates = new Map(); // Tracks the current state of each coin (above/below EMA)
const trackedPairs = new Set(); // Keep track of pairs we're already monitoring
// Flat EMA hysteresis counters: tracks consecutive non-flat readings per symbol/tf
// Key format: symbol_tf (e.g., "BTCUSDT_5m") or just symbol in single-mode
const flatEmaHysteresisCounters = new Map();

// Persist and restore alert state so restarts don't re-fire existing crossovers
const ALERT_STATE_PATH = path.join(__dirname, 'alert_state.json');
function loadAlertState() {
    try {
        if (!fs.existsSync(ALERT_STATE_PATH)) return;
        const { alerts, states } = JSON.parse(fs.readFileSync(ALERT_STATE_PATH, 'utf8'));
        if (Array.isArray(alerts)) for (const [k, v] of alerts) lastAlerts.set(k, v);
        if (Array.isArray(states)) for (const [k, v] of states) coinStates.set(k, v);
        log('Alert state restored from disk', 'info');
    } catch (e) {
        log(`Could not load alert state: ${e.message}`, 'warning');
    }
}
function saveAlertState() {
    fs.writeFile(ALERT_STATE_PATH, JSON.stringify({
        alerts: [...lastAlerts.entries()],
        states: [...coinStates.entries()]
    }), () => {});
}

// Deferred update queue — replaces unbounded 24h setTimeout calls
// Each entry: { executeAt: timestamp, fn: async () => ... }
const deferredUpdates = [];
// O(log n) sorted insert — keeps deferredUpdates in ascending executeAt order
function deferredInsert(entry) {
    let lo = 0, hi = deferredUpdates.length;
    while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        if (deferredUpdates[mid].executeAt <= entry.executeAt) lo = mid + 1;
        else hi = mid;
    }
    deferredUpdates.splice(lo, 0, entry);
}

// WebSocket related variables
const activeWebSockets = new Map(); // Track active WebSocket connections
const klineCache = new Map(); // Cache for kline data
const emaCache = new Map(); // Cache for calculated EMAs
const ema9Cache = new Map(); // Cache for EMA(9) values (dual mode)
const ema15Cache = new Map(); // Cache for EMA(15) values (dual mode)
const lastWsCandleTs = new Map(); // Tracks last seen WS event timestamp per symbol/tf (diagnostic only)
const oiSnapshotCache = new Map(); // Cache latest OI per symbol for delta calculation
// Composite cache key for dual-TF mode — "BTCUSDT_5m" / "BTCUSDT_15m"
function tfKey(symbol, tf) { return `${symbol}_${tf}`; }

/**
 * Check if EMA is flat (sideways market) to suppress false crossover alerts.
 * Uses per-timeframe thresholds and hysteresis to avoid chop.
 *
 * @param {string} symbol - Trading pair symbol
 * @param {string} tf - Timeframe (e.g., '5m', '15m') or '' for single-mode
 * @param {boolean} useDualEma - Whether to check EMA15 (dual mode) or single EMA
 * @returns {Object} { isFlat: boolean, reason: string|null }
 */
function isEmaFlat(symbol, tf = '', useDualEma = false) {
    // By default do not suppress dual EMA 9/15 crossovers with flat filter.
    if (useDualEma && !DUAL_EMA_FLAT_FILTER) {
        return { isFlat: false, reason: null };
    }

    // Build correct cache key for dual mode
    const cacheKey = tf ? tfKey(symbol, tf) : symbol;
    const hysteresisKey = cacheKey;

    // Get EMA values from appropriate cache
    const emaValues = useDualEma
        ? (ema15Cache.get(cacheKey) || [])
        : (emaCache.get(cacheKey) || []);

    // CRITICAL: Bounds check - need at least FLAT_EMA_PERIODS + 1 entries
    // (to exclude live candle and have enough history)
    if (emaValues.length <= FLAT_EMA_PERIODS + 1) {
        return { isFlat: false, reason: 'insufficient_data' };
    }

    // Exclude the live (unclosed) candle - use only closed candles for slope calculation
    // This prevents false flat readings mid-candle during consolidation
    const closedEmaValues = emaValues.slice(0, -1);
    const lastEMA = closedEmaValues[closedEmaValues.length - 1];
    const pastEMA = closedEmaValues[closedEmaValues.length - 1 - FLAT_EMA_PERIODS];

    // Guard against invalid values
    if (!lastEMA || !pastEMA || pastEMA === 0) {
        return { isFlat: false, reason: 'invalid_ema_values' };
    }

    // Calculate EMA change percentage over the lookback period
    const emaChangePct = Math.abs((lastEMA - pastEMA) / pastEMA * 100);

    // Get threshold for this timeframe (default to 0.02% if not found)
    const threshold = (FLAT_EMA_THRESHOLDS[tf || TIMEFRAME] || 0.02) * FLAT_EMA_THRESHOLD_SCALE;

    // Check if EMA is flat
    const isCurrentlyFlat = emaChangePct < threshold;

    // Hysteresis: require FLAT_EMA_HYSTERESIS consecutive non-flat readings before allowing alerts
    let consecutiveNonFlat = flatEmaHysteresisCounters.get(hysteresisKey) || 0;

    if (isCurrentlyFlat) {
        // Reset hysteresis counter when flat
        flatEmaHysteresisCounters.set(hysteresisKey, 0);
        return {
            isFlat: true,
            reason: `EMA slope ${emaChangePct.toFixed(4)}% < threshold ${threshold}% (${FLAT_EMA_PERIODS} periods)`
        };
    } else {
        // Increment counter for non-flat reading
        consecutiveNonFlat++;
        flatEmaHysteresisCounters.set(hysteresisKey, consecutiveNonFlat);

        // Only allow alerts after hysteresis threshold is met
        if (consecutiveNonFlat < FLAT_EMA_HYSTERESIS) {
            return {
                isFlat: true,
                reason: `hysteresis: ${consecutiveNonFlat}/${FLAT_EMA_HYSTERESIS} consecutive non-flat readings (${emaChangePct.toFixed(4)}%)`
            };
        }

        // Reset counter after allowing alert to require hysteresis again next time
        flatEmaHysteresisCounters.set(hysteresisKey, 0);
        return { isFlat: false, reason: null };
    }
}

let mlRuntime = null; // Initialized in initialize() after core services are ready
const reconnectionAttempts = new Map(); // Track reconnection attempts (keyed by pool index: "pool_0", "pool_1", …)
const MAX_RECONNECTION_ATTEMPTS = 5;
const RECONNECTION_DELAY = 5000; // 5 seconds
const WS_TOPICS_PER_CONN = 100;  // Keep pool size conservative for stable public WS usage
const wsPool = [];                // [{ ws, symbols: Set<string>, index }]
const MIN_CROSS_PCT = 0.0003; // 0.03% minimum crossover margin to reduce whipsaw
// Validation constants — shared by settings loader and callback handler
const VALID_VOLUMES    = [2_000_000, 5_000_000, 10_000_000, 20_000_000, 50_000_000, 100_000_000, 200_000_000];
const VALID_TIMEFRAMES = ['1m', '5m', '15m', '1h', '4h'];
const VALID_EMA_PERIODS = [50, 100, 200];
const ALERT_TRACE_ENABLED = (process.env.ALERT_TRACE || 'false').toLowerCase() === 'true';
let isReconnecting = false; // Prevents stacked reconnectionsduring graceful restarts
let monitoringInterval = null; // Reference to the periodic check interval

// Prevent destructive commands from being spammed concurrently.
const COMMAND_COOLDOWN_MS = 30 * 1000;
const commandCooldown = new Map(); // command -> { running: boolean, lastRunAt: number }

function beginCommand(command) {
    const now = Date.now();
    const state = commandCooldown.get(command);

    if (state?.running) {
        return { ok: false, reason: 'already-running' };
    }

    if (state && now - state.lastRunAt < COMMAND_COOLDOWN_MS) {
        return { ok: false, reason: 'cooldown' };
    }

    commandCooldown.set(command, { running: true, lastRunAt: now });
    return { ok: true };
}

function endCommand(command) {
    commandCooldown.set(command, { running: false, lastRunAt: Date.now() });
}

function traceAlert(message) {
    if (!ALERT_TRACE_ENABLED) return;
    log(`[ALERT_TRACE] ${message}`, 'info');
}

// Telegram circuit breaker — pauses alert sends after 5 consecutive failures
let _tgFailCount = 0;
let _tgPausedUntil = 0;
async function safeSendAlert(chatId, text, opts) {
    if (Date.now() < _tgPausedUntil) {
        log('Telegram circuit open — alert suppressed', 'warning');
        return;
    }
    try {
        await bot.sendMessage(chatId, text, opts);
        _tgFailCount = 0;
    } catch (e) {
        if (++_tgFailCount >= 5) {
            _tgPausedUntil = Date.now() + 5 * 60 * 1000;
            log('Telegram circuit breaker tripped — pausing sends for 5 minutes', 'warning');
            _tgFailCount = 0;
        }
        throw e;
    }
}

// Build a chart URL for a given symbol on TradingView (using Delta Exchange data)
// Delta Exchange trades perpetual futures, so symbols need .P suffix on TradingView
function getChartUrl(symbol, tf = '') {
    // Normalize Delta symbols for TradingView DELTA feed.
    // Example: BTCUSD -> DELTA:BTCUSDT.P
    const raw = String(symbol || '').toUpperCase().replace(/[^A-Z0-9.]/g, '');
    const withoutPerp = raw.endsWith('.P') ? raw.slice(0, -2) : raw;
    const withQuote = withoutPerp.endsWith('USD') && !withoutPerp.endsWith('USDT')
        ? `${withoutPerp}T`
        : withoutPerp;
    const tvSymbol = withQuote.endsWith('.P') ? withQuote : `${withQuote}.P`;

    // Map bot timeframe to TradingView interval
    const timeframe = tf || TIMEFRAME;
    const tvIntervalMap = {
        '1m': '1',
        '3m': '3',
        '5m': '5',
        '15m': '15',
        '30m': '30',
        '1h': '60',
        '2h': '120',
        '4h': '240',
        '6h': '360',
        '12h': '720',
        '1d': '1D'
    };
    const tvInterval = tvIntervalMap[timeframe] || '15';

    // Primary: TradingView with Delta Exchange data
    const params = new URLSearchParams({
        symbol: `DELTA:${tvSymbol}`,
        interval: tvInterval
    });
    const tradingViewUrl = `https://www.tradingview.com/chart/?${params.toString()}`;

    return tradingViewUrl;
}

function getHtmlSafeUrl(url) {
    return String(url || '').replace(/&/g, '&amp;');
}
// Returns a human-readable timeframe label for the current mode.
// Dual mode doesn't have a single TF, so we reflect the actual tf arg or show both.
function activeTimeframeLabel(tf = '') {
    if (DUAL_EMA_MODE) return tf ? tf.toUpperCase() : getDualEmaTimeframes().join(' + ');
    return TIMEFRAME;
}
// Central source of truth for dual-mode timeframe set.
function isFastTfsActive() {
    return FORCE_ALL_DUAL_TFS || INCLUDE_FAST_TFS;
}

function getDualEmaTimeframes() {
    return isFastTfsActive() ? ['1m', '3m', '5m', '15m'] : ['5m', '15m'];
}
function timeframeToSeconds(tf) {
    const map = {
        '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
        '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '12h': 43200, '1d': 86400
    };
    return map[tf] || 300;
}

// Normalize websocket timestamps to seconds.
// Delta payloads can differ by environment (seconds/ms/microseconds).
function normalizeWsTimestamp(rawTs) {
    const n = Number(rawTs);
    if (!Number.isFinite(n) || n <= 0) return Math.floor(Date.now() / 1000);
    if (n > 1e15) return Math.floor(n / 1e6); // microseconds -> seconds
    if (n > 1e11) return Math.floor(n / 1000); // milliseconds -> seconds
    return Math.floor(n); // already seconds
}
// Rate limiting for API calls
const API_RATE_LIMIT = 1200; // 1.2 seconds between API calls
let lastApiCall = 0;

// Serialised rate-limit queue — prevents concurrent callers from all reading the
// same lastApiCall timestamp in the same millisecond, which would bypass the limiter.
let _rateLimitQueue = Promise.resolve();
function enforceRateLimit() {
    _rateLimitQueue = _rateLimitQueue.then(() => {
        const now = Date.now();
        const wait = API_RATE_LIMIT - (now - lastApiCall);
        if (wait > 0) {
            lastApiCall = now + wait;
            return new Promise(r => setTimeout(r, wait));
        }
        lastApiCall = now;
    });
    return _rateLimitQueue;
}

// ML directories
const ML_DATA_DIR = path.join(__dirname, 'ml_data');
const CSV_DATA_DIR = path.join(__dirname, 'csv_data');
const MODEL_PATH = path.join(__dirname, 'ml_models');

// At the top of your file, after other requires
// const brainML = require('./src/ml/alternative');

// Create a log directory for persistent logging
const LOG_DIR = path.join(__dirname, 'logs');
if (!fs.existsSync(LOG_DIR)) {
    fs.mkdirSync(LOG_DIR);
}

// Cached daily log file path — refreshes once per day to avoid per-call allocations
let _cachedLogDate = '';
let _cachedLogPath = '';
function getDailyLogPath() {
    const today = new Date().toISOString().split('T')[0];
    if (today !== _cachedLogDate) {
        _cachedLogDate = today;
        _cachedLogPath = path.join(LOG_DIR, `ema-tracker-${today}.log`);
    }
    return _cachedLogPath;
}

// Create ML directories
if (!fs.existsSync(ML_DATA_DIR)) {
    fs.mkdirSync(ML_DATA_DIR, { recursive: true });
}

if (!fs.existsSync(CSV_DATA_DIR)) {
    fs.mkdirSync(CSV_DATA_DIR, { recursive: true });
}

if (!fs.existsSync(MODEL_PATH)) {
    fs.mkdirSync(MODEL_PATH, { recursive: true });
}

// Log function that writes to both console and file
function log(message, type = 'info') {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${message}`;

    // Console logging with colors
    switch (type) {
        case 'error':
            console.error(logMessage.red);
            break;
        case 'success':
            console.log(logMessage.green);
            break;
        case 'warning':
            console.log(logMessage.yellow);
            break;
        default:
            console.log(logMessage);
    }

    // File logging
    const logFile = getDailyLogPath();
    fs.appendFileSync(logFile, logMessage + '\n');
}

// Check if TensorFlow.js can be loaded
function checkTensorFlowAvailability() {
    return mlRuntime ? mlRuntime.checkTensorFlowAvailability() : false;
}

// Resolve SnoreToast binary bundled with node-notifier (Windows only)
const SNORE_TOAST = (() => {
    try {
        const p = path.join(require.resolve('node-notifier'), '..', 'vendor', 'snoreToast', 'snoretoast-x64.exe');
        return fs.existsSync(p) ? p : null;
    } catch (e) { return null; }
})();

// Register EMATracker as a toast-capable app so Windows shows notifications
// and pipe-based click callbacks work. Only needs to run once but is idempotent.
function registerToastApp() {
    if (process.platform !== 'win32' || !SNORE_TOAST) return;
    try {
        execFile(SNORE_TOAST, ['-install', 'EMA Tracker', SNORE_TOAST, 'EMATracker'], (err) => {
            if (err) log(`Toast app registration note: ${err.message}`, 'warning');
            else log('Toast app registered (EMATracker)', 'info');
        });
    } catch (e) { /* ignore */ }
}

// Show desktop notification.
// On Windows, uses SnoreToast directly with a named pipe so that click events are
// reliably detected and the URL is opened in the default browser.
function showDesktopNotification(title, message, type = 'info', url = null) {
    try {
        const displayMsg = url ? `${message}\n\uD83D\uDD17 Click to open chart` : message;

        if (process.platform === 'win32' && SNORE_TOAST) {
            if (url) {
                // Named-pipe approach: SnoreToast writes 'activate' into the pipe when clicked
                const pipeName = `\\\\.\\pipe\\emaTracker-${Date.now()}`;
                const pipeServer = net.createServer((stream) => {
                    let buf = '';
                    stream.on('data', (chunk) => {
                        // Different SnoreToast builds may emit UTF-16LE or UTF-8 callback payloads.
                        buf += `${chunk.toString('utf16le')}|${chunk.toString('utf8')}`;
                    });
                    stream.on('end', () => {
                        pipeServer.close();
                        const action = buf.toLowerCase();
                        if (action.includes('activate') || action.includes('clicked') || action.includes('buttonpressed')) {
                            // Use execFile (no shell) + PowerShell single-quote escaping to safely open URL
                            const psUrl = String(url).replace(/'/g, "''");
                            execFile('powershell', ['-NoProfile', '-NonInteractive', '-Command', `Start-Process '${psUrl}'`], (err) => {
                                if (err) log(`Failed to open browser: ${err.message}`, 'error');
                            });
                        }
                    });
                    stream.on('error', () => {});
                });
                pipeServer.on('error', (err) => log(`Toast pipe error: ${err.message}`, 'error'));
                pipeServer.listen(pipeName, () => {
                    execFile(SNORE_TOAST, ['-t', title, '-m', displayMsg, '-pipeName', pipeName, '-appID', 'EMATracker'],
                        (err) => { if (err && err.code !== 0) { try { pipeServer.close(); } catch (e) {} } }
                    );
                });
                // Auto-close pipe server after 2 minutes if user never interacts
                setTimeout(() => { try { pipeServer.close(); } catch (e) {} }, 2 * 60 * 1000);
            } else {
                // No URL — fire-and-forget toast
                execFile(SNORE_TOAST, ['-t', title, '-m', displayMsg, '-appID', 'EMATracker']);
            }
        } else {
            // Non-Windows fallback
            notifier.notify({ title, message: displayMsg, sound: true });
        }

        log(`Desktop notification shown: ${title} - ${message}`);
    } catch (error) {
        log(`Failed to show desktop notification: ${error.message}`, 'error');
    }
}

// Initialize terminal
function initializeTerminal() {
    console.clear();
    console.log(figlet.textSync('EMA Tracker', { font: 'Standard' }).green);
    console.log('Monitoring Delta Exchange Futures for EMA Crossovers'.yellow.bold);
    console.log(`Configuration: ${DUAL_EMA_MODE ? `EMA 9/15 Cross [${getDualEmaTimeframes().join('+')}]` : EMA_PERIOD + ' EMA'} | ${DUAL_EMA_MODE ? getDualEmaTimeframes().join(' + ') : TIMEFRAME} Timeframe | Volume > ${VOLUME_THRESHOLD.toLocaleString()}`.cyan);
    console.log(`Alert Cooldown: ${ALERT_COOLDOWN / 60000} minutes`.magenta);
    console.log(`Telegram Alerts: Enabled for Chat ID ${String(TELEGRAM_CHAT_ID).slice(0, 4)}****`.blue);
    console.log(`WebSocket Real-Time Monitoring: Enabled`.green);
    console.log(`Machine Learning: ${ML_ENABLED ? 'Enabled'.green : 'Disabled'.red}`);
    console.log('='.repeat(80).dim);
    console.log('\nCROSSOVER EVENTS:'.cyan.bold);

    log(`EMA Tracker started with configuration: Mode=${DUAL_EMA_MODE ? `EMA9/15[${getDualEmaTimeframes().join('+')}]` : 'EMA' + EMA_PERIOD}, Timeframe=${DUAL_EMA_MODE ? getDualEmaTimeframes().join('+') : TIMEFRAME}, Volume Threshold=${VOLUME_THRESHOLD}, ML=${ML_ENABLED}`);
}

// Helper function to format volume
function formatVolume(volume) {
    if (volume >= 1_000_000_000) {
        return (volume / 1_000_000_000).toFixed(2) + 'B';
    } else if (volume >= 1_000_000) {
        return (volume / 1_000_000).toFixed(2) + 'M';
    } else if (volume >= 1_000) {
        return (volume / 1_000).toFixed(2) + 'K';
    }
    return volume.toFixed(2);
}

// Format price with appropriate precision based on value
function formatPrice(price) {
    if (price < 0.001) return price.toFixed(8);
    if (price < 1) return price.toFixed(6);
    if (price < 100) return price.toFixed(4);
    return price.toFixed(2);
}

// Function to get 24hr stats for a symbol
async function get24HrStats(symbol) {
    try {
        await enforceRateLimit();
        const response = await axios.get(`${DELTA_REST_BASE_URL}/tickers/${symbol}`, {
            timeout: 10000
        });
        const ticker = response?.data?.result;
        if (!ticker) throw new Error(`Ticker not found for ${symbol}`);
        return {
            priceChangePercent: parseFloat(ticker.ltp_change_24h || 0).toFixed(2),
            quoteVolume: parseFloat(ticker.turnover_usd || ticker.turnover || 0)
        };
    } catch (error) {
        log(`Error fetching 24hr stats for ${symbol}: ${error.message}`, 'error');
        return { priceChangePercent: '0.00', quoteVolume: '0' };
    }
}

// Fetch Delta perpetual futures pairs with 24hr turnover above threshold
async function getFuturesPairs() {
    try {
        // Enforce rate limiting
        await enforceRateLimit();

        const response = await axios.get(`${DELTA_REST_BASE_URL}/tickers`, {
            params: { contract_types: 'perpetual_futures' },
            timeout: 10000
        });

        const tickers = response?.data?.result;
        if (!Array.isArray(tickers)) throw new Error('Unexpected Delta tickers response shape');
        const newPairs = [];

        const pairs = tickers
            .filter(ticker => {
                if (ticker.contract_type !== 'perpetual_futures') return false;

                const volume = parseFloat(ticker.turnover_usd || ticker.turnover || 0);
                const symbol = ticker.symbol;

                if (volume > VOLUME_THRESHOLD) {
                    // Only track new pairs that cross threshold after initial load
                    if (initialLoadComplete && !trackedPairs.has(symbol)) {
                        newPairs.push({
                            symbol,
                            volume,
                            price: parseFloat(ticker.close || 0),
                            change: parseFloat(ticker.ltp_change_24h || 0)
                        });
                    }
                    trackedPairs.add(symbol);
                    return true;
                }
                return false;
            })
            .map(ticker => ticker.symbol);

        // Alert for new pairs that crossed the volume threshold (only after initial load)
        if (newPairs.length > 0) {
            alertNewHighVolumePairs(newPairs);
        }

        return pairs;
    } catch (error) {
        log(`Error fetching futures pairs: ${error.message}`, 'error');

        if (error.response && error.response.status === 429) {
            log('Rate limited by Delta (429). Waiting 2 minutes before next call...', 'warning');
            await new Promise(resolve => setTimeout(resolve, 2 * 60 * 1000));
        }

        return [];
    }
}

// Alert when new pairs cross the volume threshold
async function alertNewHighVolumePairs(newPairs) {
    for (const pair of newPairs) {
        const chartUrl = getChartUrl(pair.symbol);
        const message = `🔔 <b>NEW HIGH VOLUME PAIR DETECTED</b>\n\n` +
            `<b>Symbol:</b> ${pair.symbol}\n` +
            `<b>Volume:</b> ${formatVolume(pair.volume)}\n` +
            `<b>Price:</b> ${formatPrice(pair.price)}\n` +
            `<b>24h Change:</b> ${pair.change.toFixed(2)}%\n` +
            `<b>Time:</b> ${new Date().toLocaleString()}\n\n` +
            `This pair has been added to the monitoring list.\n\n` +
            `<a href="${getHtmlSafeUrl(chartUrl)}">View Chart on TradingView</a>`;

        try {
            await bot.sendMessage(TELEGRAM_CHAT_ID, message, { 
                parse_mode: 'HTML',
                disable_web_page_preview: true
            });

            // Show desktop notification — match Telegram content
            showDesktopNotification(
                `🔔 New High Volume Pair: ${pair.symbol}`,
                `Volume: ${formatVolume(pair.volume)}  Price: ${formatPrice(pair.price)}\n24h Change: ${pair.change.toFixed(2)}%  Added to monitoring`,
                'info',
                chartUrl
            );

            log(`New high volume pair alert sent for ${pair.symbol} with volume ${formatVolume(pair.volume)}`, 'success');

            // Subscribe the new pair to the existing pool
            subscribeSymbolToPool(pair.symbol);
        } catch (error) {
            log(`Error sending new pair alert for ${pair.symbol}: ${error.message}`, 'error');
        }
    }
}

// Retrieve historical candlestick data for the given symbol
// tf — optional timeframe override (e.g. '5m', '15m'); defaults to TIMEFRAME
async function getKlines(symbol, tf = null) {
    try {
        const interval = tf || TIMEFRAME;
        // Request enough candles for the active EMA period
        // Request enough candles for the active EMA period + a large warmup for accuracy
        const limit = 1000;

        const end = Math.floor(Date.now() / 1000);
        const start = end - (timeframeToSeconds(interval) * (limit + 5));

        await enforceRateLimit();

        const response = await axios.get(`${DELTA_REST_BASE_URL}/history/candles`, {
            params: { resolution: interval, symbol, start, end },
            timeout: 10000
        });

        const list = response.data?.result;
        if (!Array.isArray(list) || list.length === 0) {
            log(`Empty kline response for ${symbol} [${interval}] — Delta may be rate-limiting`, 'warning');
            return [];
        }
        const raw = list;

        const klines = raw.map(k => ({
            time:   parseInt(k.time, 10) * 1000,
            open:   parseFloat(k.open),
            high:   parseFloat(k.high),
            low:    parseFloat(k.low),
            close:  parseFloat(k.close),
            volume: parseFloat(k.volume || 0)
        })).sort((a, b) => a.time - b.time);

        // Use composite key when tf is explicitly provided (dual-TF mode)
        const cacheKey = tf ? tfKey(symbol, tf) : symbol;

        // Update the kline cache
        klineCache.set(cacheKey, klines);

        // Calculate and cache EMA based on current mode
        const closes = klines.map(k => k.close);

        if (DUAL_EMA_MODE) {
            // Dual EMA mode: calculate EMA(9) and EMA(15)
            const ema9Values = calculateEMA(closes, 9);
            const ema15Values = calculateEMA(closes, 15);
            ema9Cache.set(cacheKey, ema9Values);
            ema15Cache.set(cacheKey, ema15Values);
        } else {
            // Single EMA mode: calculate one EMA for the configured period
            const emaValues = calculateEMA(closes, EMA_PERIOD);
            emaCache.set(cacheKey, emaValues);
        }

        const minPeriod = DUAL_EMA_MODE ? 15 : EMA_PERIOD;
        if (klines.length < minPeriod) {
            log(`Warning: Not enough candles for ${symbol} [${interval}]. Needed ${minPeriod}, got ${klines.length}`, 'warning');
        }

        return klines;
    } catch (error) {
        log(`Error fetching klines for ${symbol}: ${error.message}`, 'error');
        return [];
    }
}

// Calculate the EMA for an array of prices given a period
function calculateEMA(prices, period) {
    if (prices.length < period) {
        log(`Warning: Not enough prices for EMA calculation. Needed ${period}, got ${prices.length}`, 'warning');
        return [];
    }

    const k = 2 / (period + 1);
    let emaArray = [];

    // Start with the simple moving average as the first EMA
    let sma = prices.slice(0, period).reduce((sum, p) => sum + p, 0) / period;
    let ema = sma;

    // Add the first EMA (which is the SMA)
    emaArray.push(ema);

    // Calculate EMA for the remaining prices
    for (let i = period; i < prices.length; i++) {
        ema = (prices[i] - ema) * k + ema;
        emaArray.push(ema);
    }

    return emaArray;
}

// Update EMA with a new price (for real-time updates)
function updateEMA(symbol, newPrice, isNewCandle = false) {
    // Get cached EMA values
    let emaValues = emaCache.get(symbol);

    // If no cached values, we need to fetch historical data first
    if (!emaValues || emaValues.length === 0) {
        log(`No cached EMA values for ${symbol}, fetching historical data...`, 'warning');
        return false;
    }

    const k = 2 / (EMA_PERIOD + 1);

    if (isNewCandle) {
        const lastEMA = emaValues[emaValues.length - 1];
        const newEMA = (newPrice - lastEMA) * k + lastEMA;
        emaValues.push(newEMA);
    } else {
        if (emaValues.length < 2) {
            emaValues[emaValues.length - 1] = newPrice;
        } else {
            const prevClosedEMA = emaValues[emaValues.length - 2];
            const updatedEMA = (newPrice - prevClosedEMA) * k + prevClosedEMA;
            emaValues[emaValues.length - 1] = updatedEMA;
        }
    }

    // Keep the cache size reasonable by removing older values
    if (emaValues.length > EMA_PERIOD * 2) {
        emaValues = emaValues.slice(-EMA_PERIOD * 2);
    }

    emaCache.set(symbol, emaValues);
    return true;
}

// Incremental O(1) dual EMA update
// key — tfKey(symbol, tf) in dual mode, or symbol in single mode
function updateDualEMA(key, newClose, isNewCandle = false) {
    const e9  = ema9Cache.get(key);
    const e15 = ema15Cache.get(key);
    if (!e9?.length || !e15?.length) return false;

    const k9  = 2 / (9  + 1);
    const k15 = 2 / (15 + 1);

    if (isNewCandle) {
        e9.push((newClose - e9.at(-1)) * k9  + e9.at(-1));
        e15.push((newClose - e15.at(-1)) * k15 + e15.at(-1));
    } else {
        if (e9.length >= 2) {
            const prevClosedE9 = e9.at(-2);
            e9[e9.length - 1] = (newClose - prevClosedE9) * k9 + prevClosedE9;
        } else {
            e9[e9.length - 1] = newClose;
        }

        if (e15.length >= 2) {
            const prevClosedE15 = e15.at(-2);
            e15[e15.length - 1] = (newClose - prevClosedE15) * k15 + prevClosedE15;
        } else {
            e15[e15.length - 1] = newClose;
        }
    }

    // Keep arrays bounded (max 200 entries is plenty for EMA9/15)
    if (e9.length  > 200) e9.splice(0, e9.length - 200);
    if (e15.length > 200) e15.splice(0, e15.length - 200);

    ema9Cache.set(key, e9);
    ema15Cache.set(key, e15);
    return true;
}

// Fetch Open Interest delta around the crossover candle.
// Returns { oiNow, oiPrev, deltaPercent } or null on error.
// Rising OI (>0) = new money entering = stronger signal.
// Falling OI (<0) = liquidation-driven move = weaker signal.
async function getOIDelta(symbol) {
    try {
        await enforceRateLimit();
        const response = await axios.get(`${DELTA_REST_BASE_URL}/tickers/${symbol}`, {
            timeout: 8000
        });
        const ticker = response?.data?.result;
        if (!ticker) return null;
        const oiNow = parseFloat(ticker.oi_value_usd || ticker.oi_value || ticker.oi || 0);
        const oiPrev = oiSnapshotCache.get(symbol);
        oiSnapshotCache.set(symbol, oiNow);
        if (oiPrev === undefined) return null;
        if (!oiPrev) return null;
        const deltaPercent = (oiNow - oiPrev) / oiPrev * 100;
        return { oiNow, oiPrev, deltaPercent };
    } catch (e) {
        log(`OI delta fetch failed for ${symbol}: ${e.message}`, 'warning');
        return null;
    }
}

// Send Telegram notification with enhanced formatting
async function sendTelegramAlert(symbol, crossType, price, ema, difference) {
    try {
        const emoji = crossType === 'up' ? '🟢' : '🔴';
        const signal = crossType === 'up' ? 'BULLISH SIGNAL' : 'BEARISH SIGNAL';
        const formattedPrice = formatPrice(price);
        const formattedEma = formatPrice(ema);

        // Get 24hr stats for the symbol
        const stats = await get24HrStats(symbol);
        const oi = await getOIDelta(symbol).catch(() => null);

        // Create a chart link
        const chartUrl = getChartUrl(symbol);

        const oiLine = oi
            ? `<b>OI Delta:</b> ${oi.deltaPercent >= 0 ? '+' : ''}${oi.deltaPercent.toFixed(2)}% ${oi.deltaPercent >= 0.5 ? '\u{1F4C8} new money (stronger)' : oi.deltaPercent <= -0.5 ? '\u{1F4C9} liquidation (weaker)' : '\u2192 neutral'}\n`
            : '';
        const message = `${emoji} <b>${signal}</b> ${emoji}\n\n` +
            `<b>Symbol:</b> ${symbol}\n` +
            `<b>Price:</b> ${formattedPrice}\n` +
            `<b>EMA(${EMA_PERIOD}):</b> ${formattedEma}\n` +
            `<b>Difference:</b> ${difference.toFixed(2)}%\n` +
            `<b>24h Change:</b> ${stats.priceChangePercent}%\n` +
            `<b>24h Volume:</b> ${formatVolume(stats.quoteVolume)}\n` +
            oiLine +
            `<b>Timeframe:</b> ${activeTimeframeLabel()}\n\n` +
            `<b>Time:</b> ${new Date().toLocaleString()}\n\n` +
            `<a href="${getHtmlSafeUrl(chartUrl)}">View Chart on TradingView</a>`;

        await safeSendAlert(TELEGRAM_CHAT_ID, message, {
            parse_mode: 'HTML',
            disable_web_page_preview: true
        });

        // Show desktop notification — mirrors Telegram message content
        // Clicking the toast opens the chart in the browser
        showDesktopNotification(
            `${crossType === 'up' ? '🟢 BULLISH' : '🔴 BEARISH'} — ${symbol}`,
            `Price: ${formattedPrice}  EMA(${EMA_PERIOD}): ${formattedEma}\nDiff: ${difference.toFixed(2)}%  24h: ${stats.priceChangePercent}%\nVol: ${formatVolume(stats.quoteVolume)}  TF: ${TIMEFRAME}`,
            crossType === 'up' ? 'info' : 'warning',
            chartUrl
        );

        log(`Telegram alert sent for ${symbol} (${crossType})`, 'success');
    } catch (error) {
        log(`Error sending Telegram message: ${error.message}`, 'error');

        // Retry with simpler message if parse_mode might be the issue
        try {
            const simpleMessage = `${crossType === 'up' ? '🟢 BULLISH' : '🔴 BEARISH'} SIGNAL: ${symbol} at ${formatPrice(price)}`;
            await safeSendAlert(TELEGRAM_CHAT_ID, simpleMessage);
            log(`Sent simplified alert for ${symbol} after error`, 'warning');
        } catch (retryError) {
            log(`Failed to send even simplified message: ${retryError.message}`, 'error');
        }
    }
}

// Check if we should alert for this symbol based on direction change and cooldown
// tf — '5m' or '15m' in dual-TF mode; '' for single-mode (uses old key format)
function shouldAlert(symbol, currentState, tf = '') {
    const now = Date.now();
    // 4 independent cooldown buckets per symbol in dual-TF mode:
    //   BTCUSDT_5m_ema9_above  /  BTCUSDT_5m_ema9_below
    //   BTCUSDT_15m_ema9_above /  BTCUSDT_15m_ema9_below
    const stateKey = tf ? tfKey(symbol, tf) : symbol;
    const alertKey = tf ? `${symbol}_${tf}_${currentState}` : `${symbol}_${currentState}`;
    const previousState   = coinStates.get(stateKey);
    const lastAlertTime   = lastAlerts.get(alertKey) || 0;

    if (previousState !== currentState && now - lastAlertTime >= ALERT_COOLDOWN) {
        coinStates.set(stateKey, currentState);
        lastAlerts.set(alertKey, now);
        saveAlertState();
        traceAlert(`${symbol}${tf ? ` [${tf}]` : ''} state change ${previousState || 'none'} -> ${currentState}; alert allowed`);
        return true;
    } else if (previousState !== currentState) {
        log(`Alert for ${symbol}${tf ? ` [${tf.toUpperCase()}]` : ''} (${currentState}) skipped due to cooldown.`, 'warning');
        traceAlert(`${symbol}${tf ? ` [${tf}]` : ''} state change ${previousState || 'none'} -> ${currentState}; alert blocked by cooldown`);
    }
    return false;
}

// WebSocket setup for a symbol
// ---------------------------------------------------------------------------
// Pooled WebSocket architecture — a small number of connections (typically
// 1-3) each subscribe to many symbols, instead of one connection per symbol.
// This avoids too many simultaneous TCP handshakes on startup.
// ---------------------------------------------------------------------------

// Build the subscribe args array for a set of symbols
function buildSubscribeArgs(symbols) {
    const timeframes = DUAL_EMA_MODE ? getDualEmaTimeframes() : [TIMEFRAME];
    return timeframes.map(tf => ({
        name: `candlestick_${tf}`,
        symbols
    }));
}

// Create a single pool connection that subscribes to a chunk of symbols.
// Returns a poolEntry { ws, symbols, index } stored in wsPool[index].
function setupPoolConnection(index, symbols) {
    const poolKey = `pool_${index}`;
    const wsUrl = DELTA_PUBLIC_WS_URL;
    let reconnectScheduled = false;

    try {
        const ws = new WebSocket(wsUrl);
        const poolEntry = { ws, symbols: new Set(symbols), index };

        ws.on('open', () => {
            reconnectionAttempts.set(poolKey, 0);
            reconnectScheduled = false;
            log(`Pool WS #${index} connected — subscribing ${symbols.length} symbols`, 'success');
            ws.send(JSON.stringify({ type: 'subscribe', payload: { channels: buildSubscribeArgs(symbols) } }));
        });

        ws.on('message', (data) => {
            try {
                const message = JSON.parse(data);
                if (!message.type || !message.type.startsWith('candlestick_')) return;

                const symbol = message.sy;
                if (!symbol) return;
                const tf = message.res || message.type.replace('candlestick_', '');
                const wsKey = `${symbol}_${tf}`;
                const eventTs = normalizeWsTimestamp(message.ts);
                lastWsCandleTs.set(wsKey, eventTs);

                const kline = {
                    t: eventTs,
                    o: message.o,
                    h: message.h,
                    l: message.l,
                    c: message.c,
                    v: message.v || 0
                };

                // Always process updates and confirm closure inside processClosedCandle.
                // Relying on a provider-specific "x" flag causes missed closes or false positives.
                processClosedCandle(symbol, kline, DUAL_EMA_MODE ? tf : null);
            } catch (error) {
                log(`Error processing pool WS #${index} message: ${error.message}`, 'error');
            }
        });

        ws.on('error', (error) => {
            log(`Pool WS #${index} error: ${error.message}`, 'error');
            if (isReconnecting || reconnectScheduled) return;
            reconnectScheduled = true;

            const currentAttempts = reconnectionAttempts.get(poolKey) || 0;
            if (currentAttempts < MAX_RECONNECTION_ATTEMPTS) {
                reconnectionAttempts.set(poolKey, currentAttempts + 1);
                const backoff = Math.min(RECONNECTION_DELAY * Math.pow(2, currentAttempts), 5 * 60 * 1000);
                log(`Pool WS #${index} reconnecting in ${Math.round(backoff / 1000)}s (attempt ${currentAttempts + 1}/${MAX_RECONNECTION_ATTEMPTS})`, 'info');
                setTimeout(() => {
                    const live = Array.from(poolEntry.symbols).filter(s => trackedPairs.has(s));
                    if (live.length > 0) backfillAndReconnect(index, live);
                }, backoff);
            } else {
                log(`Pool WS #${index} max reconnection attempts reached`, 'warning');
            }
        });

        ws.on('close', () => {
            log(`Pool WS #${index} closed`, 'warning');
            // error handler already scheduled a reconnect — skip
            if (isReconnecting || reconnectScheduled) return;
            reconnectScheduled = true;

            const currentAttempts = reconnectionAttempts.get(poolKey) || 0;
            if (currentAttempts < MAX_RECONNECTION_ATTEMPTS) {
                reconnectionAttempts.set(poolKey, currentAttempts + 1);
                const backoff = Math.min(RECONNECTION_DELAY * Math.pow(2, currentAttempts), 5 * 60 * 1000);
                log(`Pool WS #${index} reconnecting in ${Math.round(backoff / 1000)}s (attempt ${currentAttempts + 1}/${MAX_RECONNECTION_ATTEMPTS})`, 'info');
                setTimeout(() => {
                    const live = Array.from(poolEntry.symbols).filter(s => trackedPairs.has(s));
                    if (live.length > 0) backfillAndReconnect(index, live);
                }, backoff);
            }
        });

        return poolEntry;
    } catch (error) {
        log(`Error creating pool WS #${index}: ${error.message}`, 'error');
        return null;
    }
}

// Reconnect pool connection with historical data backfill
async function backfillAndReconnect(index, symbols) {
    try {
        log(`Backfilling historical data for pool WS #${index} before reconnecting...`, 'info');
        const promises = [];
        if (DUAL_EMA_MODE) {
            for (const tf of getDualEmaTimeframes()) {
                for (const symbol of symbols) {
                    promises.push(() => getKlines(symbol, tf).catch(e => log(`Error backfilling ${symbol} [${tf}]: ${e.message}`, 'warning')));
                }
            }
        } else {
            for (const symbol of symbols) {
                promises.push(() => getKlines(symbol).catch(e => log(`Error backfilling ${symbol}: ${e.message}`, 'warning')));
            }
        }

        // Fetch in chunks
        await fetchInChunks(promises, 8, 1500);
        log(`Backfill complete for pool WS #${index}. Reconnecting...`, 'success');
    } catch (error) {
        log(`Error during backfill for pool WS #${index}: ${error.message}`, 'error');
    } finally {
        wsPool[index] = setupPoolConnection(index, symbols);
    }
}

// Tear down old pool connections and create a fresh pool for all symbols.
// Historical kline data is fetched once per symbol (not per reconnect).
function setupPooledWebSockets(allSymbols) {
    // Close old pool connections
    for (const entry of wsPool) {
        if (entry && entry.ws) {
            try { entry.ws.close(); } catch (e) { /* ignore */ }
        }
    }
    wsPool.length = 0;

    // Pre-create ML dirs & seed historical data for every symbol
    for (const symbol of allSymbols) {
        if (ML_ENABLED) {
            const safeSymbol = symbol.replace(/[^A-Z0-9]/g, '');
            fs.mkdirSync(path.join(ML_DATA_DIR, safeSymbol), { recursive: true });
        }
        if (DUAL_EMA_MODE) {
            for (const tf of getDualEmaTimeframes()) {
                getKlines(symbol, tf).catch(e => log(`Error loading ${tf} history for ${symbol}: ${e.message}`, 'error'));
            }
        } else {
            getKlines(symbol).catch(e => log(`Error loading history for ${symbol}: ${e.message}`, 'error'));
        }
    }

    // Chunk symbols into groups and stagger pool connection creation
    const chunks = [];
    for (let i = 0; i < allSymbols.length; i += WS_TOPICS_PER_CONN) {
        chunks.push(allSymbols.slice(i, i + WS_TOPICS_PER_CONN));
    }
    chunks.forEach((chunk, idx) => {
        setTimeout(() => {
            wsPool[idx] = setupPoolConnection(idx, chunk);
            log(`Pool WS #${idx} started with ${chunk.length} symbols`, 'info');
        }, idx * 500);  // 500 ms stagger between pool-level connections
    });
}

// Subscribe a single new symbol to an existing pool connection (mid-session discovery).
function subscribeSymbolToPool(symbol) {
    // Skip if already subscribed in any pool connection
    if (wsPool.some(e => e && e.symbols.has(symbol))) {
        log(`${symbol} already subscribed in pool — skipping`, 'info');
        return;
    }

    if (ML_ENABLED) {
        const safeSymbol = symbol.replace(/[^A-Z0-9]/g, '');
        fs.mkdirSync(path.join(ML_DATA_DIR, safeSymbol), { recursive: true });
    }
    if (DUAL_EMA_MODE) {
        for (const tf of getDualEmaTimeframes()) {
            getKlines(symbol, tf).catch(e => log(`Error loading ${tf} history for ${symbol}: ${e.message}`, 'error'));
        }
    } else {
        getKlines(symbol).catch(e => log(`Error loading history for ${symbol}: ${e.message}`, 'error'));
    }

    // Find a pool connection with room
    const target = wsPool.find(e => e && e.ws.readyState === WebSocket.OPEN && e.symbols.size < WS_TOPICS_PER_CONN);
    if (target) {
        target.symbols.add(symbol);
        target.ws.send(JSON.stringify({ type: 'subscribe', payload: { channels: buildSubscribeArgs([symbol]) } }));
        log(`Subscribed ${symbol} to pool WS #${target.index}`, 'info');
    } else {
        // No room or no open connections — create a new pool entry
        const newIdx = wsPool.length;
        wsPool[newIdx] = setupPoolConnection(newIdx, [symbol]);
        log(`Created new pool WS #${newIdx} for ${symbol}`, 'info');
    }
}


// Process a closed candle from WebSocket with improved ML data collection
// tf — '5m' or '15m' in dual-TF mode; null in single-mode (uses global TIMEFRAME)
async function processClosedCandle(symbol, kline, tf = null) {
    try {
        lastCandleTime = Date.now();
        // Composite key for dual-TF mode so 5m and 15m caches never overwrite each other
        const cacheKey = tf ? tfKey(symbol, tf) : symbol;

        // Get cached klines or initialize if not exists
        let klines = klineCache.get(cacheKey) || [];

        // Determine if it's a new candle bucket based on timeframe
        // Use the candle's own timestamp (kline.t) instead of Date.now() to handle delayed WS messages correctly
        const tfSecs = timeframeToSeconds(tf || TIMEFRAME);
        const tfMs = tfSecs * 1000;
        const candleTimeMs = Math.floor((kline.t * 1000) / tfMs) * tfMs;

        // Ignore stale messages that are older than our most recent candle
        if (klines.length > 0 && candleTimeMs < klines[klines.length - 1].time) {
            return;
        }

        const hadPreviousCandle = klines.length > 0;
        const isNewCandle = !hadPreviousCandle || candleTimeMs > klines[klines.length - 1].time;
        const justClosedKlines = isNewCandle && hadPreviousCandle ? klines.slice() : null;

        // Create new kline object
        const newKline = {
            time: candleTimeMs,
            open: parseFloat(kline.o),
            high: parseFloat(kline.h),
            low: parseFloat(kline.l),
            close: parseFloat(kline.c),
            volume: parseFloat(kline.v)
        };

        if (isNewCandle) {
            klines.push(newKline);
        } else {
            // Update the existing candle with the latest real-time data
            klines[klines.length - 1] = newKline;
        }

        // Keep cache size reasonable
        const maxCacheSize = DUAL_EMA_MODE ? 200 : EMA_PERIOD * 2;
        if (klines.length > maxCacheSize) {
            klines = klines.slice(-maxCacheSize);
        }

        klineCache.set(cacheKey, klines);

        // Get closes for EMA cache updates (includes live candle)
        const closes = klines.map(k => k.close);
        const volumes = klines.map(k => k.volume);

        // Calculate EMA values based on current mode
        if (DUAL_EMA_MODE) {
            // Incremental O(1) dual EMA update
            const updated = updateDualEMA(cacheKey, newKline.close, isNewCandle);
            if (!updated) {
                ema9Cache.set(cacheKey, calculateEMA(closes, 9));
                ema15Cache.set(cacheKey, calculateEMA(closes, 15));
            }

            // Only evaluate crossover when a candle is confirmed closed.
            // This avoids intrabar noise and false alerts.
            if (justClosedKlines && justClosedKlines.length >= 16) {
                const closedCloses = justClosedKlines.map(k => k.close);
                const closedEma9 = calculateEMA(closedCloses, 9);
                const closedEma15 = calculateEMA(closedCloses, 15);

                if (closedEma9.length >= 2 && closedEma15.length >= 2) {
                    // Align arrays — EMA(9) accumulates more entries than EMA(15)
                    const offset = closedEma9.length - closedEma15.length;
                    const a9  = offset > 0 ? closedEma9.slice(offset) : closedEma9;
                    const a15 = offset < 0 ? closedEma15.slice(-offset) : closedEma15;

                    await checkForDualEmaCrossover(
                        symbol,
                        a9.at(-2), a9.at(-1),
                        a15.at(-2), a15.at(-1),
                        justClosedKlines.at(-1).close, tf
                    );
                }
            }
        } else {
            // Single EMA mode — update incrementally
            const updated = updateEMA(symbol, newKline.close, isNewCandle);
            if (!updated) {
                const freshEma = calculateEMA(closes, EMA_PERIOD);
                emaCache.set(symbol, freshEma);
            }

            // Only evaluate crossover when a candle is confirmed closed.
            if (justClosedKlines && justClosedKlines.length >= EMA_PERIOD + 1) {
                const closedCloses = justClosedKlines.map(k => k.close);
                const closedEma = calculateEMA(closedCloses, EMA_PERIOD);
                if (closedEma.length >= 2) {
                    const lastPrice = closedCloses.at(-1);
                    const prevPrice = closedCloses.at(-2);
                    const lastEMA = closedEma.at(-1);
                    const prevEMA = closedEma.at(-2);
                    await checkForCrossover(symbol, prevPrice, lastPrice, prevEMA, lastEMA);
                }
            }
        }

        // Collect data for ML training ONLY on candle close (when a new candle bucket starts)
        // to avoid writing 100s of identical data points per candle to disk.
        if (justClosedKlines && ML_ENABLED && !DUAL_EMA_MODE && mlRuntime) {
            const emaValues = emaCache.get(symbol) || [];
            await mlRuntime.onClosedCandleForTraining(symbol, justClosedKlines, emaValues, deferredInsert, deferredUpdates);
        }
    } catch (error) {
        log(`Error processing closed candle for ${symbol}: ${error.message}`, 'error');
    }
}

function exportToCSV(symbol) {
    if (mlRuntime) mlRuntime.exportToCSV(symbol);
}

// Export all training data to CSV
function exportAllDataToCSV() {
    if (mlRuntime) mlRuntime.exportAllDataToCSV();
}

// Check for crossover and send alerts if needed with ML prediction
async function checkForCrossover(symbol, prevPrice, lastPrice, prevEMA, lastEMA) {
    try {
        if (![prevPrice, lastPrice, prevEMA, lastEMA].every(Number.isFinite) || prevEMA === 0 || lastEMA === 0) {
            traceAlert(`${symbol} single-mode skipped: invalid numeric inputs`);
            return;
        }

        // Determine current state (above or below EMA)
        const currentState = lastPrice > lastEMA ? 'above' : 'below';
        const difference = (lastPrice - lastEMA) / lastEMA * 100;

        // Get ML prediction if available
        let prediction = null;
        if (ML_ENABLED) {
            try {
                prediction = await predictPriceMovement(symbol, lastPrice, lastEMA, difference);
            } catch (predictionError) {
                log(`Error getting prediction for ${symbol}: ${predictionError.message}`, 'warning');
                // Continue without prediction
            }
        }

        // Upward crossover: price crossing from below to above EMA (with minimum margin)
        if (prevPrice < prevEMA && lastPrice > lastEMA && (lastPrice - lastEMA) / lastEMA > MIN_CROSS_PCT) {
            traceAlert(`${symbol} single-mode bullish candidate diff=${difference.toFixed(4)}%`);
            console.log('\n');
            console.log('▲'.green + ' UPWARD CROSSOVER '.white.bgGreen + ' ' + symbol.bold);
            console.log(`  Previous Price: ${formatPrice(prevPrice).gray} → Current Price: ${formatPrice(lastPrice).green}`);
            console.log(`  Previous EMA: ${formatPrice(prevEMA).gray} → Current EMA: ${formatPrice(lastEMA).cyan}`);
            console.log(`  Difference: ${difference.toFixed(2)}%`.yellow);

            if (prediction !== null) {
                console.log(`  ML Prediction: ${prediction.toFixed(2)}% expected change`.cyan);
            }

            // CRITICAL: Check flat EMA BEFORE calling shouldAlert() to avoid wasting cooldown
            const flatCheck = isEmaFlat(symbol, '', false); // single-mode, useDualEma = false

            if (flatCheck.isFlat) {
                // Update state so next real crossover is detected, but don't consume cooldown
                coinStates.set(symbol, currentState);
                traceAlert(`${symbol} single-mode bullish suppressed: ${flatCheck.reason}`);
                return;
            }

            if (shouldAlert(symbol, currentState)) {
                if (prediction !== null) {
                    await sendTelegramAlertWithML(symbol, 'up', lastPrice, lastEMA, difference, prediction);
                } else {
                    await sendTelegramAlert(symbol, 'up', lastPrice, lastEMA, difference);
                }
                traceAlert(`${symbol} single-mode bullish alert emitted`);
            }
        }
        // Downward crossover: price crossing from above to below EMA (with minimum margin)
        else if (prevPrice > prevEMA && lastPrice < lastEMA && (lastEMA - lastPrice) / lastEMA > MIN_CROSS_PCT) {
            traceAlert(`${symbol} single-mode bearish candidate diff=${difference.toFixed(4)}%`);
            console.log('\n');
            console.log('▼'.red + ' DOWNWARD CROSSOVER '.white.bgRed + ' ' + symbol.bold);
            console.log(`  Previous Price: ${formatPrice(prevPrice).gray} → Current Price: ${formatPrice(lastPrice).red}`);
            console.log(`  Previous EMA: ${formatPrice(prevEMA).gray} → Current EMA: ${formatPrice(lastEMA).cyan}`);
            console.log(`  Difference: ${difference.toFixed(2)}%`.yellow);

            if (prediction !== null) {
                console.log(`  ML Prediction: ${prediction.toFixed(2)}% expected change`.cyan);
            }

            // CRITICAL: Check flat EMA BEFORE calling shouldAlert() to avoid wasting cooldown
            const flatCheck = isEmaFlat(symbol, '', false); // single-mode, useDualEma = false

            if (flatCheck.isFlat) {
                // Update state so next real crossover is detected, but don't consume cooldown
                coinStates.set(symbol, currentState);
                traceAlert(`${symbol} single-mode bearish suppressed: ${flatCheck.reason}`);
                return;
            }

            if (shouldAlert(symbol, currentState)) {
                if (prediction !== null) {
                    await sendTelegramAlertWithML(symbol, 'down', lastPrice, lastEMA, difference, prediction);
                } else {
                    await sendTelegramAlert(symbol, 'down', lastPrice, lastEMA, difference);
                }
                traceAlert(`${symbol} single-mode bearish alert emitted`);
            }
        } else {
            // Update state even if no crossover
            coinStates.set(symbol, currentState);
            traceAlert(`${symbol} single-mode no cross; state=${currentState} diff=${difference.toFixed(4)}%`);
        }
    } catch (error) {
        log(`Error checking for crossover for ${symbol}: ${error.message}`, 'error');
    }
}

// Check for dual EMA(9) vs EMA(15) crossover
// tf — '5m' or '15m' in dual-TF mode; '' in legacy single-TF mode
async function checkForDualEmaCrossover(symbol, prevEma9, lastEma9, prevEma15, lastEma15, currentPrice, tf = '') {
    try {
        if (![prevEma9, lastEma9, prevEma15, lastEma15].every(Number.isFinite) || prevEma15 === 0 || lastEma15 === 0) {
            traceAlert(`${symbol}${tf ? ` [${tf}]` : ''} dual-mode skipped: invalid numeric inputs`);
            return;
        }

        // State: is EMA(9) above or below EMA(15)?
        const currentState = lastEma9 > lastEma15 ? 'ema9_above' : 'ema9_below';
        const difference = (lastEma9 - lastEma15) / lastEma15 * 100;
        const tfTag = tf ? ` [${tf.toUpperCase()}]` : '';

        // Bullish: EMA(9) crosses above EMA(15) (with minimum margin)
        if (prevEma9 < prevEma15 && lastEma9 > lastEma15 && (lastEma9 - lastEma15) / lastEma15 > MIN_CROSS_PCT) {
            traceAlert(`${symbol}${tf ? ` [${tf}]` : ''} dual bullish candidate spread=${difference.toFixed(4)}%`);
            console.log('\n');
            console.log('▲'.green + ` EMA 9/15 BULLISH CROSSOVER${tfTag} `.white.bgGreen + ' ' + symbol.bold);
            console.log(`  EMA(9): ${formatPrice(prevEma9).gray} → ${formatPrice(lastEma9).green}`);
            console.log(`  EMA(15): ${formatPrice(prevEma15).gray} → ${formatPrice(lastEma15).cyan}`);
            console.log(`  Price: ${formatPrice(currentPrice).white}`);
            console.log(`  EMA Spread: ${difference.toFixed(4)}%`.yellow);

            // CRITICAL: Check flat EMA BEFORE calling shouldAlert() to avoid wasting cooldown
            const flatCheck = isEmaFlat(symbol, tf, true); // useDualEma = true (check EMA15)
            const stateKey = tf ? tfKey(symbol, tf) : symbol;

            if (flatCheck.isFlat) {
                // Update state so next real crossover is detected, but don't consume cooldown
                coinStates.set(stateKey, currentState);
                traceAlert(`${symbol}${tf ? ` [${tf}]` : ''} dual bullish suppressed: ${flatCheck.reason}`);
                return;
            }

            if (shouldAlert(symbol, currentState, tf)) {
                await sendDualEmaAlert(symbol, 'up', currentPrice, lastEma9, lastEma15, difference, tf);
                traceAlert(`${symbol}${tf ? ` [${tf}]` : ''} dual bullish alert emitted`);
            }
        }
        // Bearish: EMA(9) crosses below EMA(15) (with minimum margin)
        else if (prevEma9 > prevEma15 && lastEma9 < lastEma15 && (lastEma15 - lastEma9) / lastEma15 > MIN_CROSS_PCT) {
            traceAlert(`${symbol}${tf ? ` [${tf}]` : ''} dual bearish candidate spread=${difference.toFixed(4)}%`);
            console.log('\n');
            console.log('▼'.red + ` EMA 9/15 BEARISH CROSSOVER${tfTag} `.white.bgRed + ' ' + symbol.bold);
            console.log(`  EMA(9): ${formatPrice(prevEma9).gray} → ${formatPrice(lastEma9).red}`);
            console.log(`  EMA(15): ${formatPrice(prevEma15).gray} → ${formatPrice(lastEma15).cyan}`);
            console.log(`  Price: ${formatPrice(currentPrice).white}`);
            console.log(`  EMA Spread: ${difference.toFixed(4)}%`.yellow);

            // CRITICAL: Check flat EMA BEFORE calling shouldAlert() to avoid wasting cooldown
            const flatCheck = isEmaFlat(symbol, tf, true); // useDualEma = true (check EMA15)
            const stateKey = tf ? tfKey(symbol, tf) : symbol;

            if (flatCheck.isFlat) {
                // Update state so next real crossover is detected, but don't consume cooldown
                coinStates.set(stateKey, currentState);
                traceAlert(`${symbol}${tf ? ` [${tf}]` : ''} dual bearish suppressed: ${flatCheck.reason}`);
                return;
            }

            if (shouldAlert(symbol, currentState, tf)) {
                await sendDualEmaAlert(symbol, 'down', currentPrice, lastEma9, lastEma15, difference, tf);
                traceAlert(`${symbol}${tf ? ` [${tf}]` : ''} dual bearish alert emitted`);
            }
        } else {
            // No crossover — update tracked state so future crossovers are detected
            const stateKey = tf ? tfKey(symbol, tf) : symbol;
            coinStates.set(stateKey, currentState);
            traceAlert(`${symbol}${tf ? ` [${tf}]` : ''} dual no cross; state=${currentState} spread=${difference.toFixed(4)}%`);
        }
    } catch (error) {
        log(`Error checking dual EMA crossover for ${symbol}: ${error.message}`, 'error');
    }
}

// Send Telegram alert for dual EMA 9/15 crossover
// tf — '5m' or '15m' in dual-TF mode; '' for legacy single-TF mode
async function sendDualEmaAlert(symbol, crossType, price, ema9, ema15, spread, tf = '') {
    try {
        const emoji   = crossType === 'up' ? '🟢' : '🔴';
        const tfLabel = tf ? ` [${tf.toUpperCase()}]` : '';
        const signal  = crossType === 'up'
            ? `BULLISH EMA 9/15 CROSS${tfLabel}`
            : `BEARISH EMA 9/15 CROSS${tfLabel}`;

        // Get 24hr stats
        const stats = await get24HrStats(symbol);
        const oi = await getOIDelta(symbol).catch(() => null);

        // Chart link - pass timeframe for correct interval on TradingView
        const chartUrl = getChartUrl(symbol, tf);

        const oiLine = oi
            ? `<b>OI Delta:</b> ${oi.deltaPercent >= 0 ? '+' : ''}${oi.deltaPercent.toFixed(2)}% ${oi.deltaPercent >= 0.5 ? '\u{1F4C8} new money (stronger)' : oi.deltaPercent <= -0.5 ? '\u{1F4C9} liquidation (weaker)' : '\u2192 neutral'}\n`
            : '';
        const message = `${emoji} <b>${signal}</b> ${emoji}\n\n` +
            `<b>Symbol:</b> ${symbol}\n` +
            `<b>Price:</b> ${formatPrice(price)}\n` +
            `<b>EMA(9):</b> ${formatPrice(ema9)}\n` +
            `<b>EMA(15):</b> ${formatPrice(ema15)}\n` +
            `<b>EMA Spread:</b> ${spread.toFixed(4)}%\n` +
            `<b>24h Change:</b> ${stats.priceChangePercent}%\n` +
            `<b>24h Volume:</b> ${formatVolume(stats.quoteVolume)}\n` +
            oiLine +
            `<b>Timeframe:</b> ${activeTimeframeLabel(tf)}\n\n` +
            `<b>Time:</b> ${new Date().toLocaleString()}\n\n` +
            `<a href="${getHtmlSafeUrl(chartUrl)}">View Chart on TradingView</a>`;

        await safeSendAlert(TELEGRAM_CHAT_ID, message, {
            parse_mode: 'HTML',
            disable_web_page_preview: true
        });

        // Show desktop notification — mirrors Telegram message content
        // Clicking the toast opens the chart in the browser
        showDesktopNotification(
            `${crossType === 'up' ? '🟢 EMA 9/15 BULL' : '🔴 EMA 9/15 BEAR'}${tfLabel} — ${symbol}`,
            `EMA(9): ${formatPrice(ema9)}  EMA(15): ${formatPrice(ema15)}\nSpread: ${spread.toFixed(4)}%  24h: ${stats.priceChangePercent}%\nVol: ${formatVolume(stats.quoteVolume)}  TF: ${tf || TIMEFRAME}`,
            crossType === 'up' ? 'info' : 'warning',
            chartUrl
        );

        log(`Dual EMA alert sent for ${symbol}${tfLabel} (${crossType})`, 'success');
    } catch (error) {
        log(`Error sending dual EMA alert: ${error.message}`, 'error');
        try {
            const tfLabel = tf ? ` [${tf.toUpperCase()}]` : '';
            const simpleMsg = `${crossType === 'up' ? '🟢 BULLISH' : '🔴 BEARISH'} EMA 9/15 CROSS${tfLabel}: ${symbol} at ${formatPrice(price)}`;
            await safeSendAlert(TELEGRAM_CHAT_ID, simpleMsg);
        } catch (retryError) {
            log(`Failed to send even simplified dual EMA message: ${retryError.message}`, 'error');
        }
    }
}

// Function to make price movement prediction
async function predictPriceMovement(symbol, price, ema, emaDiff) {
    if (!mlRuntime || !ML_ENABLED) return null;
    return mlRuntime.predictPriceMovement(symbol, price, ema, emaDiff, deferredInsert, deferredUpdates);
}

// Update model accuracy after 24 hours
async function updateModelAccuracy(symbol, originalPrice, prediction) {
    // Handled inside ML runtime; retained for backward compatibility.
    return;
}

// Setup WebSockets for all tracked pairs
async function setupAllWebSockets() {
    try {
        const pairs = await getFuturesPairs();
        const pairSet = new Set(pairs);

        fs.appendFileSync(
            getDailyLogPath(),
            `[${new Date().toISOString()}] Setting up pooled WebSockets for ${pairs.length} pairs\n`
        );

        // Clean caches for symbols that dropped out of the active pair list
        for (const symbol of Array.from(trackedPairs)) {
            if (!pairSet.has(symbol)) {
                fs.appendFileSync(
                    getDailyLogPath(),
                    `[${new Date().toISOString()}] Cleaning caches for ${symbol} (no longer tracked)\n`
                );
                trackedPairs.delete(symbol);

                const tfKeysToClear = new Set(['5m', '15m', ...getDualEmaTimeframes()]);
                const cacheKeysToClear = [symbol, ...Array.from(tfKeysToClear).map(tf => tfKey(symbol, tf))];
                for (const key of cacheKeysToClear) {
                    klineCache.delete(key);
                    emaCache.delete(key);
                    ema9Cache.delete(key);
                    ema15Cache.delete(key);
                }
                for (const key of [...coinStates.keys()].filter(k => k === symbol || k.startsWith(`${symbol}_`))) {
                    coinStates.delete(key);
                }
                for (const key of [...lastAlerts.keys()].filter(k => k === symbol || k.startsWith(`${symbol}_`))) {
                    lastAlerts.delete(key);
                }
                for (const key of [...flatEmaHysteresisCounters.keys()].filter(k => k === symbol || k.startsWith(`${symbol}_`))) {
                    flatEmaHysteresisCounters.delete(key);
                }
            }
        }

        // Close old pool connections and recreate with the current pair list
        setupPooledWebSockets(pairs);

        fs.appendFileSync(
            getDailyLogPath(),
            `[${new Date().toISOString()}] Pooled WebSocket setup completed — ${pairs.length} symbols across ${Math.ceil(pairs.length / WS_TOPICS_PER_CONN)} connection(s)\n`
        );
    } catch (error) {
        fs.appendFileSync(
            getDailyLogPath(),
            `[${new Date().toISOString()}] Error setting up pooled WebSockets: ${error.message}\n`
        );
    }
}

// Fetch REST promises in small batches to avoid saturating enforceRateLimit.
// Without chunking, a concurrent Promise.all on 80+ getKlines calls all read the
// same lastApiCall timestamp in the same millisecond — effectively bypassing the
// limiter and increasing 429/backoff risk.
async function fetchInChunks(factories, chunkSize = 8, delayMs = 1500) {
    const results = [];
    for (let i = 0; i < factories.length; i += chunkSize) {
        const batch = factories.slice(i, i + chunkSize).map(f => f());
        results.push(...await Promise.all(batch));
        if (i + chunkSize < factories.length) {
            await new Promise(r => setTimeout(r, delayMs));
        }
    }
    return results;
}

// Check for EMA crossovers (traditional method, still used for initial load and periodic checks)
// NOTE: Race condition awareness — Both checkEMACross() (periodic) and processClosedCandle() (WebSocket)
// can call shouldAlert() concurrently. In Node.js, two async chains can read the same coinStates value
// before either writes back, because await in get24HrStats() and getOIDelta() yields the event loop.
// The flat EMA check (synchronous in isEmaFlat) does not eliminate this window — both chains can still
// pass the filter and race into shouldAlert(). This is a known architectural limitation.
// Mitigation: ALERT_COOLDOWN prevents immediate duplicate alerts, but very rare duplicates may still occur.
async function checkEMACross({ emitAlerts = false } = {}) {
    try {
        const pairs = await getFuturesPairs();
        const timestamp = new Date().toLocaleString();

        console.log(`\n[${timestamp}] ${'Checking'.cyan} ${pairs.length.toString().yellow} ${'pairs...'.cyan}`);
        process.stdout.write('Processing: '.cyan);

        // Fetch klines for all pairs concurrently with error handling
        let results;

        if (DUAL_EMA_MODE) {
            // In dual-TF mode fetch all active timeframes for every pair
            const dualPromises = [];
            for (const tf of getDualEmaTimeframes()) {
                for (const pair of pairs) {
                    dualPromises.push(
                        () => getKlines(pair, tf)
                            .then(klines => ({ pair, tf, klines, error: null }))
                            .catch(error => ({ pair, tf, klines: [], error }))
                    );
                }
            }
            results = await fetchInChunks(dualPromises);
        } else {
            // Single-mode: use the same chunked fetcher as dual-mode so all 80+
            // getKlines calls don't race to read the same lastApiCall timestamp,
            // bypassing enforceRateLimit and triggering rate-limit stalls.
            const singlePromises = pairs.map(pair =>
                () => getKlines(pair)
                    .then(klines => ({ pair, tf: null, klines, error: null }))
                    .catch(error => ({ pair, tf: null, klines: [], error }))
            );
            results = await fetchInChunks(singlePromises);
        }

        for (let i = 0; i < results.length; i++) {
            const { pair, tf, klines, error } = results[i];
            process.stdout.write('.');
            if ((i + 1) % 50 === 0) process.stdout.write('\n  ');

            const requiredPeriod = DUAL_EMA_MODE ? 15 : EMA_PERIOD;
            if (error || klines.length < requiredPeriod) {
                if (klines.length < requiredPeriod) {
                    log(`Skipping ${pair}${tf ? ` [${tf}]` : ''}: Not enough candles (${klines.length}/${requiredPeriod})`, 'warning');
                }
                continue;
            }

            if (DUAL_EMA_MODE) {
                // Read EMAs from cache — getKlines() already populated them
                const key = tfKey(pair, tf);
                const e9  = ema9Cache.get(key)  || [];
                const e15 = ema15Cache.get(key) || [];

                if (e9.length < 2 || e15.length < 2) {
                    log(`Skipping ${pair} [${tf}]: Not enough EMA values`, 'warning');
                    continue;
                }

                // Align arrays — EMA(9) accumulates 6 more values than EMA(15)
                const _offset = e9.length - e15.length;
                const a9  = _offset > 0 ? e9.slice(_offset)   : e9;
                const a15 = _offset < 0 ? e15.slice(-_offset) : e15;

                const closes = klines.map(k => k.close);
                if (emitAlerts) {
                    await checkForDualEmaCrossover(
                        pair,
                        a9.at(-2), a9.at(-1),
                        a15.at(-2), a15.at(-1),
                        closes.at(-1), tf
                    );
                } else {
                    coinStates.set(tfKey(pair, tf), a9.at(-1) > a15.at(-1) ? 'ema9_above' : 'ema9_below');
                }
            } else {
                // Single EMA crossover check (price vs EMA)
                const closes = klines.map(k => k.close);
                const ema = calculateEMA(closes, EMA_PERIOD);

                if (ema.length < 2) {
                    log(`Skipping ${pair}: Not enough EMA values calculated`, 'warning');
                    continue;
                }

                const lastPrice = closes[closes.length - 1];
                const lastEMA = ema[ema.length - 1];
                const prevPrice = closes[closes.length - 2];
                const prevEMA = ema[ema.length - 2];

                if (emitAlerts) {
                    await checkForCrossover(pair, prevPrice, lastPrice, prevEMA, lastEMA);
                } else {
                    coinStates.set(pair, lastPrice > lastEMA ? 'above' : 'below');
                }
            }
        }

        console.log('\n');
        console.log(`Check completed at ${timestamp}. WebSockets are now monitoring in real-time.`.gray);
        console.log('='.repeat(80).dim);
    } catch (error) {
        log(`Error in checkEMACross: ${error.message}`, 'error');
        console.error('Stack trace:', error.stack);
    }
}

// Save training data to disk (both JSON and CSV)
function saveTrainingData() {
    if (mlRuntime) mlRuntime.saveTrainingData();
}

// Load training data from disk
function loadTrainingData() {
    if (mlRuntime) mlRuntime.loadTrainingData();
}

// Function to train all models
async function trainAllModels(chatId) {
    if (mlRuntime) await mlRuntime.trainAllModels(chatId);
}

// Enhanced Telegram alert with ML confidence
async function sendTelegramAlertWithML(symbol, crossType, price, ema, difference, prediction) {
    try {
        const emoji = crossType === 'up' ? '🟢' : '🔴';
        const signal = crossType === 'up' ? 'BULLISH SIGNAL' : 'BEARISH SIGNAL';
        const formattedPrice = formatPrice(price);
        const formattedEma = formatPrice(ema);

        // Get 24hr stats for the symbol
        const stats = await get24HrStats(symbol);
        const oi = await getOIDelta(symbol).catch(() => null);

        // Format ML prediction with confidence emoji
        let confidenceEmoji = '⚠️'; // Neutral/uncertain
        if (Math.abs(prediction) > 3) {
            confidenceEmoji = prediction > 0 ? '🔥' : '❄️'; // Strong signal
        } else if (Math.abs(prediction) > 1) {
            confidenceEmoji = prediction > 0 ? '📈' : '📉'; // Moderate signal
        }

        const chartUrl = getChartUrl(symbol);

        const oiLine = oi
            ? `<b>OI Delta:</b> ${oi.deltaPercent >= 0 ? '+' : ''}${oi.deltaPercent.toFixed(2)}% ${oi.deltaPercent >= 0.5 ? '📈 new money (stronger)' : oi.deltaPercent <= -0.5 ? '📉 liquidation (weaker)' : '→ neutral'}\n`
            : '';
        const message = `${emoji} <b>${signal}</b> ${emoji}\n\n` +
            `<b>Symbol:</b> ${symbol}\n` +
            `<b>Price:</b> ${formattedPrice}\n` +
            `<b>EMA(${EMA_PERIOD}):</b> ${formattedEma}\n` +
            `<b>Difference:</b> ${difference.toFixed(2)}%\n` +
            `<b>24h Change:</b> ${stats.priceChangePercent}%\n` +
            `<b>24h Volume:</b> ${formatVolume(stats.quoteVolume)}\n` +
            oiLine +
            `<b>Timeframe:</b> ${activeTimeframeLabel()}\n` +
            `<b>ML Prediction:</b> ${confidenceEmoji} ${prediction.toFixed(2)}% (24h)\n\n` +
            `<b>Time:</b> ${new Date().toLocaleString()}\n\n` +
            `<a href="${getHtmlSafeUrl(chartUrl)}">View Chart on TradingView</a>`;

        await safeSendAlert(TELEGRAM_CHAT_ID, message, {
            parse_mode: 'HTML',
            disable_web_page_preview: true
        });

        // Show desktop notification — mirrors Telegram message content
        // Clicking the toast opens the chart in the browser
        showDesktopNotification(
            `${crossType === 'up' ? '🟢 BULLISH+ML' : '🔴 BEARISH+ML'} — ${symbol}`,
            `Price: ${formattedPrice}  EMA(${EMA_PERIOD}): ${formattedEma}\nDiff: ${difference.toFixed(2)}%  ML: ${confidenceEmoji} ${prediction.toFixed(2)}% (24h)\n24h: ${stats.priceChangePercent}%  TF: ${TIMEFRAME}`,
            crossType === 'up' ? 'info' : 'warning',
            chartUrl
        );

        log(`ML-enhanced Telegram alert sent for ${symbol} (${crossType})`, 'success');
    } catch (error) {
        log(`Error sending ML-enhanced Telegram message: ${error.message}`, 'error');

        // Fall back to regular alert
        await sendTelegramAlert(symbol, crossType, price, ema, difference)
            .catch(e => log(`Fallback alert failed for ${symbol}: ${e.message}`, 'error'));
    }
}

// Command handler
async function handleMessage(msg) {
    const chatId = msg.chat.id;

    // Authorization: only the configured owner may control this bot
    if (chatId.toString() !== TELEGRAM_CHAT_ID) {
        bot.sendMessage(chatId, '⛔ Unauthorized.').catch(() => {});
        log(`Unauthorized access attempt from chatId ${chatId}`, 'warning');
        return;
    }

    if (msg.text === '/start' || msg.text === '/menu') {
        sendMainMenu(chatId);
    } else if (msg.text === '/status') {
        sendStatusUpdate(chatId);
    } else if (msg.text === '/settings') {
        sendSettingsMenu(chatId);
    } else if (msg.text === '/help') {
        sendHelpMessage(chatId);
    } else if (msg.text === '/top') {
        sendTopPerformers(chatId);
    } else if (msg.text === '/refresh') {
        refreshWebSockets(chatId);
    } else if (msg.text === '/mlstatus') {
        sendModelPerformance(chatId);
    } else if (msg.text === '/train') {
        const guard = beginCommand('/train');
        if (!guard.ok) {
            await bot.sendMessage(chatId, '⚠️ /train is already running or cooling down. Please wait.').catch(() => {});
            return;
        }
        try {
            await trainAllModels(chatId);
        } finally {
            endCommand('/train');
        }
    } else if (msg.text === '/collectdata') {
        const guard = beginCommand('/collectdata');
        if (!guard.ok) {
            await bot.sendMessage(chatId, '⚠️ /collectdata is already running or cooling down. Please wait.').catch(() => {});
            return;
        }
        try {
            await startManualDataCollection(chatId);
        } finally {
            endCommand('/collectdata');
        }
    } else if (msg.text === '/exportcsv') {
        exportAllDataToCSV();
        bot.sendMessage(chatId, '📊 All training data exported to CSV format successfully!');
    }
}

// Function to manually collect data for all tracked pairs
async function startManualDataCollection(chatId) {
    if (mlRuntime) await mlRuntime.startManualDataCollection(chatId);
}

// Graceful WebSocket reconnect — closes all connections, waits 30 s, then reconnects
// with whatever settings (TIMEFRAME, EMA_PERIOD, VOLUME_THRESHOLD) are currently active.
// Uses isReconnecting to prevent concurrent reconnects.
async function refreshWebSockets(chatId) {
    if (isReconnecting) {
        if (chatId) await bot.sendMessage(chatId, '⚠️ A reconnection is already in progress. Please wait for it to finish.').catch(() => {});
        return;
    }
    isReconnecting = true;
    try {
        log('Graceful reconnect started — closing all WebSocket streams...', 'info');
        if (chatId) await bot.sendMessage(chatId, '⏳ *Disconnecting all WebSocket streams...*', { parse_mode: 'Markdown' }).catch(() => {});

        // Terminate every pool connection
        for (const entry of wsPool) {
            if (entry && entry.ws) { try { entry.ws.close(); } catch (e) { /* ignore */ } }
        }
        wsPool.length = 0;
        reconnectionAttempts.clear();

        log('All WebSockets closed. Waiting 10 s before reconnecting...', 'info');
        if (chatId) await bot.sendMessage(chatId, '⏳ Waiting 10 seconds before reconnecting...').catch(() => {});

        await new Promise(resolve => setTimeout(resolve, 10000));

        log('Reconnecting WebSockets with current settings...', 'info');
        await setupAllWebSockets();

        if (chatId) await bot.sendMessage(chatId, '✅ *WebSocket connections re-established!*', { parse_mode: 'Markdown' }).catch(() => {});
        log('Graceful reconnect complete.', 'success');
    } catch (error) {
        log(`Error during graceful reconnect: ${error.message}`, 'error');
        if (chatId) await bot.sendMessage(chatId, `❌ Reconnect error: ${error.message}`).catch(() => {});
    } finally {
        isReconnecting = false;
    }
}

// Callback query handler for inline buttons
async function handleCallbackQuery(callbackQuery) {
    const action = callbackQuery.data;
    const chatId = callbackQuery.message.chat.id;

    // Authorization: silently reject callbacks from anyone other than the owner
    if (chatId.toString() !== TELEGRAM_CHAT_ID) {
        await bot.answerCallbackQuery(callbackQuery.id, { text: '⛔ Unauthorized.' }).catch(() => {});
        log(`Unauthorized callback attempt from chatId ${chatId}`, 'warning');
        return;
    }

    try {
        if (action === 'status') {
            await sendStatusUpdate(chatId);
        } else if (action === 'settings') {
            await sendSettingsMenu(chatId);
        } else if (action === 'top_gainers') {
            await sendTopPerformers(chatId, 'gainers');
        } else if (action === 'top_losers') {
            await sendTopPerformers(chatId, 'losers');
        } else if (action === 'top_volume') {
            await sendTopPerformers(chatId, 'volume');
        } else if (action === 'menu') {
            await sendMainMenu(chatId);
        } else if (action === 'help') {
            await sendHelpMessage(chatId);
        } else if (action === 'refresh_ws') {
            refreshWebSockets(chatId); // runs in background, sends its own progress messages
        } else if (action === 'export_csv') {
            exportAllDataToCSV();
            await bot.sendMessage(chatId, '📊 All training data exported to CSV format successfully!');
        } else if (action.startsWith('timeframe_')) {
            const newTimeframe = action.replace('timeframe_', '');
            if (!VALID_TIMEFRAMES.includes(newTimeframe)) {
                await bot.answerCallbackQuery(callbackQuery.id, { text: '⛔ Invalid timeframe.' });
                return;
            }
            TIMEFRAME = newTimeframe;
            log(`Timeframe updated to ${newTimeframe}`, 'success');
            saveSettings();
            await sendSettingsMenu(chatId); // show updated menu immediately
            refreshWebSockets(chatId);      // reconnect in background — sends its own progress msgs
        } else if (action.startsWith('ema_')) {
            const newEma = parseInt(action.replace('ema_', ''), 10);
            if (!VALID_EMA_PERIODS.includes(newEma)) {
                await bot.answerCallbackQuery(callbackQuery.id, { text: '⛔ Invalid EMA period.' });
                return;
            }
            EMA_PERIOD = newEma;
            DUAL_EMA_MODE = false;
            log(`EMA period updated to ${newEma}, dual mode disabled`, 'success');
            saveSettings();
            ema9Cache.clear();
            ema15Cache.clear();
            emaCache.clear();
            coinStates.clear();
            await sendSettingsMenu(chatId); // show updated menu immediately
            refreshWebSockets(chatId);      // reconnect in background — sends its own progress msgs
        } else if (action === 'toggle_dual_ema') {
            DUAL_EMA_MODE = !DUAL_EMA_MODE;
            log(`Dual EMA 9/15 mode ${DUAL_EMA_MODE ? 'enabled' : 'disabled'}`, 'success');
            saveSettings();
            emaCache.clear();
            ema9Cache.clear();
            ema15Cache.clear();
            coinStates.clear();
            await bot.sendMessage(
                chatId,
                DUAL_EMA_MODE
                    ? `✅ *EMA 9/15 Crossover Mode Enabled [${getDualEmaTimeframes().join(' + ')}]*\nAlerts will fire when EMA(9) crosses EMA(15) on all active dual-mode timeframes independently. Single EMA settings are ignored.`
                    : `✅ *EMA 9/15 Mode Disabled*\nReverted to Price vs EMA(${EMA_PERIOD}) crossover mode.`,
                { parse_mode: 'Markdown' }
            );
            await sendSettingsMenu(chatId); // show updated menu immediately
            refreshWebSockets(chatId);      // reconnect in background — sends its own progress msgs
        } else if (action === 'toggle_fast_tfs') {
            if (FORCE_ALL_DUAL_TFS) {
                await bot.sendMessage(
                    chatId,
                    '⚡ *Fast TF Add-on is locked ON by FORCE_ALL_DUAL_TFS=true*\nDual EMA mode always monitors 1m + 3m + 5m + 15m.',
                    { parse_mode: 'Markdown' }
                );
                await sendSettingsMenu(chatId);
                return;
            }

            INCLUDE_FAST_TFS = !INCLUDE_FAST_TFS;
            log(`Fast dual timeframes ${INCLUDE_FAST_TFS ? 'enabled' : 'disabled'}`, 'success');
            saveSettings();
            emaCache.clear();
            ema9Cache.clear();
            ema15Cache.clear();
            coinStates.clear();
            await bot.sendMessage(
                chatId,
                INCLUDE_FAST_TFS
                    ? '⚡ *Fast TF Add-on Enabled*\nDual EMA mode will monitor 1m + 3m + 5m + 15m together.'
                    : '⚡ *Fast TF Add-on Disabled*\nDual EMA mode will monitor 5m + 15m only.',
                { parse_mode: 'Markdown' }
            );
            await sendSettingsMenu(chatId);
            refreshWebSockets(chatId);
        } else if (action.startsWith('volume_')) {
            const newVolume = parseInt(action.replace('volume_', ''), 10);
            if (!VALID_VOLUMES.includes(newVolume)) {
                await bot.answerCallbackQuery(callbackQuery.id, { text: '⛔ Invalid volume threshold.' });
                return;
            }
            VOLUME_THRESHOLD = newVolume;
            log(`Volume threshold updated to ${newVolume}`, 'success');
            saveSettings();
            await sendSettingsMenu(chatId); // show updated menu immediately
            refreshWebSockets(chatId);      // reconnect in background — re-fetches pairs with new threshold
        } else if (action === 'ml_status') {
            await sendModelPerformance(chatId);
        } else if (action === 'train_models') {
            const guard = beginCommand('/train');
            if (!guard.ok) {
                await bot.sendMessage(chatId, '⚠️ /train is already running or cooling down. Please wait.').catch(() => {});
            } else {
                try {
                    await trainAllModels(chatId);
                } finally {
                    endCommand('/train');
                }
            }
        } else if (action === 'toggle_ml') {
            ML_ENABLED = !ML_ENABLED;
            saveSettings();
            await bot.sendMessage(
                chatId,
                `🧠 Machine Learning is now ${ML_ENABLED ? 'enabled' : 'disabled'}`
            );
            await sendSettingsMenu(chatId);
        }

        // Answer callback query to remove loading state
        await bot.answerCallbackQuery(callbackQuery.id);
    } catch (error) {
        log(`Error handling callback query: ${error.message}`, 'error');
        await bot.answerCallbackQuery(callbackQuery.id, { text: 'An error occurred' });
    }
}

// Add this function to save settings to a file
function saveSettings() {
    try {
        const settings = {
            EMA_PERIOD,
            TIMEFRAME,
            VOLUME_THRESHOLD,
            CHECK_INTERVAL,
            ALERT_COOLDOWN,
            ML_ENABLED,
            DUAL_EMA_MODE,
            INCLUDE_FAST_TFS
        };

        fs.writeFile(
            path.join(__dirname, 'settings.json'),
            JSON.stringify(settings, null, 2),
            (err) => { if (err) log(`Error writing settings file: ${err.message}`, 'error'); }
        );
        log('Settings saved to file', 'success');
    } catch (error) {
        log(`Error saving settings: ${error.message}`, 'error');
    }
}

// Add this function to load settings from file
function loadSettings() {
    try {
        const settingsPath = path.join(__dirname, 'settings.json');
        if (fs.existsSync(settingsPath)) {
            const parsed = JSON.parse(fs.readFileSync(settingsPath, 'utf8'));
            if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
                throw new Error('Invalid settings format: expected a JSON object');
            }

            const allowedKeys = new Set([
                'EMA_PERIOD',
                'TIMEFRAME',
                'VOLUME_THRESHOLD',
                'CHECK_INTERVAL',
                'ALERT_COOLDOWN',
                'ML_ENABLED',
                'DUAL_EMA_MODE',
                'INCLUDE_FAST_TFS'
            ]);

            for (const key of Object.keys(parsed)) {
                if (!allowedKeys.has(key)) {
                    throw new Error(`Unknown settings key: ${key}`);
                }
            }

            // Clone to a plain data object so inherited/prototype properties are ignored.
            const settings = Object.assign(Object.create(null), parsed);

            // Update variables with saved settings (whitelist-validated to prevent corrupted settings file)
            if (VALID_EMA_PERIODS.includes(settings.EMA_PERIOD)) EMA_PERIOD = settings.EMA_PERIOD;
            if (VALID_TIMEFRAMES.includes(settings.TIMEFRAME)) TIMEFRAME = settings.TIMEFRAME;
            if (VALID_VOLUMES.includes(settings.VOLUME_THRESHOLD)) VOLUME_THRESHOLD = settings.VOLUME_THRESHOLD;
            ML_ENABLED = settings.ML_ENABLED !== undefined ? settings.ML_ENABLED : ML_ENABLED;
            DUAL_EMA_MODE = settings.DUAL_EMA_MODE !== undefined ? settings.DUAL_EMA_MODE : DUAL_EMA_MODE;
            if (typeof settings.INCLUDE_FAST_TFS === 'boolean') INCLUDE_FAST_TFS = settings.INCLUDE_FAST_TFS;

            log('Settings loaded from file', 'success');
        }
    } catch (error) {
        log(`Error loading settings: ${error.message}`, 'error');
    }
}

// Send main menu with ML options
async function sendMainMenu(chatId) {
    const keyboard = {
        inline_keyboard: [
            [{ text: '📊 Status', callback_data: 'status' }],
            [{ text: '⚙️ Settings', callback_data: 'settings' }],
            [
                { text: '📈 Top Gainers', callback_data: 'top_gainers' },
                { text: '📉 Top Losers', callback_data: 'top_losers' }
            ],
            [{ text: '💰 Highest Volume', callback_data: 'top_volume' }],
            [{ text: '🔄 Refresh WebSockets', callback_data: 'refresh_ws' }],
            [
                { text: '🧠 ML Status', callback_data: 'ml_status' },
                { text: '🔬 Train Models', callback_data: 'train_models' }
            ],
            [
                { text: '📊 Export CSV', callback_data: 'export_csv' },
                { text: '❓ Help', callback_data: 'help' }
            ]
        ]
    };

    await bot.sendMessage(chatId, '*EMA Tracker Bot Menu*\nSelect an option:', {
        parse_mode: 'Markdown',
        reply_markup: keyboard
    });
}

// Send status update
async function sendStatusUpdate(chatId) {
    try {
        const pairs = await getFuturesPairs();
        const activeWsCount = wsPool.filter(e => e && e.ws && e.ws.readyState === WebSocket.OPEN).length;

        const message = `*EMA Tracker Status*\n\n` +
            `*Active Configuration:*\n` +
            `- EMA Mode: ${DUAL_EMA_MODE ? `EMA 9/15 Crossover [${getDualEmaTimeframes().join(' + ')}]` : 'Price vs EMA(' + EMA_PERIOD + ')'}\n` +
            `- Timeframe: ${DUAL_EMA_MODE ? getDualEmaTimeframes().join(' + ') : TIMEFRAME}\n` +
            `- Volume Threshold: ${VOLUME_THRESHOLD.toLocaleString()}\n` +
            `- Monitoring: ${pairs.length} pairs\n` +
            `- Active WebSockets: ${activeWsCount}/${pairs.length}\n` +
            `- Machine Learning: ${ML_ENABLED ? 'Enabled ✅' : 'Disabled ❌'}\n` +
            `- Last Check: ${new Date().toLocaleString()}\n\n` +
            `Bot is actively monitoring for ${DUAL_EMA_MODE ? `EMA 9/15 [${getDualEmaTimeframes().join(' + ')}]` : 'EMA'} crossovers in real-time.`;

        await bot.sendMessage(chatId, message, {
            parse_mode: 'Markdown',
            reply_markup: {
                inline_keyboard: [
                    [{ text: '🔄 Refresh WebSockets', callback_data: 'refresh_ws' }],
                    [{ text: '🔙 Back to Menu', callback_data: 'menu' }]
                ]
            }
        });
    } catch (error) {
        log(`Error sending status update: ${error.message}`, 'error');
        await bot.sendMessage(chatId, '❌ Error fetching status');
    }
}

// Send settings menu with ML toggle and dual EMA option
async function sendSettingsMenu(chatId) {
    // Build the EMA mode display string
    const emaModeText = DUAL_EMA_MODE ? 'EMA 9/15 Cross' : `EMA ${EMA_PERIOD}`;

    const keyboard = {
        inline_keyboard: [
            [
                { text: '1m', callback_data: 'timeframe_1m' },
                { text: '5m', callback_data: 'timeframe_5m' },
                { text: '15m', callback_data: 'timeframe_15m' },
                { text: '1h', callback_data: 'timeframe_1h' },
                { text: '4h', callback_data: 'timeframe_4h' }
            ],
            [
                { text: 'EMA 50', callback_data: 'ema_50' },
                { text: 'EMA 100', callback_data: 'ema_100' },
                { text: 'EMA 200', callback_data: 'ema_200' }
            ],
            [
                { text: `EMA 9/15 Cross: ${DUAL_EMA_MODE ? 'Enabled ✅' : 'Disabled ❌'}`, callback_data: 'toggle_dual_ema' }
            ],
            [
                {
                    text: FORCE_ALL_DUAL_TFS
                        ? 'Fast TF (1m/3m): Locked ON 🔒'
                        : `Fast TF (1m/3m): ${INCLUDE_FAST_TFS ? 'Enabled ✅' : 'Disabled ❌'}`,
                    callback_data: 'toggle_fast_tfs'
                }
            ],
            [
                { text: 'Vol 2M', callback_data: 'volume_2000000' },
                { text: 'Vol 5M', callback_data: 'volume_5000000' }
            ],
            [
                { text: 'Vol 10M', callback_data: 'volume_10000000' },
                { text: 'Vol 20M', callback_data: 'volume_20000000' }
            ],
            [
                { text: 'Vol 50M', callback_data: 'volume_50000000' },
                { text: 'Vol 100M', callback_data: 'volume_100000000' }
            ],
            [
                { text: 'Vol 200M', callback_data: 'volume_200000000' }
            ],
            [
                { text: `ML: ${ML_ENABLED ? 'Enabled ✅' : 'Disabled ❌'}`, callback_data: 'toggle_ml' }
            ],
            [{ text: '🔙 Back to Menu', callback_data: 'menu' }]
        ]
    };

    const configText = DUAL_EMA_MODE
        ? `*Settings*\n\nCurrent Configuration:\n- EMA Mode: 9/15 Crossover ✅\n- Timeframe: ${getDualEmaTimeframes().join(' + ')} (all monitored)\n- Fast TF Add-on: ${FORCE_ALL_DUAL_TFS ? 'Locked ON 🔒 (FORCE_ALL_DUAL_TFS)' : (INCLUDE_FAST_TFS ? 'Enabled ✅' : 'Disabled ❌')}\n- Volume Threshold: ${formatVolume(VOLUME_THRESHOLD)}\n- Machine Learning: ${ML_ENABLED ? 'Enabled ✅' : 'Disabled ❌'}\n\n_EMA 9/15 monitors all active dual timeframes simultaneously. Single EMA settings (50/100/200) are ignored._\n\nSelect a new setting:`
        : `*Settings*\n\nCurrent Configuration:\n- EMA: ${EMA_PERIOD}\n- Timeframe: ${TIMEFRAME}\n- Volume Threshold: ${formatVolume(VOLUME_THRESHOLD)}\n- Machine Learning: ${ML_ENABLED ? 'Enabled ✅' : 'Disabled ❌'}\n\nSelect a new setting:`;

    await bot.sendMessage(chatId, configText, {
        parse_mode: 'Markdown',
        reply_markup: keyboard
    });
}

// Send help message
async function sendHelpMessage(chatId) {
    const helpText = `*EMA Tracker Bot Help*\n\n` +
        `This bot monitors Delta Exchange Futures markets for EMA crossovers and sends alerts when they occur.\n\n` +
        `*Available Commands:*\n` +
        `/menu - Show the main menu\n` +
        `/status - Check bot status\n` +
        `/settings - Configure bot settings\n` +
        `/top - View top performing coins\n` +
        `/refresh - Refresh WebSocket connections\n` +
        `/mlstatus - Check ML model performance\n` +
        `/train - Train ML models manually\n` +
        `/collectdata - Manually collect training data\n` +
        `/exportcsv - Export data to CSV format\n` +
        `/help - Show this help message\n\n` +
        `*How It Works:*\n` +
        `The bot uses WebSockets to track price movements in real-time.\n\n` +
        `*Price vs EMA Mode:* Detects when price crosses above or below a single EMA (50/100/200) on the ${TIMEFRAME} timeframe.\n\n` +
        `*EMA 9/15 Cross Mode:* Detects when EMA(9) crosses above or below EMA(15) — a short-term momentum strategy. Toggle it in Settings.\n\n` +
        `*Machine Learning:*\n` +
        `When enabled, ML models predict future price movements after crossovers to enhance signal quality.`;

    await bot.sendMessage(chatId, helpText, {
        parse_mode: 'Markdown',
        reply_markup: {
            inline_keyboard: [[{ text: '🔙 Back to Menu', callback_data: 'menu' }]]
        }
    });
}

// Send top performers (gainers, losers, or by volume)
async function sendTopPerformers(chatId, type = 'gainers') {
    try {
        await bot.sendMessage(chatId, '⏳ Fetching data...');

        await enforceRateLimit();
        const response = await axios.get(`${DELTA_REST_BASE_URL}/tickers`, {
            params: { contract_types: 'perpetual_futures' },
            timeout: 10000
        });
        const apiCoins = response?.data?.result;
        if (!Array.isArray(apiCoins)) {
            throw new Error('Unexpected Delta tickers response shape');
        }

        let coins = apiCoins.filter(coin => coin.symbol && coin.contract_type === 'perpetual_futures');

        // Sort based on type
        if (type === 'gainers') {
            coins.sort((a, b) => parseFloat(b.ltp_change_24h || 0) - parseFloat(a.ltp_change_24h || 0));
            coins = coins.slice(0, 10); // Top 10 gainers
        } else if (type === 'losers') {
            coins.sort((a, b) => parseFloat(a.ltp_change_24h || 0) - parseFloat(b.ltp_change_24h || 0));
            coins = coins.slice(0, 10); // Top 10 losers
        } else if (type === 'volume') {
            coins.sort((a, b) => parseFloat(b.turnover_usd || b.turnover || 0) - parseFloat(a.turnover_usd || a.turnover || 0));
            coins = coins.slice(0, 10); // Top 10 by volume
        }

        if (coins.length === 0) {
            await bot.sendMessage(chatId, '⚠️ No ticker data available for top performers right now.');
            return;
        }

        let title;
        if (type === 'gainers') title = '📈 *Top Gainers (24h)*';
        else if (type === 'losers') title = '📉 *Top Losers (24h)*';
        else title = '💰 *Highest Volume (24h)*';

        let message = `${title}\n\n`;

        coins.forEach((coin, index) => {
            const symbol = coin.symbol;
            const price = formatPrice(parseFloat(coin.close || 0));
            const change = parseFloat(coin.ltp_change_24h || 0).toFixed(2);
            const volume = formatVolume(parseFloat(coin.turnover_usd || coin.turnover || 0));

            const changeEmoji = parseFloat(change) >= 0 ? '🟢' : '🔴';
            message += `${index + 1}. ${symbol}: ${price} (${changeEmoji} ${change}%) - Vol: ${volume}\n`;
        });

        await bot.sendMessage(chatId, message, {
            parse_mode: 'Markdown',
            reply_markup: {
                inline_keyboard: [
                    [
                        { text: '📈 Gainers', callback_data: 'top_gainers' },
                        { text: '📉 Losers', callback_data: 'top_losers' },
                        { text: '💰 Volume', callback_data: 'top_volume' }
                    ],
                    [{ text: '🔙 Back to Menu', callback_data: 'menu' }]
                ]
            }
        });
    } catch (error) {
        log(`Error fetching top performers: ${error.message}`, 'error');
        await bot.sendMessage(chatId, '❌ Error fetching data');
    }
}

// Add a command to check model performance
async function sendModelPerformance(chatId) {
    if (mlRuntime) await mlRuntime.sendModelPerformance(chatId);
}

// Send initial startup message to Telegram
async function sendStartupMessage() {
    try {
        const message = `🤖 *EMA Tracker Bot Started* 🤖\n\n` +
            `*Configuration:*\n` +
            `- EMA Mode: ${DUAL_EMA_MODE ? `EMA 9/15 Crossover [${getDualEmaTimeframes().join(' + ')}]` : 'Price vs EMA(' + EMA_PERIOD + ')'}\n` +
            `- Timeframe: ${DUAL_EMA_MODE ? `${getDualEmaTimeframes().join(' + ')} (all monitored)` : TIMEFRAME}\n` +
            `- Volume Threshold: ${VOLUME_THRESHOLD.toLocaleString()}\n` +
            `- Check Interval: ${(CHECK_INTERVAL / 60000).toFixed(1)} minutes\n` +
            `- Alert Cooldown: ${(ALERT_COOLDOWN / 60000).toFixed(1)} minutes\n` +
            `- WebSocket Monitoring: Enabled\n` +
            `- ML Enhancement: ${ML_ENABLED ? 'Enabled' : 'Disabled'}\n\n` +
            `Bot is now monitoring for ${DUAL_EMA_MODE ? `EMA 9/15 crossovers on ${getDualEmaTimeframes().join(', ')}` : 'EMA crossovers'} in real-time${ML_ENABLED ? ' with ML predictions' : ''}...`;

        await bot.sendMessage(TELEGRAM_CHAT_ID, message, { parse_mode: 'Markdown' });
        log('Startup message sent to Telegram', 'success');

        // Show desktop notification — mirrors startup message content
        showDesktopNotification(
            '🤖 EMA Tracker Started',
            `Mode: ${DUAL_EMA_MODE ? `EMA 9/15 [${getDualEmaTimeframes().join('+')}]` : 'Price vs EMA(' + EMA_PERIOD + ')'}\nTimeframe: ${DUAL_EMA_MODE ? getDualEmaTimeframes().join(' + ') : TIMEFRAME}  Vol: ${formatVolume(VOLUME_THRESHOLD)}\nML: ${ML_ENABLED ? 'Enabled' : 'Disabled'}`,
            'info'
        );

        // Send the menu after startup message
        await sendMainMenu(TELEGRAM_CHAT_ID);
    } catch (error) {
        log(`Error sending startup message: ${error.message}`, 'error');
    }
}

// WebSocket heartbeat function — operates on the pool instead of per-symbol connections
function startWebSocketHeartbeat() {
    // Check pool connections every minute
    setInterval(() => {
        if (isReconnecting) return;
        try {
            let reconnected = 0;
            for (let i = 0; i < wsPool.length; i++) {
                const entry = wsPool[i];
                if (!entry || !entry.ws) continue;
                if (entry.ws.readyState === WebSocket.CLOSED || entry.ws.readyState === WebSocket.CLOSING) {
                    fs.appendFileSync(
                        getDailyLogPath(),
                        `[${new Date().toISOString()}] Pool WS #${i} is dead. Reconnecting ${entry.symbols.size} symbols...\n`
                    );
                    const live = Array.from(entry.symbols).filter(s => trackedPairs.has(s));
                    if (live.length > 0) {
                        reconnectionAttempts.set(`pool_${i}`, 0);
                        backfillAndReconnect(i, live);
                        reconnected++;
                    }
                }
            }
            if (reconnected > 0) {
                fs.appendFileSync(
                    getDailyLogPath(),
                    `[${new Date().toISOString()}] Reconnected ${reconnected} pool connection(s) during heartbeat\n`
                );
            }
        } catch (error) {
            fs.appendFileSync(
                getDailyLogPath(),
                `[${new Date().toISOString()}] Error in WebSocket heartbeat: ${error.message}\n`
            );
        }
    }, 60000);

    // TCP-level ping every 3 minutes
    setInterval(() => {
        for (const entry of wsPool) {
            if (entry && entry.ws && entry.ws.readyState === WebSocket.OPEN) {
                entry.ws.ping();
            }
        }
    }, 3 * 60 * 1000);

    // Public WS heartbeat ping
    setInterval(() => {
        for (const entry of wsPool) {
            if (entry && entry.ws && entry.ws.readyState === WebSocket.OPEN) {
                entry.ws.send(JSON.stringify({ type: 'ping' }));
            }
        }
    }, 20 * 1000);
}

// Schedule periodic model training
function scheduleModelTraining() {
    if (mlRuntime) mlRuntime.scheduleModelTraining(() => ML_ENABLED);
}

// Set up message and callback query handlers
bot.on('message', handleMessage);
bot.on('callback_query', handleCallbackQuery);

// Handle process termination gracefully
async function gracefulShutdown(signal) {
    try {
        log(`Received ${signal}. Shutting down gracefully...`, 'warning');
        // Prevent pool close handlers from scheduling reconnects during shutdown
        isReconnecting = true;
        // Close all pool WebSocket connections
        for (const entry of wsPool) {
            if (entry && entry.ws) {
                try {
                    entry.ws.close();
                    log(`Closed pool WS #${entry.index} (${entry.symbols.size} symbols)`, 'info');
                } catch (e) {
                    // Ignore errors when closing
                }
            }
        }
        wsPool.length = 0;

        // Flush alert state so cooldowns and coin states survive the restart
        saveAlertState();

        await Promise.race([
            bot.sendMessage(TELEGRAM_CHAT_ID, '⚠️ EMA Tracker Bot is shutting down...'),
            new Promise((_, reject) => setTimeout(() => reject(new Error('shutdown message timeout')), 3000))
        ]).catch(() => {});

        // Show desktop notification
        showDesktopNotification(
            'EMA Tracker Shutting Down',
            'The bot is shutting down gracefully',
            'warning'
        );

        process.exit(0);
    } catch (error) {
        log(`Error during shutdown: ${error.message}`, 'error');
        process.exit(1);
    }
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// Error handling for unhandled rejections
process.on('unhandledRejection', (reason, promise) => {
    log(`Unhandled Rejection at: ${promise}, reason: ${reason}`, 'error');

    // Show desktop notification for unhandled rejection
    showDesktopNotification(
        'Error in EMA Tracker',
        'An unhandled rejection occurred. Check logs for details.',
        'error'
    );
});


// // installRequiredPackages — COMMENTED OUT: not needed, packages are installed via npm
// async function installRequiredPackages() { ... }

// Calculate ATR (Average True Range) using Wilder's smoothing
function calculateATR(klines, period = 14) {
    if (!mlRuntime) return 0;
    return mlRuntime.calculateATR(klines, period);
}

// Remove log files older than 7 days to prevent unbounded disk growth
function rotateLogs() {
    try {
        const sevenDaysAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;
        if (!fs.existsSync(LOG_DIR)) return;
        const files = fs.readdirSync(LOG_DIR).filter(f => f.endsWith('.log'));
        for (const file of files) {
            const filePath = path.join(LOG_DIR, file);
            const stats = fs.statSync(filePath);
            if (stats.mtimeMs < sevenDaysAgo) {
                fs.unlinkSync(filePath);
                log(`Rotated old log file: ${file}`, 'info');
            }
        }
    } catch (error) {
        log(`Error rotating logs: ${error.message}`, 'error');
    }
}

// Tiny HTTP health endpoint for external monitoring (UptimeRobot, cron jobs, etc.).
// Returns 200 + JSON status blob on GET /health.
// Configure port via HEALTH_PORT env var (default: 3000).
function startHealthServer() {
    const port = parseInt(process.env.HEALTH_PORT, 10) || 3000;
    const server = http.createServer((req, res) => {
        if (req.url !== '/health') { res.writeHead(404); res.end(); return; }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            status: 'ok',
            uptime: Math.floor(process.uptime()),
            activePoolConnections: wsPool.filter(e => e && e.ws && e.ws.readyState === WebSocket.OPEN).length,
            totalPoolConnections: wsPool.length,
            trackedPairs: trackedPairs.size,
            lastCandleTime,
            initialLoadComplete
        }));
    });
    server.listen(port, () => log(`Health endpoint listening on port ${port}`, 'info'));
    server.on('error', e => log(`Health server error: ${e.message}`, 'warning'));
}

// Initialize the terminal and start monitoring
async function initialize() {
    try {
        // Initialize terminal and load settings
        loadSettings();
        initializeTerminal();
        startHealthServer();
        loadAlertState(); // restore last-alert timestamps so restarts don't re-fire crossovers
        rotateLogs();

        // Register toast app so click-to-open works on Windows
        registerToastApp();

        console.log('\nStarting initial check...'.green);

        // Initialize ML components
        console.log('Initializing machine learning components...'.cyan);

        mlRuntime = createMLRuntime({
            log,
            bot,
            telegramChatId: TELEGRAM_CHAT_ID,
            mlDataDir: ML_DATA_DIR,
            csvDataDir: CSV_DATA_DIR,
            modelPath: MODEL_PATH,
            deltaRestBaseUrl: DELTA_REST_BASE_URL,
            enforceRateLimit,
            getFuturesPairs,
            getKlines,
            processClosedCandle,
            getDualMode: () => DUAL_EMA_MODE,
            getDualEmaTimeframes,
            tfKey,
            klineCache
        });

        // Create models directory if it doesn't exist
        const modelsDir = path.join(__dirname, 'models');
        if (!fs.existsSync(modelsDir)) {
            fs.mkdirSync(modelsDir);
        }

        // Create ML data directory if it doesn't exist
        if (!fs.existsSync(ML_DATA_DIR)) {
            fs.mkdirSync(ML_DATA_DIR, { recursive: true });
        }

        if (ML_ENABLED) {
            // Check TensorFlow only when ML is enabled in settings
            ML_ENABLED = checkTensorFlowAvailability();
            if (ML_ENABLED) {
                loadTrainingData();
            }
        } else {
            log('ML is disabled in settings; skipping TensorFlow availability check', 'info');
        }

        // Send startup message
        await sendStartupMessage();

        // Do initial check to populate data
        await checkEMACross({ emitAlerts: false });

        // Mark initial load complete before WebSocket/interval registration so
        // periodic tasks never run with stale startup state.
        initialLoadComplete = true;
        log('Initial load complete. Volume threshold notifications enabled for new pairs.', 'info');

        // Setup WebSockets for all tracked pairs
        await setupAllWebSockets();

        // Start WebSocket heartbeat
        startWebSocketHeartbeat();

        // Periodically reset reconnection counters for maxed-out pool connections
        setInterval(() => {
            for (let i = 0; i < wsPool.length; i++) {
                const poolKey = `pool_${i}`;
                const attempts = reconnectionAttempts.get(poolKey) || 0;
                const entry = wsPool[i];
                if (attempts >= MAX_RECONNECTION_ATTEMPTS && entry) {
                    const live = Array.from(entry.symbols).filter(s => trackedPairs.has(s));
                    if (live.length > 0 && entry.ws.readyState !== WebSocket.OPEN) {
                        log(`Resetting reconnection counter for pool WS #${i} and re-attempting (${live.length} symbols)...`, 'info');
                        reconnectionAttempts.set(poolKey, 0);
                        wsPool[i] = setupPoolConnection(i, live);
                    }
                }
            }
        }, 5 * 60 * 1000); // every 5 minutes

        // Schedule model training if ML is enabled
        if (ML_ENABLED) {
            scheduleModelTraining();
        }

        // Schedule periodic saving of training data
        setInterval(saveTrainingData, 30 * 60 * 1000); // Save every 30 minutes

        // Process deferred updates (replaces unbounded 24h setTimeout timers)
        setInterval(async () => {
            const now = Date.now();
            while (deferredUpdates.length > 0 && deferredUpdates[0].executeAt <= now) {
                const task = deferredUpdates.shift();
                try { await task.fn(); } catch (e) { log(`Deferred update error: ${e.message}`, 'error'); }
            }
        }, 60 * 1000); // check every minute

        // Run the check at the specified interval as a backup
        // This is in addition to the real-time WebSocket monitoring
        monitoringInterval = setInterval(async () => {
            log('Running periodic check as backup to WebSockets...', 'info');
            await checkEMACross({ emitAlerts: false });
        }, CHECK_INTERVAL);

        log(`Initialization complete. Bot is now monitoring in real-time via WebSockets${ML_ENABLED ? ' with ML enhancement' : ''}.`, 'success');
    } catch (error) {
        log(`Failed to initialize: ${error.message}`, 'error');

        // Show desktop notification for startup failure
        showDesktopNotification(
            'EMA Tracker Failed to Start',
            `Error: ${error.message}`,
            'error'
        );

        // Try to send error message to Telegram
        try {
            await bot.sendMessage(TELEGRAM_CHAT_ID, `❌ *Error Starting Bot*\n\n${error.message}`, {
                parse_mode: 'Markdown'
            });
        } catch (e) {
            log(`Could not send error message to Telegram: ${e.message}`, 'error');
        }
    }
}

// Start the bot
initialize();