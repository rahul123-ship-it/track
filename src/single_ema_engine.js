/**
 * Single-EMA Crossover Engine
 * ============================
 * Handles price vs EMA(50/100/200) crossover detection and Telegram alerting.
 *
 * STATUS: DISABLED — Bot is currently running EMA 9/15 dual-crossover mode.
 *
 * TO RE-ENABLE:
 *   1. In main.js, import this module and pass deps:
 *        const singleEmaEngine = require('./src/single_ema_engine');
 *        const singleEma = singleEmaEngine.createSingleEmaEngine({ ... });
 *
 *   2. In main.js processClosedCandle(), uncomment the else-branch that calls
 *        singleEma.checkForCrossover(...)
 *
 *   3. In main.js checkEMACross(), uncomment the single-EMA branch.
 *
 *   4. In main.js sendSettingsMenu(), uncomment the EMA 50/100/200 buttons.
 *
 *   5. In main.js handleCallbackQuery(), uncomment the ema_ action handler.
 *
 * Dependencies injected via createSingleEmaEngine(deps):
 *   - bot               — TelegramBot instance
 *   - TELEGRAM_CHAT_ID  — string
 *   - getEMAPeriod      — () => number (getter for mutable EMA_PERIOD)
 *   - getTimeframe      — () => string (getter for mutable TIMEFRAME)
 *   - isMLEnabled       — () => boolean
 *   - MIN_CROSS_PCT     — number constant
 *   - coinStates        — Map<string, string>
 *   - formatPrice       — (n) => string
 *   - formatVolume      — (n) => string
 *   - getChartUrl       — (symbol, tf?) => string
 *   - getHtmlSafeUrl    — (url) => string
 *   - get24HrStats      — async (symbol) => { priceChangePercent, quoteVolume }
 *   - getOIDelta        — async (symbol) => oiObject | null
 *   - safeSendAlert     — async (chatId, text, opts?) => void
 *   - showDesktopNotification — (title, body, type, url?) => void
 *   - shouldAlert       — (symbol, state, tf?) => boolean
 *   - isEmaFlat         — (symbol, tf, useDualEma) => { isFlat, reason }
 *   - traceAlert        — (msg) => void
 *   - activeTimeframeLabel — (tf?) => string
 *   - predictPriceMovement — async (symbol, price, ema, diff) => number | null
 *   - log               — (msg, level?) => void
 */

'use strict';

/**
 * Factory — wire up and return the single-EMA engine.
 * @param {object} deps — see module JSDoc above
 * @returns {{ checkForCrossover, sendTelegramAlert, sendTelegramAlertWithML }}
 */
function createSingleEmaEngine(deps) {
    const {
        bot,
        TELEGRAM_CHAT_ID,
        getEMAPeriod,
        getTimeframe,
        isMLEnabled,
        MIN_CROSS_PCT,
        coinStates,
        formatPrice,
        formatVolume,
        getChartUrl,
        getHtmlSafeUrl,
        get24HrStats,
        getOIDelta,
        safeSendAlert,
        showDesktopNotification,
        shouldAlert,
        isEmaFlat,
        traceAlert,
        activeTimeframeLabel,
        predictPriceMovement,
        log,
    } = deps;

    // -------------------------------------------------------------------------
    // sendTelegramAlert — single-EMA (price vs EMA) alert
    // -------------------------------------------------------------------------
    async function sendTelegramAlert(symbol, crossType, price, ema, difference) {
        const EMA_PERIOD = getEMAPeriod();
        const TIMEFRAME  = getTimeframe();
        try {
            const emoji          = crossType === 'up' ? '🟢' : '🔴';
            const signal         = crossType === 'up' ? 'BULLISH SIGNAL' : 'BEARISH SIGNAL';
            const formattedPrice = formatPrice(price);
            const formattedEma   = formatPrice(ema);

            const stats   = await get24HrStats(symbol);
            const oi      = await getOIDelta(symbol).catch(() => null);
            const chartUrl = getChartUrl(symbol);

            const oiLine = oi
                ? `<b>OI Delta:</b> ${oi.deltaPercent >= 0 ? '+' : ''}${oi.deltaPercent.toFixed(2)}% ${
                      oi.deltaPercent >= 0.5  ? '\u{1F4C8} new money (stronger)' :
                      oi.deltaPercent <= -0.5 ? '\u{1F4C9} liquidation (weaker)' :
                                               '\u2192 neutral'}\n`
                : '';

            const message =
                `${emoji} <b>${signal}</b> ${emoji}\n\n` +
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
                disable_web_page_preview: true,
            });

            showDesktopNotification(
                `${crossType === 'up' ? '🟢 BULLISH' : '🔴 BEARISH'} — ${symbol}`,
                `Price: ${formattedPrice}  EMA(${EMA_PERIOD}): ${formattedEma}\n` +
                `Diff: ${difference.toFixed(2)}%  24h: ${stats.priceChangePercent}%\n` +
                `Vol: ${formatVolume(stats.quoteVolume)}  TF: ${TIMEFRAME}`,
                crossType === 'up' ? 'info' : 'warning',
                chartUrl
            );

            log(`Telegram alert sent for ${symbol} (${crossType})`, 'success');
        } catch (error) {
            log(`Error sending Telegram message: ${error.message}`, 'error');
            try {
                await safeSendAlert(
                    TELEGRAM_CHAT_ID,
                    `${crossType === 'up' ? '🟢 BULLISH' : '🔴 BEARISH'} SIGNAL: ${symbol} at ${formatPrice(price)}`
                );
                log(`Sent simplified alert for ${symbol} after error`, 'warning');
            } catch (retryError) {
                log(`Failed to send even simplified message: ${retryError.message}`, 'error');
            }
        }
    }

    // -------------------------------------------------------------------------
    // sendTelegramAlertWithML — single-EMA alert enhanced with ML prediction
    // -------------------------------------------------------------------------
    async function sendTelegramAlertWithML(symbol, crossType, price, ema, difference, prediction) {
        const EMA_PERIOD = getEMAPeriod();
        const TIMEFRAME  = getTimeframe();
        try {
            const emoji          = crossType === 'up' ? '🟢' : '🔴';
            const signal         = crossType === 'up' ? 'BULLISH SIGNAL' : 'BEARISH SIGNAL';
            const formattedPrice = formatPrice(price);
            const formattedEma   = formatPrice(ema);

            const stats   = await get24HrStats(symbol);
            const oi      = await getOIDelta(symbol).catch(() => null);
            const chartUrl = getChartUrl(symbol);

            let confidenceEmoji = '⚠️';
            if (Math.abs(prediction) > 3)      confidenceEmoji = prediction > 0 ? '🔥' : '❄️';
            else if (Math.abs(prediction) > 1)  confidenceEmoji = prediction > 0 ? '📈' : '📉';

            const oiLine = oi
                ? `<b>OI Delta:</b> ${oi.deltaPercent >= 0 ? '+' : ''}${oi.deltaPercent.toFixed(2)}% ${
                      oi.deltaPercent >= 0.5  ? '📈 new money (stronger)' :
                      oi.deltaPercent <= -0.5 ? '📉 liquidation (weaker)' :
                                               '→ neutral'}\n`
                : '';

            const message =
                `${emoji} <b>${signal}</b> ${emoji}\n\n` +
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
                disable_web_page_preview: true,
            });

            showDesktopNotification(
                `${crossType === 'up' ? '🟢 BULLISH+ML' : '🔴 BEARISH+ML'} — ${symbol}`,
                `Price: ${formattedPrice}  EMA(${EMA_PERIOD}): ${formattedEma}\n` +
                `Diff: ${difference.toFixed(2)}%  ML: ${confidenceEmoji} ${prediction.toFixed(2)}% (24h)\n` +
                `24h: ${stats.priceChangePercent}%  TF: ${TIMEFRAME}`,
                crossType === 'up' ? 'info' : 'warning',
                chartUrl
            );

            log(`ML-enhanced Telegram alert sent for ${symbol} (${crossType})`, 'success');
        } catch (error) {
            log(`Error sending ML-enhanced Telegram message: ${error.message}`, 'error');
            await sendTelegramAlert(symbol, crossType, price, ema, difference)
                .catch(e => log(`Fallback alert failed for ${symbol}: ${e.message}`, 'error'));
        }
    }

    // -------------------------------------------------------------------------
    // checkForCrossover — price vs single EMA(50/100/200) crossover detection
    // -------------------------------------------------------------------------
    async function checkForCrossover(symbol, prevPrice, lastPrice, prevEMA, lastEMA) {
        const EMA_PERIOD = getEMAPeriod();
        try {
            if (![prevPrice, lastPrice, prevEMA, lastEMA].every(Number.isFinite) || prevEMA === 0 || lastEMA === 0) {
                traceAlert(`${symbol} single-mode skipped: invalid numeric inputs`);
                return;
            }

            const currentState = lastPrice > lastEMA ? 'above' : 'below';
            const difference   = (lastPrice - lastEMA) / lastEMA * 100;

            let prediction = null;
            if (isMLEnabled()) {
                try {
                    prediction = await predictPriceMovement(symbol, lastPrice, lastEMA, difference);
                } catch (predictionError) {
                    log(`Error getting prediction for ${symbol}: ${predictionError.message}`, 'warning');
                }
            }

            // Upward crossover
            if (prevPrice < prevEMA && lastPrice > lastEMA && (lastPrice - lastEMA) / lastEMA > MIN_CROSS_PCT) {
                traceAlert(`${symbol} single-mode bullish candidate diff=${difference.toFixed(4)}%`);
                console.log('▲'.green + ' UPWARD CROSSOVER '.white.bgGreen + ' ' + symbol.bold);

                const flatCheck = isEmaFlat(symbol, '', false);
                if (flatCheck.isFlat) {
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
            // Downward crossover
            else if (prevPrice > prevEMA && lastPrice < lastEMA && (lastEMA - lastPrice) / lastEMA > MIN_CROSS_PCT) {
                traceAlert(`${symbol} single-mode bearish candidate diff=${difference.toFixed(4)}%`);
                console.log('▼'.red + ' DOWNWARD CROSSOVER '.white.bgRed + ' ' + symbol.bold);

                const flatCheck = isEmaFlat(symbol, '', false);
                if (flatCheck.isFlat) {
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
                coinStates.set(symbol, currentState);
                traceAlert(`${symbol} single-mode no cross; state=${currentState} diff=${difference.toFixed(4)}%`);
            }
        } catch (error) {
            log(`Error checking for crossover for ${symbol}: ${error.message}`, 'error');
        }
    }

    return { checkForCrossover, sendTelegramAlert, sendTelegramAlertWithML };
}

module.exports = { createSingleEmaEngine };
