const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { createObjectCsvWriter } = require('csv-writer');

function createMLRuntime(options) {
    const {
        log,
        bot,
        telegramChatId,
        mlDataDir,
        csvDataDir,
        modelPath,
        deltaRestBaseUrl,
        enforceRateLimit,
        getFuturesPairs,
        getKlines,
        processClosedCandle,
        getDualMode,
        getDualEmaTimeframes,
        tfKey,
        klineCache
    } = options;

    const trainingData = new Map();
    const modelPerformance = new Map();
    const csvWriterCache = new Map();
    let indicatorsModule = null;

    function ensureDirs() {
        if (!fs.existsSync(mlDataDir)) fs.mkdirSync(mlDataDir, { recursive: true });
        if (!fs.existsSync(csvDataDir)) fs.mkdirSync(csvDataDir, { recursive: true });
        if (!fs.existsSync(modelPath)) fs.mkdirSync(modelPath, { recursive: true });
    }

    function getIndicators() {
        if (!indicatorsModule) indicatorsModule = require('../indicators');
        return indicatorsModule;
    }

    function checkTensorFlowAvailability() {
        try {
            require('@tensorflow/tfjs-node');
            log('TensorFlow.js is available', 'success');
            return true;
        } catch (e) {
            try {
                require('@tensorflow/tfjs-node-cpu');
                log('TensorFlow.js CPU version is available', 'warning');
                return true;
            } catch (e2) {
                log(`TensorFlow.js is not available: ${e2.message}`, 'error');
                log('ML predictions will be disabled', 'warning');
                return false;
            }
        }
    }

    function calculateATR(klines, period = 14) {
        if (klines.length < period + 1) {
            return 0;
        }

        const trueRanges = [];

        for (let i = 1; i < klines.length; i++) {
            const high = klines[i].high;
            const low = klines[i].low;
            const prevClose = klines[i - 1].close;

            const tr1 = high - low;
            const tr2 = Math.abs(high - prevClose);
            const tr3 = Math.abs(low - prevClose);
            trueRanges.push(Math.max(tr1, tr2, tr3));
        }

        let atr = trueRanges.slice(0, period).reduce((sum, tr) => sum + tr, 0) / period;
        for (let i = period; i < trueRanges.length; i++) {
            atr = (atr * (period - 1) + trueRanges[i]) / period;
        }

        return atr;
    }

    async function saveDataPoint(symbol, dataPoint) {
        try {
            const safeSymbol = symbol.replace(/[^A-Z0-9]/g, '');
            const symbolDir = path.join(mlDataDir, safeSymbol);

            const date = new Date();
            const filename = `${date.getFullYear()}-${(date.getMonth() + 1).toString().padStart(2, '0')}.ndjson`;
            const filePath = path.join(symbolDir, filename);

            await fs.promises.mkdir(symbolDir, { recursive: true });
            await fs.promises.appendFile(filePath, JSON.stringify(dataPoint) + '\n');
            return true;
        } catch (error) {
            log(`Error saving data point for ${symbol}: ${error.message}`, 'error');
            return false;
        }
    }

    function getCsvWriter(csvPath) {
        if (!csvWriterCache.has(csvPath)) {
            csvWriterCache.set(csvPath, createObjectCsvWriter({
                path: csvPath,
                header: [
                    { id: 'timestamp', title: 'TIMESTAMP' },
                    { id: 'symbol', title: 'SYMBOL' },
                    { id: 'open', title: 'OPEN' },
                    { id: 'high', title: 'HIGH' },
                    { id: 'low', title: 'LOW' },
                    { id: 'close', title: 'CLOSE' },
                    { id: 'volume', title: 'VOLUME' },
                    { id: 'ema', title: 'EMA' },
                    { id: 'ema_diff', title: 'EMA_DIFF' },
                    { id: 'rsi', title: 'RSI' },
                    { id: 'macd', title: 'MACD' },
                    { id: 'macd_signal', title: 'MACD_SIGNAL' },
                    { id: 'macd_hist', title: 'MACD_HIST' },
                    { id: 'bb_upper', title: 'BB_UPPER' },
                    { id: 'bb_middle', title: 'BB_MIDDLE' },
                    { id: 'bb_lower', title: 'BB_LOWER' },
                    { id: 'bb_width', title: 'BB_WIDTH' },
                    { id: 'atr', title: 'ATR' },
                    { id: 'atr_valid', title: 'ATR_VALID' },
                    { id: 'volume_change', title: 'VOLUME_CHANGE' },
                    { id: 'future_price_change', title: 'FUTURE_PRICE_CHANGE' },
                    { id: 'label', title: 'LABEL' }
                ]
            }));
        }
        return csvWriterCache.get(csvPath);
    }

    function exportToCSV(symbol) {
        try {
            if (!trainingData.has(symbol) || trainingData.get(symbol).length === 0) {
                return;
            }

            const data = trainingData.get(symbol);
            const safeSymbol = symbol.replace(/[^A-Z0-9]/g, '');
            const symbolDir = path.join(csvDataDir, safeSymbol);
            if (!fs.existsSync(symbolDir)) {
                fs.mkdirSync(symbolDir, { recursive: true });
            }

            const csvPath = path.join(symbolDir, `${safeSymbol}_training_data.csv`);
            const csvWriter = getCsvWriter(csvPath);
            csvWriter.writeRecords(data)
                .then(() => {
                    log(`CSV export completed for ${symbol} with ${data.length} records`, 'success');
                })
                .catch(error => {
                    log(`Error writing CSV for ${symbol}: ${error.message}`, 'error');
                });
        } catch (error) {
            log(`Error exporting to CSV for ${symbol}: ${error.message}`, 'error');
        }
    }

    function exportAllDataToCSV() {
        try {
            log('Exporting all training data to CSV...', 'info');
            for (const [symbol, data] of trainingData.entries()) {
                if (data.length > 0) exportToCSV(symbol);
            }
            log('All training data exported to CSV successfully', 'success');
        } catch (error) {
            log(`Error exporting all data to CSV: ${error.message}`, 'error');
        }
    }

    async function getCurrentPrice(symbol) {
        try {
            await enforceRateLimit();
            const response = await axios.get(`${deltaRestBaseUrl}/tickers/${symbol}`, {
                timeout: 10000
            });
            return parseFloat(response?.data?.result?.close || 0);
        } catch (error) {
            log(`Error getting current price for ${symbol}: ${error.message}`, 'error');
            throw error;
        }
    }

    async function updateStoredDataPoint(symbol, timestamp, priceChange) {
        try {
            const safeSymbol = symbol.replace(/[^A-Z0-9]/g, '');
            const symbolDir = path.join(mlDataDir, safeSymbol);
            if (!fs.existsSync(symbolDir)) return false;

            const label = priceChange > 1.0 ? 2 : priceChange < -1.0 ? 0 : 1;
            const labelsPath = path.join(symbolDir, 'labels.ndjson');
            await fs.promises.appendFile(
                labelsPath,
                JSON.stringify({ timestamp, future_price_change: priceChange, label }) + '\n'
            );
            return true;
        } catch (error) {
            log(`Error updating stored data point for ${symbol}: ${error.message}`, 'error');
            return false;
        }
    }

    async function updateFuturePriceChange(symbol, timestamp) {
        try {
            if (!trainingData.has(symbol)) return;

            const data = trainingData.get(symbol);
            const dataPoint = data.find(d => d.timestamp === timestamp);
            if (!dataPoint) return;

            const currentPrice = await getCurrentPrice(symbol);
            const originalPrice = dataPoint.close;
            const priceChange = ((currentPrice - originalPrice) / originalPrice * 100);

            dataPoint.future_price_change = priceChange;
            const LABEL_THRESHOLD = 1.0;
            dataPoint.label = priceChange > LABEL_THRESHOLD ? 2 : priceChange < -LABEL_THRESHOLD ? 0 : 1;

            log(`Updated future price change for ${symbol}: ${priceChange.toFixed(2)}%`, 'info');
            updateStoredDataPoint(symbol, timestamp, priceChange);
            exportToCSV(symbol);
        } catch (error) {
            log(`Error updating future price change: ${error.message}`, 'error');
        }
    }

    async function updateModelAccuracy(symbol, originalPrice, prediction) {
        try {
            const currentPrice = await getCurrentPrice(symbol);
            const actualChange = ((currentPrice - originalPrice) / originalPrice * 100);
            const predictionCorrect = (prediction > 0 && actualChange > 0) || (prediction < 0 && actualChange < 0);

            if (modelPerformance.has(symbol)) {
                const perf = modelPerformance.get(symbol);
                if (predictionCorrect) {
                    perf.correctPredictions++;
                }
                perf.accuracy = perf.correctPredictions / perf.predictions;
                modelPerformance.set(symbol, perf);
                log(`Updated model accuracy for ${symbol}: ${(perf.accuracy * 100).toFixed(2)}% (${perf.correctPredictions}/${perf.predictions})`, 'info');
            }

            saveTrainingData();
        } catch (error) {
            log(`Error updating model accuracy for ${symbol}: ${error.message}`, 'error');
        }
    }

    async function predictPriceMovement(symbol, price, ema, emaDiff, deferredInsert, deferredUpdates) {
        try {
            const mlModel = require('./model');
            const mlCacheKey = getDualMode() ? tfKey(symbol, '15m') : symbol;
            const klines = klineCache.get(mlCacheKey) || [];
            if (klines.length < 30) return null;

            const closes = klines.map(k => k.close);
            const volumes = klines.map(k => k.volume || 0);

            const { calculateRSI, calculateMACD, calculateBollingerBands } = getIndicators();
            const rsi = calculateRSI(closes);
            const macd = calculateMACD(closes);
            const bb = calculateBollingerBands(closes);
            const atr = calculateATR(klines);

            const features = {
                priceDiff: emaDiff,
                volume24h: volumes[volumes.length - 1],
                volumeChange: volumes[volumes.length - 1] / volumes[volumes.length - 2] - 1,
                relativeVolume: volumes[volumes.length - 1] / volumes.slice(-10).reduce((sum, vol) => sum + vol, 0) * 10,
                atr: atr || 0,
                bbWidth: (bb.upper[bb.upper.length - 1] - bb.lower[bb.lower.length - 1]) / bb.middle[bb.middle.length - 1],
                rsi: rsi[rsi.length - 1],
                macdHist: macd.histogram[macd.histogram.length - 1]
            };

            const prediction = await mlModel.predictPriceChange(symbol, features);
            if (prediction !== null) {
                if (!modelPerformance.has(symbol)) {
                    modelPerformance.set(symbol, {
                        predictions: 1,
                        correctPredictions: 0,
                        accuracy: 0,
                        lastTraining: '',
                        dataPoints: 0
                    });
                } else {
                    const perf = modelPerformance.get(symbol);
                    perf.predictions++;
                    modelPerformance.set(symbol, perf);
                }

                deferredInsert({
                    executeAt: Date.now() + 24 * 60 * 60 * 1000,
                    fn: () => updateModelAccuracy(symbol, price, prediction)
                });
                if (deferredUpdates.length > 5000) deferredUpdates.splice(0, deferredUpdates.length - 5000);
            }

            return prediction;
        } catch (error) {
            log(`Error predicting price movement for ${symbol}: ${error.message}`, 'error');
            return null;
        }
    }

    async function onClosedCandleForTraining(symbol, closedKlines, emaValues, deferredInsert, deferredUpdates) {
        if (!closedKlines || closedKlines.length < 31) return;
        if (!emaValues || emaValues.length < 2) return;

        try {
            const closedCloses = closedKlines.map(k => k.close);
            const closedVolumes = closedKlines.map(k => k.volume);
            const lastClosedKline = closedKlines[closedKlines.length - 1];
            const lastClosedEMA = emaValues[emaValues.length - 2];

            const { calculateRSI, calculateMACD, calculateBollingerBands } = getIndicators();
            const rsi = calculateRSI(closedCloses);
            const macd = calculateMACD(closedCloses);
            const bb = calculateBollingerBands(closedCloses);

            const atr = calculateATR(closedKlines);
            const atrValid = closedKlines.length >= 15;

            const dataPoint = {
                timestamp: lastClosedKline.time,
                symbol,
                open: lastClosedKline.open,
                high: lastClosedKline.high,
                low: lastClosedKline.low,
                close: lastClosedKline.close,
                volume: lastClosedKline.volume,
                ema: lastClosedEMA,
                ema_diff: ((lastClosedKline.close - lastClosedEMA) / lastClosedEMA * 100),
                rsi: rsi[rsi.length - 1],
                macd: macd.macd[macd.macd.length - 1],
                macd_signal: macd.signal[macd.signal.length - 1],
                macd_hist: macd.histogram[macd.histogram.length - 1],
                bb_upper: bb.upper[bb.upper.length - 1],
                bb_middle: bb.middle[bb.middle.length - 1],
                bb_lower: bb.lower[bb.lower.length - 1],
                bb_width: (bb.upper[bb.upper.length - 1] - bb.lower[bb.lower.length - 1]) / bb.middle[bb.middle.length - 1],
                atr,
                atr_valid: atrValid,
                volume_change: closedVolumes.length > 1 ? closedVolumes[closedVolumes.length - 1] / closedVolumes[closedVolumes.length - 2] - 1 : 0,
                future_price_change: null,
                label: null
            };

            if (!trainingData.has(symbol)) {
                trainingData.set(symbol, []);
            }
            trainingData.get(symbol).push(dataPoint);
            if (trainingData.get(symbol).length > 1000) {
                trainingData.set(symbol, trainingData.get(symbol).slice(-1000));
            }

            saveDataPoint(symbol, dataPoint).catch(e =>
                log(`saveDataPoint failed for ${symbol}: ${e.message}`, 'error')
            );

            if (trainingData.get(symbol).length % 10 === 0) {
                exportToCSV(symbol);
            }

            deferredInsert({
                executeAt: Date.now() + 24 * 60 * 60 * 1000,
                fn: () => updateFuturePriceChange(symbol, lastClosedKline.time)
            });
            if (deferredUpdates.length > 5000) deferredUpdates.splice(0, deferredUpdates.length - 5000);
        } catch (error) {
            log(`Error preparing ML datapoint for ${symbol}: ${error.message}`, 'error');
        }
    }

    function saveTrainingData() {
        try {
            for (const symbol of trainingData.keys()) {
                exportToCSV(symbol);
            }

            const perfPath = path.join(mlDataDir, 'model_performance.json');
            fs.writeFile(perfPath, JSON.stringify(Array.from(modelPerformance.entries()), null, 2),
                (err) => { if (err) log(`Error writing model performance data: ${err.message}`, 'error'); }
            );

            log(`Saved training data for ${trainingData.size} symbols`, 'success');
        } catch (error) {
            log(`Error saving training data: ${error.message}`, 'error');
        }
    }

    function loadTrainingData() {
        try {
            log('Loading training data...', 'info');

            if (!fs.existsSync(mlDataDir)) {
                fs.mkdirSync(mlDataDir, { recursive: true });
                log('Created ML data directory', 'info');
                return;
            }

            const symbols = fs.readdirSync(mlDataDir)
                .filter(item => fs.statSync(path.join(mlDataDir, item)).isDirectory());

            for (const symbol of symbols) {
                const symbolDir = path.join(mlDataDir, symbol);
                const files = fs.readdirSync(symbolDir).filter(f =>
                    (f.endsWith('.json') || f.endsWith('.ndjson')) &&
                    f !== 'labels.ndjson'
                );
                let symbolData = [];

                for (const file of files) {
                    try {
                        const filePath = path.join(symbolDir, file);
                        let fileData;
                        if (file.endsWith('.ndjson')) {
                            fileData = fs.readFileSync(filePath, 'utf8')
                                .split('\n')
                                .filter(Boolean)
                                .map(line => JSON.parse(line));
                        } else {
                            fileData = JSON.parse(fs.readFileSync(filePath, 'utf8'));
                        }
                        symbolData = symbolData.concat(fileData);
                    } catch (error) {
                        log(`Error loading data file ${file} for ${symbol}: ${error.message}`, 'warning');
                    }
                }

                if (symbolData.length > 0) {
                    const labelsPath = path.join(symbolDir, 'labels.ndjson');
                    if (fs.existsSync(labelsPath)) {
                        try {
                            const lblMap = new Map(
                                fs.readFileSync(labelsPath, 'utf8')
                                    .split('\n').filter(Boolean)
                                    .map(l => JSON.parse(l))
                                    .map(l => [l.timestamp, l])
                            );
                            for (const d of symbolData) {
                                const lbl = lblMap.get(d.timestamp);
                                if (lbl) {
                                    d.future_price_change = lbl.future_price_change;
                                    d.label = lbl.label;
                                }
                            }
                        } catch (e) {
                            log(`Error loading labels for ${symbol}: ${e.message}`, 'warning');
                        }
                    }
                    trainingData.set(symbol, symbolData);
                    log(`Loaded ${symbolData.length} data points for ${symbol}`, 'info');
                }
            }

            const perfPath = path.join(mlDataDir, 'model_performance.json');
            if (fs.existsSync(perfPath)) {
                try {
                    const perfData = JSON.parse(fs.readFileSync(perfPath, 'utf8'));
                    for (const [symbol, data] of perfData) {
                        modelPerformance.set(symbol, data);
                    }
                    log(`Loaded performance data for ${modelPerformance.size} models`, 'info');
                } catch (error) {
                    log(`Error loading model performance data: ${error.message}`, 'warning');
                }
            }

            log(`Loaded training data for ${trainingData.size} symbols`, 'success');
        } catch (error) {
            log(`Error loading training data: ${error.message}`, 'error');
        }
    }

    async function trainAllModels(chatId) {
        try {
            await bot.sendMessage(chatId, '🧠 Starting model training. This may take some time...');

            const symbolsToTrain = Array.from(trainingData.keys())
                .filter(symbol => {
                    const data = trainingData.get(symbol);
                    const validData = data.filter(d => d.future_price_change !== null);
                    return validData.length >= 100;
                });

            if (symbolsToTrain.length === 0) {
                await bot.sendMessage(chatId, '❌ No symbols have enough data for training yet.');
                return;
            }

            await bot.sendMessage(chatId, `Training models for ${symbolsToTrain.length} symbols...`);

            let trainedCount = 0;
            let failedCount = 0;

            for (const symbol of symbolsToTrain) {
                try {
                    const { trainModelForSymbol } = require('./model');
                    const allData = trainingData.get(symbol);
                    const validData = allData.filter(d => d.future_price_change !== null);

                    if (validData.length < 100) {
                        log(`Not enough valid data points for ${symbol}: ${validData.length}`, 'warning');
                        failedCount++;
                        continue;
                    }

                    const result = await trainModelForSymbol(symbol);
                    if (result) {
                        trainedCount++;

                        if (!modelPerformance.has(symbol)) {
                            modelPerformance.set(symbol, {
                                predictions: 0,
                                correctPredictions: 0,
                                accuracy: 0,
                                lastTraining: new Date().toISOString(),
                                dataPoints: validData.length
                            });
                        } else {
                            const perf = modelPerformance.get(symbol);
                            perf.lastTraining = new Date().toISOString();
                            perf.dataPoints = validData.length;
                            modelPerformance.set(symbol, perf);
                        }

                        if (trainedCount % 5 === 0) {
                            await bot.sendMessage(
                                chatId,
                                `Progress: ${trainedCount}/${symbolsToTrain.length} models trained`
                            );
                        }
                    } else {
                        failedCount++;
                    }

                    await new Promise(resolve => setTimeout(resolve, 5000));
                } catch (error) {
                    log(`Error training model for ${symbol}: ${error.message}`, 'error');
                    failedCount++;
                }
            }

            saveTrainingData();

            await bot.sendMessage(
                chatId,
                `🧠 *ML Training Complete*\n\n` +
                `✅ Successfully trained: ${trainedCount} models\n` +
                `❌ Failed: ${failedCount} models\n\n` +
                `Use /mlstatus to check model performance.`,
                { parse_mode: 'Markdown' }
            );
        } catch (error) {
            log(`Error in trainAllModels: ${error.message}`, 'error');
            await bot.sendMessage(chatId, `❌ Error training models: ${error.message}`);
        }
    }

    async function startManualDataCollection(chatId) {
        try {
            await bot.sendMessage(chatId, '📊 Starting manual data collection for all tracked pairs...');

            const pairs = await getFuturesPairs();
            if (pairs.length === 0) {
                await bot.sendMessage(chatId, '❌ No pairs are currently being tracked.');
                return;
            }

            await bot.sendMessage(chatId, `Collecting data for ${pairs.length} pairs...`);

            let successCount = 0;
            let failedCount = 0;

            for (const symbol of pairs) {
                try {
                    if (getDualMode()) {
                        for (const tf of getDualEmaTimeframes()) {
                            await getKlines(symbol, tf);
                        }
                    }

                    const klines = await getKlines(symbol, getDualMode() ? '15m' : null);
                    if (klines.length < 30) {
                        log(`Skipping ${symbol}: Not enough candles`, 'warning');
                        failedCount++;
                        continue;
                    }

                    for (let i = 0; i < klines.length; i++) {
                        if (i < klines.length - 100) continue;

                        const candle = klines[i];
                        const klineObj = {
                            t: candle.time,
                            o: candle.open.toString(),
                            h: candle.high.toString(),
                            l: candle.low.toString(),
                            c: candle.close.toString(),
                            v: candle.volume.toString()
                        };

                        const replayTf = getDualMode() ? '15m' : null;
                        await processClosedCandle(symbol, klineObj, replayTf);
                    }

                    successCount++;
                    if ((successCount + failedCount) % 10 === 0) {
                        await bot.sendMessage(
                            chatId,
                            `Progress: ${successCount + failedCount}/${pairs.length} pairs processed`
                        );
                    }

                    await new Promise(resolve => setTimeout(resolve, 500));
                } catch (error) {
                    log(`Error collecting data for ${symbol}: ${error.message}`, 'error');
                    failedCount++;
                }
            }

            saveTrainingData();
            exportAllDataToCSV();

            await bot.sendMessage(
                chatId,
                `📊 *Data Collection Complete*\n\n` +
                `✅ Successfully collected data for ${successCount} pairs\n` +
                `❌ Failed: ${failedCount} pairs\n\n` +
                `Future price changes will be updated in 24 hours.\n` +
                `Data has been exported to CSV format for easier analysis.`,
                { parse_mode: 'Markdown' }
            );
        } catch (error) {
            log(`Error in manual data collection: ${error.message}`, 'error');
            await bot.sendMessage(chatId, `❌ Error during data collection: ${error.message}`);
        }
    }

    async function sendModelPerformance(chatId) {
        try {
            if (modelPerformance.size === 0) {
                await bot.sendMessage(chatId, '❌ No model performance data available yet.');
                return;
            }

            let message = '*ML Model Performance*\n\n';

            const sortedSymbols = Array.from(modelPerformance.keys())
                .sort((a, b) => {
                    const aMetrics = modelPerformance.get(a);
                    const bMetrics = modelPerformance.get(b);
                    return (bMetrics.accuracy || 0) - (aMetrics.accuracy || 0);
                })
                .slice(0, 10);

            for (const symbol of sortedSymbols) {
                const metrics = modelPerformance.get(symbol);
                if (!metrics || metrics.predictions < 10) continue;

                message += `*${symbol}*\n` +
                    `- Overall Accuracy: ${((metrics.accuracy || 0) * 100).toFixed(2)}%\n` +
                    `- Total Predictions: ${metrics.predictions || 0}\n` +
                    `- Data Points: ${metrics.dataPoints || 0}\n` +
                    `- Last Trained: ${metrics.lastTraining ? new Date(metrics.lastTraining).toLocaleString() : 'Unknown'}\n\n`;
            }

            const totalModels = modelPerformance.size;
            const totalPredictions = Array.from(modelPerformance.values())
                .reduce((sum, metrics) => sum + (metrics.predictions || 0), 0);
            const qualifiedModels = Array.from(modelPerformance.values())
                .filter(metrics => metrics.predictions >= 10);
            const avgAccuracy = qualifiedModels.length > 0
                ? qualifiedModels.reduce((sum, m) => sum + (m.accuracy || 0), 0) / qualifiedModels.length
                : 0;

            message += `*Summary Statistics*\n` +
                `- Total Models: ${totalModels}\n` +
                `- Total Predictions: ${totalPredictions}\n` +
                `- Average Accuracy: ${(avgAccuracy * 100).toFixed(2)}%\n\n` +
                `Use /train to train all models or /collectdata to gather more training data.`;

            await bot.sendMessage(chatId, message, {
                parse_mode: 'Markdown',
                reply_markup: {
                    inline_keyboard: [
                        [
                            { text: '🧠 Train Models', callback_data: 'train_models' },
                            { text: '📊 Export Data', callback_data: 'export_csv' }
                        ],
                        [{ text: '🔙 Back to Menu', callback_data: 'menu' }]
                    ]
                }
            });
        } catch (error) {
            log(`Error sending model performance: ${error.message}`, 'error');
            await bot.sendMessage(chatId, '❌ Error fetching model performance data');
        }
    }

    function scheduleModelTraining(isMlEnabled) {
        setInterval(async () => {
            if (!isMlEnabled()) {
                log('Scheduled model training skipped - ML is disabled', 'info');
                return;
            }

            log('Starting scheduled model training...', 'info');

            try {
                const symbolsToTrain = Array.from(trainingData.keys())
                    .filter(symbol => {
                        const data = trainingData.get(symbol);
                        const validData = data.filter(d => d.future_price_change !== null);
                        return validData.length >= 100;
                    });

                if (symbolsToTrain.length === 0) {
                    log('No symbols have enough data for training yet.', 'warning');
                    return;
                }

                log(`Training models for ${symbolsToTrain.length} symbols`, 'info');

                let trainedCount = 0;
                let failedCount = 0;

                for (const symbol of symbolsToTrain) {
                    try {
                        const { trainModelForSymbol } = require('./model');
                        const allData = trainingData.get(symbol);
                        const validData = allData.filter(d => d.future_price_change !== null);

                        if (validData.length < 100) {
                            log(`Not enough valid data points for ${symbol}: ${validData.length}`, 'warning');
                            failedCount++;
                            continue;
                        }

                        const result = await trainModelForSymbol(symbol);
                        if (result) {
                            trainedCount++;

                            if (!modelPerformance.has(symbol)) {
                                modelPerformance.set(symbol, {
                                    predictions: 0,
                                    correctPredictions: 0,
                                    accuracy: 0,
                                    lastTraining: new Date().toISOString(),
                                    dataPoints: validData.length
                                });
                            } else {
                                const perf = modelPerformance.get(symbol);
                                perf.lastTraining = new Date().toISOString();
                                perf.dataPoints = validData.length;
                                modelPerformance.set(symbol, perf);
                            }
                        } else {
                            failedCount++;
                        }

                        await new Promise(resolve => setTimeout(resolve, 5000));
                    } catch (error) {
                        log(`Error training model for ${symbol}: ${error.message}`, 'error');
                        failedCount++;
                    }
                }

                saveTrainingData();
                log(`Scheduled training completed. Trained ${trainedCount}/${symbolsToTrain.length} models.`, 'success');

                if (trainedCount > 0) {
                    await bot.sendMessage(
                        telegramChatId,
                        `🧠 *ML Model Training Completed*\n\n` +
                        `Successfully trained ${trainedCount} models.\n` +
                        `These models will now be used to enhance crossover alerts with price predictions.`,
                        { parse_mode: 'Markdown' }
                    );
                }
            } catch (error) {
                log(`Error in scheduled model training: ${error.message}`, 'error');
            }
        }, 12 * 60 * 60 * 1000);
    }

    ensureDirs();

    return {
        checkTensorFlowAvailability,
        calculateATR,
        getCurrentPrice,
        saveDataPoint,
        exportToCSV,
        exportAllDataToCSV,
        updateFuturePriceChange,
        predictPriceMovement,
        saveTrainingData,
        loadTrainingData,
        trainAllModels,
        startManualDataCollection,
        sendModelPerformance,
        scheduleModelTraining,
        onClosedCandleForTraining,
        getTrainingData: () => trainingData,
        getModelPerformance: () => modelPerformance
    };
}

module.exports = {
    createMLRuntime
};
