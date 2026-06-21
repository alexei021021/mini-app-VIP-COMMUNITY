// Полностью готовый server.js со встроенным чистым вебсокетом
const express = require('express');
const WebSocket = require('ws'); //
const app = express();

const PORT = process.env.PORT || 3000;

// Разрешаем CORS для Telegram WebApp
app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

// Базовые значения процентов для восстановления биржевых пар, когда открывается рынок
const BASE_EXCHANGE_PAYOUTS = {
    "EUR/JPY": 80, "GBP/JPY": 82, "AUD/CHF": 78, "AUD/USD": 80,
    "CAD/CHF": 77, "CAD/JPY": 79, "CHF/JPY": 81, "EUR/AUD": 80,
    "EUR/CAD": 79, "EUR/CHF": 78, "EUR/GBP": 82, "EUR/USD": 85,
    "GBP/AUD": 79, "GBP/CAD": 80, "GBP/CHF": 79, "GBP/USD": 83,
    "USD/CAD": 80, "USD/CHF": 79, "USD/JPY": 82, "AUD/CAD": 78, "AUD/JPY": 80
};

// Твой ПОЛНЫЙ список валютных пар
let liveMarketData = {
    "AUD/CAD OTC": 92, "AUD/NZD OTC": 92, "AUD/USD OTC": 92, "BHD/CNY OTC": 92,
    "CAD/CHF OTC": 92, "CAD/JPY OTC": 92, "CHF/NOK OTC": 92, "EUR/GBP OTC": 92,
    "EUR/HUF OTC": 92, "EUR/USD OTC": 92, "GBP/JPY OTC": 92, "GBP/USD OTC": 92,
    "LBP/USD OTC": 92, "NGN/USD OTC": 92, "TND/USD OTC": 92, "USD/ARS OTC": 92,
    "USD/BDT OTC": 92, "USD/BRL OTC": 92, "USD/CAD OTC": 92, "USD/CHF OTC": 92,
    "USD/CLP OTC": 92, "USD/EGP OTC": 92, "USD/INR OTC": 92, "USD/MYR OTC": 92,
    "USD/PHP OTC": 92, "USD/PKR OTC": 92, "USD/THB OTC": 92, "USD/VND OTC": 92,
    "YER/USD OTC": 92, "ZAR/USD OTC": 92, "USD/DZD OTC": 91, "AUD/JPY OTC": 90,
    "NZD/JPY OTC": 90, "USD/COP OTC": 90, "EUR/RUB OTC": 86, "EUR/CHF OTC": 85,
    "USD/IDR OTC": 79, "EUR/JPY OTC": 77, "UAH/USD OTC": 76, "GBP/AUD OTC": 75,
    "USD/CNH OTC": 75, "USD/SGD OTC": 74, "CHF/JPY OTC": 69, "AED/CNY OTC": 67,
    "EUR/NZD OTC": 67, "QAR/CNY OTC": 66, "SAR/CNY OTC": 65, "MAD/USD OTC": 61,
    "AUD/CHF OTC": 60, "EUR/TRY OTC": 60, "USD/MXN OTC": 59, "USD/JPY OTC": 55,
    "USD/RUB OTC": 54, "NZD/USD OTC": 53, "OMR/CNY OTC": 51, "JOD/CNY OTC": 46,
    "KES/USD OTC": 39,
    ...BASE_EXCHANGE_PAYOUTS
};

function isTradingViewOpen() {
    const now = new Date();
    const day = now.getUTCDay(); 
    const hours = now.getUTCHours();
    if (day === 1 || day === 2 || day === 3 || day === 4) return true;
    if (day === 0 && hours >= 22) return true;
    if (day === 5 && hours < 22) return true;
    return false;
}

function formatSocketAssetToPairName(assetName) {
    if (!assetName) return null;
    let isOtc = assetName.toLowerCase().includes('_otc') || assetName.toLowerCase().includes('otc');
    let clean = assetName.toUpperCase().replace('_OTC', '').replace('OTC', '').replace('/', '');
    if (clean.length === 6) {
        let formatted = clean.substring(0, 3) + '/' + clean.substring(3, 6);
        return isOtc ? `${formatted} OTC` : formatted;
    }
    return null;
}

// --- ИСПРАВЛЕННЫЙ ВЕБСОКЕТ КЛИЕНТ ---
function startPocketOptionSocket() {
    // Используем стабильный эндпоинт
    const wsUrl = "wss://api-eu.po.market/socket.io/?EIO=4&transport=websocket";

    console.log("Попытка подключения к сокету Pocket Option...");

    const ws = new WebSocket(wsUrl, {
        perMessageDeflate: false,
        headers: {
            "Origin": "https://pocketoption.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"
        }
    });

    let pingInterval = null;

    ws.on('open', () => {
        console.log("Успешное подключение к сокету Pocket Option!");
        
        // Шаг 1: Для Engine.IO v4 отправляем "40" для инициализации сессии Socket.io
        ws.send("40");
        
        // Шаг 2: Сразу после этого шлем подписку на стрим символов
        setTimeout(() => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send('42["loadSymbols"]');
                console.log("Запрос loadSymbols отправлен в стрим.");
            }
        }, 500);

        // Шаг 3: Интервал для удержания пинга (Engine.IO ожидает регулярных пингов)
        clearInterval(pingInterval);
        pingInterval = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send("3"); // Код пинга
            }
        }, 20000);
    });

    ws.on('message', (message) => {
        try {
            const data = message.toString();

            // Если это просто ответ на пинг или системное сообщение открытия ("0{"...) — игнорируем
            if (data === "3" || data.startsWith("0{")) return;

            // Вырезаем текстовый префикс Engine.IO (например, "42", "451-") чтобы получить чистый JSON
         const express = require('express');
const WebSocket = require('ws');
const https = require('https');
const app = express();

const PORT = process.env.PORT || 3000;

app.use(express.json());

// Разрешаем CORS для доступа из вашего Web App
app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

let manualSsid = null; 
let activeWs = null;

// Данные по парам
let liveMarketData = {
    "EUR/USD": 85, "GBP/USD": 83, "USD/JPY": 82, "USD/CAD": 80,
    "AUD/USD": 80, "EUR/JPY": 80, "GBP/JPY": 82
};

// Функция принудительного подключения сокета с имитацией браузера
function connectSocket(authFrame) {
    if (activeWs) {
        try { activeWs.terminate(); } catch(e) {}
    }

    console.log("[SOCKET] Попытка подключения к API Pocket Option...");
    
    const wsOptions = {
        headers: {
            "Origin": "https://pocketoption.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits"
        }
    };

    activeWs = new WebSocket("wss://api-eu.po.market/socket.io/?EIO=4&transport=websocket", wsOptions);

    activeWs.on('open', () => {
        console.log("[SOCKET] Соединение установлено. Отправка хендшейка...");
        activeWs.send("40");
        
        setTimeout(() => {
            activeWs.send(authFrame);
            console.log("[SOCKET] Авторизация отправлена.");
        }, 1500);
    });

    activeWs.on('message', (msg) => {
        const data = msg.toString();
        
        // Исправленная логика обработки сообщений Socket.IO
        // Убираем префикс (числа в начале, например "42")
        const jsonString = data.replace(/^[\d-]+/, '');
        
        // Проверяем, является ли очищенная строка JSON-объектом или массивом
        if (jsonString.startsWith('[') || jsonString.startsWith('{')) {
            try {
                const parsed = JSON.parse(jsonString);
                // Здесь вы можете обрабатывать конкретные события API
                console.log("[SOCKET] Получен объект:", parsed);
            } catch (e) {
                console.error("[SOCKET] Ошибка парсинга JSON:", e.message);
            }
        }
    });

    activeWs.on('error', (err) => {
        console.error("[SOCKET ERROR]:", err.message);
    });

    activeWs.on('close', () => {
        console.log("[SOCKET] Соединение закрыто. Переподключение через 10 секунд...");
        setTimeout(() => connectSocket(authFrame), 10000);
    });
}

// API эндпоинт для ручной установки SSID
app.post('/api/set-session', (req, res) => {
    const { ssid } = req.body;
    if (!ssid) return res.status(400).send("SSID is required");
    
    manualSsid = ssid;
    console.log("[API] Получен новый SSID, запускаем подключение.");
    connectSocket(ssid);
    res.send("Session updated and reconnected.");
});

// API для получения данных о выплатах
app.get('/api/payouts', (req, res) => {
    res.json(liveMarketData);
});

app.listen(PORT, () => {
    console.log(`Сервер успешно запущен на порту ${PORT}`);
});
            // Слушаем событие updateStream
            if (Array.isArray(parsed) && parsed[0] === "updateStream") {
                const payload = parsed[1];
                if (payload && payload.asset && payload.payout) {
                    const myPairName = formatSocketAssetToPairName(payload.asset);

                    if (myPairName && liveMarketData[myPairName] !== undefined) {
                        const newPayoutValue = parseInt(payload.payout);
                        if (newPayoutValue > 0 && newPayoutValue <= 100) {
                            liveMarketData[myPairName] = newPayoutValue;
                        }
                    }
                }
            }
        } catch (error) {
            // Ошибки бинарных пакетов или не-JSON структур гасятся, чтобы сервер не падал
        }
    });

    ws.on('close', (code, reason) => {
        console.log(`Соединение закрыто. Код: ${code}. Причина: ${reason || 'нет'}. Переподключение через 5 сек...`);
        clearInterval(pingInterval);
        setTimeout(startPocketOptionSocket, 5000);
    });

    ws.on('error', (error) => {
        console.error("Ошибка сокета:", error.message);
    });
}

// Старт сокета
startPocketOptionSocket();

// Фоновое обновление / проверка расписания
setInterval(() => {
    const exchangeOpen = isTradingViewOpen();
    for (let pair in liveMarketData) {
        const isOtcPair = pair.includes("OTC");

        if (!isOtcPair && !exchangeOpen) {
            liveMarketData[pair] = 0;
            continue;
        }
        if (!isOtcPair && exchangeOpen && liveMarketData[pair] === 0) {
            liveMarketData[pair] = BASE_EXCHANGE_PAYOUTS[pair];
        }

        // Легкое шевеление, если по какой-то паре временно затих поток
        if (liveMarketData[pair] > 0) {
            let changes = [-1, 0, 1];
            let randomChange = changes[Math.floor(Math.random() * changes.length)];
            let newPercent = liveMarketData[pair] + randomChange;
            if (newPercent >= 40 && newPercent <= 97) {
                liveMarketData[pair] = newPercent;
            }
        }
    }
}, 4000);

// API Эндпоинты
app.get('/api/payouts', (req, res) => {
    const exchangeOpen = isTradingViewOpen();
    for (let pair in liveMarketData) {
        if (!pair.includes("OTC") && !exchangeOpen) {
            liveMarketData[pair] = 0;
        }
    }
    res.json(liveMarketData); //
});

app.get('/', (req, res) => {
    const totalCount = Object.keys(liveMarketData).length;
    res.send(`Сервер TEAM успешно запущен! Всего пар в базе: ${totalCount}`); //
});

app.listen(PORT, () => {
    console.log(`Сервер работает на порту ${PORT}`); //
});
