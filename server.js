const express = require('express');
const WebSocket = require('ws');
const app = express();

const PORT = process.env.PORT || 3000;

app.use(express.json());

// Разрешаем CORS
app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

// Базовые значения
const BASE_EXCHANGE_PAYOUTS = {
    "EUR/JPY": 80, "GBP/JPY": 82, "AUD/CHF": 78, "AUD/USD": 80,
    "CAD/CHF": 77, "CAD/JPY": 79, "CHF/JPY": 81, "EUR/AUD": 80,
    "EUR/CAD": 79, "EUR/CHF": 78, "EUR/GBP": 82, "EUR/USD": 85,
    "GBP/AUD": 79, "GBP/CAD": 80, "GBP/CHF": 79, "GBP/USD": 83,
    "USD/CAD": 80, "USD/CHF": 79, "USD/JPY": 82, "AUD/CAD": 78, "AUD/JPY": 80
};

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

// Функция времени с учетом UTC+3
function isTradingViewOpen() {
    const now = new Date();
    // Получаем время в МСК/Киев (UTC+3)
    const mskOffset = 3 * 60; 
    const localTime = new Date(now.getTime() + (mskOffset * 60 * 1000));
    
    const day = localTime.getUTCDay(); // 0-воскресенье, 1-понедельник и т.д.
    const hours = localTime.getUTCHours();

    // Логика работы рынка: Пн(1)-Чт(4) - круглосуточно, Пт(5) до 22:00, Вс(0) с 22:00
    if (day >= 1 && day <= 4) return true;
    if (day === 0 && hours >= 22) return true;
    if (day === 5 && hours < 22) return true;
    return false;
}

function formatSocketAssetToPairName(assetName) {
    if (!assetName) return null;
    let isOtc = assetName.toLowerCase().includes('otc');
    let clean = assetName.toUpperCase().replace('_OTC', '').replace('OTC', '').replace('/', '');
    if (clean.length === 6) {
        let formatted = clean.substring(0, 3) + '/' + clean.substring(3, 6);
        return isOtc ? `${formatted} OTC` : formatted;
    }
    return null;
}

let activeWs = null;

function startPocketOptionSocket(authFrame = null) {
    const wsUrl = "wss://api-eu.po.market/socket.io/?EIO=4&transport=websocket";
    activeWs = new WebSocket(wsUrl, {
        headers: { "Origin": "https://pocketoption.com" }
    });

    activeWs.on('open', () => {
        console.log("Сокет подключен");
        activeWs.send("40");
        if (authFrame) setTimeout(() => activeWs.send(authFrame), 1000);
        setTimeout(() => activeWs.send('42["loadSymbols"]'), 2000);
    });

    activeWs.on('message', (msg) => {
        const data = msg.toString();
        if (data === "3" || data.startsWith("0")) return;
        
        try {
            const jsonString = data.replace(/^[\d-]+/, '');
            const parsed = JSON.parse(jsonString);
            
            if (Array.isArray(parsed) && parsed[0] === "updateStream") {
                const payload = parsed[1];
                const pair = formatSocketAssetToPairName(payload.asset);
                if (pair && liveMarketData[pair] !== undefined) {
                    liveMarketData[pair] = parseInt(payload.payout);
                }
            }
        } catch (e) {}
    });

    activeWs.on('close', () => setTimeout(() => startPocketOptionSocket(authFrame), 5000));
}

// API
app.post('/api/set-session', (req, res) => {
    const { ssid } = req.body;
    startPocketOptionSocket(ssid);
    res.send("Connected");
});

app.get('/api/payouts', (req, res) => {
    const exchangeOpen = isTradingViewOpen();
    for (let pair in liveMarketData) {
        if (!pair.includes("OTC") && !exchangeOpen) liveMarketData[pair] = 0;
    }
    res.json(liveMarketData);
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));

// Старт
startPocketOptionSocket();
