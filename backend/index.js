//Nic Ballesteros
//Created 6/14/21

const express = require('express');
const cors = require('cors');


//Binance Custom API Wrapper. Adds more abstraction.
const binance = require('./exchanges/binance.js');
const binanceKeys = require('./exchanges/keys/binance.js');
const redis = require('./redis.js');

// let handleErrors = (fn) => (...args) => {
//     return fn(...args).catch(err => console.error(err))
// };

/**
 * Set up Redis Module.
 */

let redisClient = new redis.Client('localhost', 6379);

/**
 * Set up Binance Client
 */

let options = {
    key: binanceKeys.key,
    secret: binanceKeys.secret,
    timeout: 15000,
    recvWindow: 10000,
    disableBeautification: false,
    handleDrift: false,
    baseUrl: 'https://api.binance.us',
    requestOptions: {},
}

let binanceClient = new binance.Client(options);

/**
 * Set up express app.
 */

const app = express();
const port = 3000;

app.use(cors());

app.use(express.json());
app.use(express.urlencoded());

let currentImportInfo = {
    ticker: null,
    exchange: null,
    startTime: null,
    endTime: null,
}

let isImporting = false;

let progress = 0;

/**
 * API Endpoint POST '/newimport'
 */

app.post('/newimport', async (req, res) => {
    console.log(`API Endpoint '/newimport' called from ${req.ip}`);

    if (isImporting) {
        res.sendStatus(405);
        return;
    }

    isImporting = true;

    currentImportInfo = req.body;

    let ticker = req.body.ticker;
    let startTime = req.body.startTime;
    let endTime = req.body.endTime;
    let exchange = req.body.exchange;

    //Get the dataSets from Redis db.
    await redisClient.reloadDataSets();
    let dataSets = redisClient.getDataSets();

    //Check that the data is not already in the database.
    let dataInDB = false;

    dataSets.forEach(element => {
        if (element.ticker === ticker) {
            if (element.exchange === exchange) {
                if (element.startTime < startTime) {
                    if (element.endTime > endTime) {
                        dataInDB = true;
                    }
                }
            }
        }
    });

    if (dataInDB) {
        res.sendStatus(400);
    }

    //Start the import.
    if (exchange === 'binance') {        
        try {
            res.sendStatus(200);
            console.log('hey');
            let exchangeData = await binanceClient.getHistoricalData(ticker, startTime, endTime, progress);

            let promises = [];

            let count = 0;

            exchangeData.forEach((item) => {
                promises.push(redisClient.putNewMin(item, ticker, exchange)
                    .then(() => {
                        count++;
                        progress = (count / exchangeData.length / 2) + .5;
                    })
                    .catch(err => console.error(err)));
            });

            await Promise.all(promises)
                .catch((err) => {
                    console.error(err);
                });

            redisClient.putNewDataSet(req.body).catch(err => console.error(err));
            return;
        } catch (err) {
            console.error(err);
        }
    }

    res.sendStatus(200);
});

app.get('currentimport', (req, res) => {
    console.log(`API Endpoint '/currentimport' called from ${req.ip}`);
});

app.get('progress', (req, res) => {
    console.log(`API Endpoint '/progress' called from ${req.ip}`);
});

/**
 * API Endpoint GET '/datasets'
 */

app.get('/datasets', (req, res) => {
    console.log(`API Endpoint '/dataSets' called from ${req.ip}`);
    
    redisClient.reloadDataSets();

    let dataSets = redisClient.getDataSets();

    res.json(dataSets);
});

app.listen(port, () => {
    console.log(`crypto-importer started on port ${port}`);
});