//Nic Ballesteros
//Created 6/14/21

//ExpressJS with cors.
const express = require('express');
const cors = require('cors');

//Binance Custom API Wrapper. Adds more abstraction.
const binance = require('./exchanges/binance.js');
const binanceKeys = require('./exchanges/keys/binance.js');

//Redis Database API Wrapper. Adds more abstraction.
const redis = require('./redis.js');

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
 * @description findOverlap finds which elements within dataSets match up with the current import
 * request.
 * @param {Array} dataSets 
 * @returns {Object} An object from dataSets that match the current import request. Returns null
 * if there is no overlap. Returns the object within dataSets that the current import request
 * overlaps with.
 */

function findOverlap(dataSets) {
    for (let i = 0; i < dataSets.length; i++) {
        let item = dataSets[i];

        if (currentImportInfo.startTime >= item.startTime && currentImportInfo.startTime <= item.endTime ||
            currentImportInfo.endTime >= item.startTime && currentImportInfo.endTime <= item.endTime) {
            return item;
        }
    }

    return null;
} /* findOverlap() */

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
 * 
 * @description
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

    let ret = await findOverlap(dataSets);

    let fixDataSet = false;

    if (ret != null) {
        if (currentImportInfo.start_time <= ret.start_time && currentImportInfo.end_time <= ret.end_time) {
            //The ending part of the request is already in memory so disregard it.      
            currentImportInfo.end_time = ret.start_time;
        } else if (currentImportInfo.start_time >= ret.start_time && currentImportInfo.end_time >= ret.end_time) {
           //The starting part of the request is already in memory so disregard it.
           currentImportInfo.start_time = ret.end_time;
        } else if (currentImportInfo.start_time >= ret.start_time && currentImportInfo.end_time <= ret.endTime) {
           //The data lies within both bounds of ret.
            res.sendStatus(400);
            return;
        } else {
            //the existing data lies outside both bounds of ret.
            res.sendStatus(400);
            return;
        }
    
    
        //Readjust the parameters because some of the requested import data already exists in redis.
        if (ret.start_time >= currentImportInfo.start_time) {
            currentImportInfo.end_time = ret.start_time;
        } else {
            currentImportInfo.end_time = ret.start_time;
        }

        fixDataSet = true;
    }

    //Start the import.
    if (exchange === 'binance') {        
        try {
            res.sendStatus(200);

            //Wait on all the queries to come back from binance, making sure not to exceed api limit.
            let exchangeData = await binanceClient.getHistoricalData(ticker, startTime, endTime, progress);

            let promises = [];

            let count = 0;

            //For each min that has been queried, put it into redis.
            exchangeData.forEach((item) => {
                promises.push(redisClient.putNewMin(item, ticker, exchange)
                    .then(() => {
                        count++;
                        progress = (count / exchangeData.length / 2) + .5;
                    })
                    .catch(err => console.error(err)));
            });

            //When redis returns 'OK' for all put requests, continue.
            await Promise.all(promises)
                .catch((err) => {
                    console.error(err);
                });

            if (fixDataSet) {
                let newObj = ret;

                if (currentImportInfo.endTime >= ret.endTime) {
                    newObj.endTime = currentImportInfo.endTime;
                } 
                
                if (currentImportInfo.startTime <= ret.startTime){
                    newObj.startTime = currentImportInfo.startTime;
                }

                redisClient.amendDataSet(ret, newObj);
                return;
            }

            redisClient.putNewDataSet(req.body).catch(err => console.error(err));
            return;
        } catch (err) {
            console.error(err);
            return;
        }
    }

    res.sendStatus(200);
});

/**
 * API Endpoint GET '/currentimport'
 * 
 * @description
 */

app.get('/currentimport', (req, res) => {
    console.log(`API Endpoint '/currentimport' called from ${req.ip}`);

    //If there is an import running, send back the details of the import.
    //Else send 204 status because there is no content from this endpoint.
    if (!isImporting) {
        res.sendStatus(204);

        //Print that there was no content to be given to the user.
        console.log("/currentimport 204");
        return;
    }

    res.json({
        exchange: currentImportInfo.exchange,
        ticker: currentImportInfo.ticker,
        startTime: currentImportInfo.startTime,
        endTime: currentImportInfo.endTime,
        progress: progress,
        status: 200,
    });

    //Print that the request was properly taken care of.
    console.log("/currentimport 200");
    res.sendStatus(200);
});

/**
 * API Endpoint GET '/progress'
 * 
 * @description
 */

app.get('/progress', (req, res) => {
    console.log(`API Endpoint '/progress' called from ${req.ip}`);

    if (!isImporting) {
        res.sendStatus(204);
        return;
    }

    res.json({
        progress: progress
    });
});

/**
 * API Endpoint GET '/datasets'
 */

app.get('/datasets', async (req, res) => {
    console.log(`API Endpoint '/dataSets' called from ${req.ip}`);
    
    await redisClient.reloadDataSets();

    let dataSets = redisClient.getDataSets();

    res.json(dataSets);
});

app.listen(port, () => {
    console.log(`crypto-importer started on port ${port}`);
});