/**
 * Nic Ballesteros
 * Created: 6/14/21
 */

const binance = require('binance');

class Binance {
    constructor(options) {
        this.binanceClient = new binance.BinanceRest(options);

        // this._handleErr = async (fn) => (...args) => {
        //     return fn(...args).catch(err => console.error(err));
        // }
    }

    async getHistoricalData(ticker, startTime, endTime, progress) {
        if (startTime > endTime) {
            throw new Error;
        }
        
        let info = {
            symbol: ticker,
            interval: '1m',
            limit: 1000,
            startTime: startTime,
            endTime: endTime,
        }
        
        //diff in milis
        let diff = endTime - startTime;

        //Convert diff to mins.
        diff /= 1000;

        //Each request is 1000 mins.
        let numberOfRequests = diff / 1000;

        let requests = [];

        let requestCount = 0;

        for (let i = 0; i < numberOfRequests; i++) {
            let delay = i * 60;
            requests.push(
                this._delayRequest(delay, this._klines.bind(this), info)
                    .then(() => {
                        requestCount++;
                        progress = requestCount / numberOfRequests / 2;
                    })
            );
        }

        let requestArrays = await Promise.all(requests).catch(err => console.error(err));

        let data = [];

        //Put all the data into one array.
        await requestArrays.forEach((item) => {
            item.forEach(element => {
                data.push(element);
            });
        });

        return data;
    }

    _klines(options) {
        return new Promise((resolve, reject) => {
            this.binanceClient.klines(options, (err, res) => {
                if (err) {
                    reject(err);
                    return;
                }

                resolve(res);
            });
        });
    }

    _delayRequest(delay, fn, ...args) {
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                fn(...args)
                    .then((res) => {
                        resolve(res);
                    })
                    .catch((err) => {
                        reject(err);
                    });
            }, delay);
        });
    }
}

exports.Client = Binance;