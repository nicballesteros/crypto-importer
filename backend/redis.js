/**
 * @author Nic Ballesteros
 * @description A redis API wrapper that allows for greater abstraction in the main .js file.
 * 
 */

const redis = require('redis');

class Redis {
    constructor(host, port) {
        //Create a new redis client.
        this.client = redis.createClient({
            port: port,
            host: host,
        });

        this.dataSets = [];

        // this._handleErr = async (fn) => (...args) => {
        //     return fn(...args).catch(err => console.error(err));
        // };

        //Run this code when the client connects to the database.
        this.client.on('connect', async () => {
            //Print to the console that a connection has been made.
            console.log('Connected to Redis');

            //Pull in dataSets from redis for the first time.
            await this.reloadDataSets();
        });
    }

    /**
     * @description reloadDataSets reloads the dataSets from the database.
     */

    async reloadDataSets() {
        //Call redis LLEN command on key 'spansets'.
        try {
            let length = await this._llen('spansets');
            this.dataSets = await this._lrange('spansets', 0, length);
        } catch (err) {
            this.dataSets = [];
            return;
        }
    }

    /**
     * @description A getter for dataSets.
     * @returns Array dataSets
     */

    getDataSets() {
        return this.dataSets;
    }

    putNewDataSet(obj) {
        let str = JSON.stringify(obj);
        
        return this._rpush('spansets', str);
    }

    putNewMin(dataObj, ticker, exchange) {
        let key = dataObj.openTime;

        key += ':';
        key += ticker;

        let field = {};

        field[exchange] = JSON.stringify(dataObj);

        return this._hmset(key, field);
    }

    /**
     * @description A Redis LLEN Promise wrapper.
     * @param String key 
     * @returns Promise
     */

    _llen(key) {
        return new Promise((resolve, reject) => {
            this.client.llen(key, (err, res) => {
                if (err) {
                    reject(err);
                    return;
                }

                resolve(res);
            });
        });
    }

    /**
     * @description A Redis LRANGE Promise wrapper.
     * @param String key 
     * @param Number from 
     * @param Number to 
     * @returns Promise
     */

    _lrange(key, from, to) {
        return new Promise((resolve, reject) => {
            this.client.lrange(key, from, to, (err, res) => {
                if (err) {
                    reject(err);
                    return;
                }

                resolve(res);
            });
        });
    }

    /**
     * @description A Redis RPUSH Promise Wrapper.
     * 
     * @param {String} key 
     * @param {String} data 
     * @returns Promise
     */

    _rpush(key, data) {
        return new Promise((resolve, reject) => {
            this.client.rpush(key, data, (err, res) => {
                if (err) {
                    reject(err);
                    return;
                }

                resolve(res);
            });
        });
    }

    /**
     * @description A Promise Wrapper for the Redis HMSET command
     * @param {String} key 
     * @param {String} field 
     * @returns Promise
     */

    _hmset(key, field) {
        return new Promise((resolve, reject) => {
            this.client.hmset(key, field, (err, res) => {
                if (err) {
                    reject(err);
                    return;
                }

                resolve(res);
            });
        });
    }

    /**
     * @description A Promise Wrapper for the Redis HMGET command.
     * @param String key 
     * @param String field 
     * @returns Promise
     */

    _hmget(key, field) {
        return new Promise((resolve, reject) => {
            this.client.hmget(key, field, (err, res) => {
                if (err) {
                    reject(err);
                    return;
                }

                resolve(res);
            });
        });
    }

    _handleErr(err) {
        console.error(err);
    }
}

exports.Client = Redis;