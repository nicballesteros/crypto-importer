/**
 * @author Nic Ballesteros
 * @description A redis API wrapper that allows for greater abstraction in the main .js file.
 * 
 */

const { resolve } = require('path/posix');
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
     * @description reloadDataSets refreshes the dataSets from the Redis database.
     * The dataSets are the span of how much data is in the database. After a new import,
     * this process needs to update the dataSets since they have changed.
     */

    async reloadDataSets() {
        //Call redis LLEN command on key 'spansets'.
        try {
            let length = await this._llen('datasets');
            this.dataSets = await this._lrange('datasets', 0, length);
            
            for(let i = 0; i < length; i++) {
                this.dataSets[i] = JSON.parse(this.dataSets[i]);
            }
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

    /**
     * @description putNewDataSet puts a new dataSet into the Redis Database after an import.
     * @param {Object} obj The data object that is going to be put into the database.
     * @returns {Promise} A promise when the database server sends back an 'OK' signal.
     */

    putNewDataSet(obj) {
        let str = JSON.stringify(obj);
        
        return this._rpush('datasets', str);
    }

    async amendDataSet(obj, newObj) {
        await this.deleteDataSet(obj);

        await this.putNewDataSet(newObj);
    }

    deleteDataSet(obj) {
        let str = JSON.stringify(obj);
        return this._lrem('datasets', 0, str)
    }

    /**
     * @description putNewMin puts a miniute of data into the Redis Database. It first
     * makes the hash key out of the openTime of the data and the ticker. Then the field is
     * set as the exchange that this data comes from. Finally the data is encoded in JSON format
     * and sent to the Redis Server.
     * @param {Object} dataObj The data that is to be put into the database.
     * @param {String} ticker The ticker of the data.
     * @param {String} exchange The exchange that the data comes from.
     * @returns {Promise} A promise when the database server sends back an 'OK' signal.
     */

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
     * @returns Promise A promise for when the database server returns an 'OK' signal.
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
     * @description This is a Promise Wrapper for the Redis LREM command.
     * @param {String} key The key to the list
     * @param {Number} count The number of elements we want to remove. (0 for all)
     * @param {String} value The value from the list that we want to remove.
     * @returns {Promise} A promise that the Redis Server will return an 'OK' signal or error.
     */

    _lrem(key, count, value) {
        return new Promise((resolve, reject) => {
            this.client.lrem(key, count, value, (err, res) => {
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

    
}

exports.Client = Redis;