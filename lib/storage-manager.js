const path = require('path');
const moment = require('moment');
const component = 'StorageManager';
const { STORAGE_PREFIX } = require('../consts/storage-prefix');
const log = require('@hkube/logger').GetLogFromContainer();

class StorageManager {
    constructor() {
        this._wasInit = false;
    }

    get DateFormat() {
        return 'YYYY-MM-DD';
    }

    async init(config, bootstrap = false) {
        if (!this._wasInit) {
            const storage = config.storageAdapters[config.defaultStorage];
            this.moduleName = storage.moduleName;
            this._adapter = require(storage.moduleName);  // eslint-disable-line
            await this._adapter.init(storage.connection, log, STORAGE_PREFIX, bootstrap);
            this._wasInit = true;
        }
    }

    async put({ jobId, taskId, data }) {
        return this._put({ Path: path.join(STORAGE_PREFIX.HKUBE, moment().format(this.DateFormat), jobId, taskId), Data: data });
    }

    async putResults({ jobId, data }) {
        return this._put({ Path: path.join(STORAGE_PREFIX.HKUBE_RESULTS, moment().format(this.DateFormat), jobId, 'result.json'), Data: data });
    }

    async putMetadata({
        date, jobId, taskId, data
    }) {
        return this._put({ Path: path.join(STORAGE_PREFIX.HKUBE_METADATA, moment(date).format(this.DateFormat), jobId, taskId), Data: data });
    }

    async putStore({ type, name, data }) {
        return this._put({ Path: path.join(STORAGE_PREFIX.HKUBE_STORE, type, `${name}.json`), Data: data });
    }

    async putExecution({ jobId, date, data }) {
        return this._put({
            Path: path.join(STORAGE_PREFIX.HKUBE_EXECUTION, jobId, 'descriptor.json'),
            Data: {
                data,
                date: moment(date).format(this.DateFormat)
            }
        });
    }

    async get(options) {
        try {
            return await this._get(options);
        }
        catch (error) {
            return { error: new Error(`failed to get from storage: ${error.message}`) };
        }
    }

    async _put(options) {
        const start = Date.now();
        const result = await this._adapter.put(options);
        const end = Date.now();
        if (this._log) {
            const diff = end - start;
            this._log.debug(`Execution of put takes ${diff} ms`, { component, operation: 'put', time: diff });
        }
        return result;
    }

    async _get(options) {
        const start = Date.now();
        const result = await this._adapter.get(options);
        const end = Date.now();

        if (this._log) {
            const diff = end - start;
            this._log.debug(`Execution of get takes ${diff} ms`, { component, operation: 'get', time: diff });
        }

        return result;
    }

    async list(options) {
        return this._list(options, STORAGE_PREFIX.HKUBE);
    }

    async listResults(options) {
        return this._list(options, STORAGE_PREFIX.HKUBE_RESULTS);
    }

    async listMetadata({ jobId, date, nodeName }) {
        return this._list({ Path: path.join(moment(date).format(this.DateFormat), jobId, nodeName) }, STORAGE_PREFIX.HKUBE_METADATA);
    }

    async listStore({ type }) {
        return this._list({ Path: type }, STORAGE_PREFIX.HKUBE_STORE);
    }

    async listExecution({ jobId }) {
        return this._list({ Path: jobId }, STORAGE_PREFIX.HKUBE_EXECUTION);
    }

    async _list(options, prefix) {
        if (options && options.Path) {
            return this._adapter.list({ Path: path.join(prefix, options.Path) });
        }
        return this._adapter.list({ Path: path.join(prefix, '/') });
    }

    async delete(options) {
        return this._adapter.delete(options);
    }
}

module.exports = new StorageManager();
