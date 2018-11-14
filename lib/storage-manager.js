const moment = require('moment');
const component = 'StorageManager';
const path = require('path');
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

    async put(options) {
        return this._put({ Path: path.join(STORAGE_PREFIX.HKUBE, moment().format(this.DateFormat), options.jobId, options.taskId), Data: options.data });
    }

    async putResults(options) {
        return this._put({ Path: path.join(STORAGE_PREFIX.HKUBE_RESULTS, moment().format(this.DateFormat), options.jobId, 'result.json'), Data: options.data });
    }

    async putMetadata(options) {
        return this._put({ Path: path.join(STORAGE_PREFIX.HKUBE_METADATA, moment().format(this.DateFormat), options.jobId, options.taskId), Data: options.data });
    }

    async putStore(options) {
        return this._put({ Path: path.join(STORAGE_PREFIX.HKUBE_STORE, options.type, `${options.name}.json`), Data: options.data });
    }

    async putExecution(options) {
        return this._put({ Path: path.join(STORAGE_PREFIX.HKUBE_EXECUTION, options.jobId, 'descriptor.json'), Data: options.data });
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

    async list(options = null) {
        if (options && options.Path) {
            return this._adapter.list({ Path: path.join(STORAGE_PREFIX.HKUBE, options.Path) });
        }
        return this._adapter.list({ Path: path.join(STORAGE_PREFIX.HKUBE, '/') });
    }

    async listResults(options = null) {
        return this._adapter.list({ ...options, Path: STORAGE_PREFIX.HKUBE_RESULTS });
    }

    async listMetadata(options = null) {
        return this._adapter.list({ ...options, Path: STORAGE_PREFIX.HKUBE_METADATA });
    }

    async listStore(options = null) {
        return this._adapter.list({ ...options, Path: STORAGE_PREFIX.HKUBE_STORE });
    }

    async listExecution(options = null) {
        return this._adapter.list({ ...options, Path: STORAGE_PREFIX.HKUBE_EXECUTION });
    }

    async delete(options) {
        return this._adapter.delete({ Path: path.join(STORAGE_PREFIX.HKUBE, options.path) });
    }

    async deleteResults(options) {
        return this._adapter.delete({ ...options, Path: STORAGE_PREFIX.HKUBE_RESULTS });
    }
}

module.exports = new StorageManager();
