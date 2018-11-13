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
            await this._adapter.init(storage.connection, log, bootstrap);
            this._wasInit = true;
        }
    }

    getAdapter() {
        return this._adapter;
    }

    async put(options) {
        return this._put({ Path: path.join(STORAGE_PREFIX.HKUBE, `${moment().format(this.DateFormat)}/${options.jobId}/${options.taskId}`), Data: options.data });
    }

    async putResults(options) {
        return this._put({ Path: path.join(STORAGE_PREFIX.HKUBE_RESULTS, `${moment().format(this.DateFormat)}/${options.jobId}/result.json`), Data: options.data });
    }

    async putMetadata(options) {
        return this._put({ Path: path.join(STORAGE_PREFIX.HKUBE_METADATA, `${moment().format(this.DateFormat)}/${options.jobId}/${options.taskId}/metadata.json`), Data: options.data });
    }

    async get(options) {
        if (options && options.data && options.data.storageInfo) {
            try {
                const data = await this._get(options.data.storageInfo);
                return { ...options, data, storageModule: this.moduleName };
            }
            catch (error) {
                return { error: new Error(`failed to get from storage: ${error.message}`) };
            }
        }
        return { error: new Error('got invalid from storage') };
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
        return this._adapter.listObjects({ ...options, Path: STORAGE_PREFIX.HKUBE });
    }

    async listResults(options = null) {
        return this._adapter.listObjects({ ...options, Path: STORAGE_PREFIX.HKUBE_RESULTS });
    }

    async delete(options) {
        return this._adapter.delete({ ...options, Path: STORAGE_PREFIX.HKUBE });
    }

    async deleteResults(options) {
        return this._adapter.delete({ ...options, Path: STORAGE_PREFIX.HKUBE_RESULTS });
    }
}

module.exports = new StorageManager();
