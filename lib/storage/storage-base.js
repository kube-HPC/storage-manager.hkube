
const now = require('performance-now');
const component = 'StorageManager';
const log = require('@hkube/logger').GetLogFromContainer();

class StorageBase {
    get DateFormat() {
        return 'YYYY-MM-DD';
    }

    constructor(adapter) {
        this._adapter = adapter;
    }

    async put(options) {
        const start = now();
        const result = await this._adapter.put(options);
        const end = now();
        const diff = (end - start).toFixed(3);
        log.debug(`put - Execution time ${diff} ms`, { component, operation: 'put', time: diff });
        return result;
    }

    async get(options) {
        try {
            const start = now();
            const result = await this._adapter.get(options);
            const end = now();
            const diff = (end - start).toFixed(3);
            log.debug(`get - Execution time ${diff} ms`, { component, operation: 'get', time: diff });
            return result;
        }
        catch (error) {
            return { error: new Error(`failed to get from storage: ${error.message}`) };
        }
    }

    async list(options) {
        try {
            const start = now();
            const result = await this._adapter.list(options);
            const end = now();
            const diff = (end - start).toFixed(3);
            log.debug(`list - Execution time ${diff} ms`, { component, operation: 'list', time: diff });
            return result;
        }
        catch (error) {
            return { error: new Error(`failed to list from storage: ${error.message}`) };
        }
    }

    async delete(options) {
        const start = now();
        const result = await this._adapter.delete(options);
        const end = now();
        const diff = (end - start).toFixed(3);
        log.debug(`delete - Execution time ${diff} ms`, { component, operation: 'delete', time: diff });
        return result;
    }
}

module.exports = StorageBase;
