
const now = require('performance-now');
const component = 'StorageManager';
const logger = require('@hkube/logger');
let log;

class StorageBase {
    get DateFormat() {
        return 'YYYY-MM-DD';
    }

    constructor(adapter) {
        this._adapter = adapter;
        log = logger.GetLogFromContainer();
    }

    async put(options, tracer) {
        let span = null;
        try {
            const start = now();
            if (tracer) {
                span = tracer();
            }
            const result = await this._adapter.put(options);
            if (span) span.finish();
            const end = now();
            const diff = (end - start).toFixed(3);
            log.debug(`put - Execution time ${diff} ms`, { component, operation: 'put', time: diff });
            return result;
        }
        catch (error) {
            if (span) {
                span.finish(error);
            }
            throw error;
        }
    }

    async get(options, tracer) {
        let span = null;
        try {
            const start = now();
            if (tracer) {
                span = tracer(options);
            }
            const result = await this._adapter.get(options);
            if (span) span.finish();
            const end = now();
            const diff = (end - start).toFixed(3);
            log.debug(`get - Execution time ${diff} ms`, { component, operation: 'get', time: diff });
            return result;
        }
        catch (error) {
            if (span) {
                span.finish(error);
            }
            throw error;
        }
    }

    async list(options) {
        const start = now();
        const result = await this._adapter.list(options);
        const end = now();
        const diff = (end - start).toFixed(3);
        log.debug(`list - Execution time ${diff} ms`, { component, operation: 'list', time: diff });
        return result;
    }

    async listPrefixes(options) {
        const start = now();
        const result = await this._adapter.listPrefixes(options);
        const end = now();
        const diff = (end - start).toFixed(3);
        log.debug(`listPrefixes - Execution time ${diff} ms`, { component, operation: 'list', time: diff });
        return result;
    }

    async delete(options) {
        const start = now();
        const result = await this._adapter.delete(options);
        const end = now();
        const diff = (end - start).toFixed(3);
        log.debug(`delete - Execution time ${diff} ms`, { component, operation: 'delete', time: diff });
        return result;
    }

    async getStream(options) {
        const start = now();
        const result = await this._adapter.getStream(options);
        const end = now();
        const diff = (end - start).toFixed(3);
        log.debug(`getStream - Execution time ${diff} ms`, { component, operation: 'getStream', time: diff });
        return result;
    }

    async putStream(options) {
        const start = now();
        const result = await this._adapter.putStream(options);
        const end = now();
        const diff = (end - start).toFixed(3);
        log.debug(`putStream - Execution time ${diff} ms`, { component, operation: 'putStream', time: diff });
        return result;
    }
}

module.exports = StorageBase;
