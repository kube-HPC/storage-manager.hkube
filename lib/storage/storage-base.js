
const now = require('performance-now');
const component = 'StorageManager';
class StorageBase {
    get DateFormat() {
        return 'YYYY-MM-DD';
    }

    constructor(adapter, log) {
        this._adapter = adapter;
        this._log = log;
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
            if (this._log) {
                this._log.debug(`put - Execution time ${diff} ms`, { component, operation: 'put', time: diff });
            }
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
            if (this._log) {
                this._log.debug(`get - Execution time ${diff} ms`, { component, operation: 'get', time: diff });
            }
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
        if (this._log) {
            this._log.debug(`list - Execution time ${diff} ms`, { component, operation: 'list', time: diff });
        }
        return result;
    }

    async listDir(options) {
        const allResult = await this._adapter.list(options);
        const dirSet = allResult.reduce((set, item) => {
            const { path } = item;
            let pathEnding = path.replace(options.path, '');
            pathEnding = pathEnding[0] === '/' && pathEnding.substring(1);
            const dir = pathEnding.substring(0, pathEnding.indexOf('/'));
            set.add(dir);
            return set;
        }, new Set());
        return Array.from(dirSet);
    }

    async listPrefixes(options) {
        const start = now();
        const result = await this._adapter.listPrefixes(options);
        const end = now();
        const diff = (end - start).toFixed(3);
        if (this._log) {
            this._log.debug(`listPrefixes - Execution time ${diff} ms`, { component, operation: 'list', time: diff });
        }
        return result;
    }

    async listWithStats(options) {
        const start = now();
        const result = await this._adapter.listWithStats(options);
        const end = now();
        const diff = (end - start).toFixed(3);
        if (this._log) {
            this._log.debug(`listWithStats - Execution time ${diff} ms`, { component, operation: 'list', time: diff });
        }
        return result;
    }

    async delete(options) {
        const start = now();
        const result = await this._adapter.delete(options);
        const end = now();
        const diff = (end - start).toFixed(3);
        if (this._log) {
            this._log.debug(`delete - Execution time ${diff} ms`, { component, operation: 'delete', time: diff });
        }
        return result;
    }

    async getStream(options) {
        const start = now();
        const result = await this._adapter.getStream(options);
        const end = now();
        const diff = (end - start).toFixed(3);
        if (this._log) {
            this._log.debug(`getStream - Execution time ${diff} ms`, { component, operation: 'getStream', time: diff });
        }
        return result;
    }

    async putStream(options) {
        const start = now();
        const result = await this._adapter.putStream(options);
        const end = now();
        const diff = (end - start).toFixed(3);
        if (this._log) {
            this._log.debug(`putStream - Execution time ${diff} ms`, { component, operation: 'putStream', time: diff });
        }
        return result;
    }
}

module.exports = StorageBase;
