
const now = require('performance-now');

const component = 'StorageManager';
class StorageBase {
    get DateFormat() {
        return 'YYYY-MM-DD';
    }

    constructor(adapter, log, encoding) {
        this._adapter = adapter;
        this._encoding = encoding;
        this._log = log;
        this.put = this._wrapper(this.put.bind(this));
        this.get = this._wrapper(this.get.bind(this));
        this.seek = this._wrapper(this.seek.bind(this));
        this.getMetadata = this._wrapper(this.getMetadata.bind(this));
        this.list = this._wrapper(this.list.bind(this));
        this.listDir = this._wrapper(this.listDir.bind(this));
        this.listPrefixes = this._wrapper(this.listPrefixes.bind(this));
        this.listWithStats = this._wrapper(this.listWithStats.bind(this));
        this.delete = this._wrapper(this.delete.bind(this));
        this.getStream = this._wrapper(this.getStream.bind(this));
        this.putStream = this._wrapper(this.putStream.bind(this));
    }

    _wrapper(func) {
        const wrapper = async (args, tracer) => {
            let span = null;
            try {
                let start;
                if (this._log) {
                    start = now();
                }
                if (tracer) {
                    span = tracer();
                }

                const result = await func(args);

                if (span) {
                    span.finish();
                }
                if (this._log) {
                    const end = now();
                    const diff = (end - start).toFixed(3);
                    const operation = func.name.replace('bound ', '');
                    this._log.debug(`${operation} - Execution time ${diff} ms`, { component, operation, time: diff });
                }
                return result;
            }
            catch (error) {
                if (span) {
                    span.finish(error);
                }
                throw error;
            }
        };
        return wrapper;
    }

    async put(options) {
        const encodeOptions = options.encodeOptions || {};
        let { data } = options;
        const { header } = options;
        const metadata = header ? { header } : null;
        if (!encodeOptions.ignoreEncode) {
            data = this._encoding.encode(options.data, encodeOptions);
        }
        const result = await this._adapter.put({ ...options, metadata, data });
        return result;
    }

    async get(options) {
        const encodeOptions = options.encodeOptions || {};
        let result = await this._adapter.get(options);
        if (!encodeOptions.ignoreEncode) {
            result = this._encoding.decode(result, encodeOptions);
        }
        return result;
    }

    async seek(options) {
        const result = await this._adapter.seek(options);
        return result;
    }

    async getHeader(options) {
        const result = await this._adapter.getHeader(options);
        return result;
    }

    async getMetadata(options) {
        return this._adapter.getMetadata(options);
    }

    async list(options) {
        const result = await this._adapter.list(options);
        return result;
    }

    async listDir(options) {
        const allResult = await this._adapter.list(options);
        const dirSet = allResult.reduce((set, item) => {
            const { path } = item;
            let pathEnding = path.replace(options.path, '');
            pathEnding = (pathEnding[0] === '/' && pathEnding.substring(1)) || pathEnding;
            pathEnding = (pathEnding[0] === '/' && pathEnding.substring(1)) || pathEnding;
            const dir = (pathEnding.indexOf('/') === -1 && pathEnding) || pathEnding.substring(0, pathEnding.indexOf('/'));
            set.add(dir);
            return set;
        }, new Set());
        return Array.from(dirSet);
    }

    async listPrefixes(options) {
        const result = await this._adapter.listPrefixes(options);
        return result;
    }

    async listWithStats(options) {
        const result = await this._adapter.listWithStats(options);
        return result;
    }

    async delete(options) {
        const result = await this._adapter.delete(options);
        return result;
    }

    async getStream(options) {
        const result = await this._adapter.getStream(options);
        return result;
    }

    async putStream(options) {
        const result = await this._adapter.putStream(options);
        return result;
    }
}

module.exports = StorageBase;
