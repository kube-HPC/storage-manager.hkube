const { Readable } = require('stream');
const { Encoding } = require('@hkube/encoding');
const prettyBytes = require('pretty-bytes');
const unitsConverter = require('@hkube/units-converter');
const { STORAGE_PREFIX } = require('../consts/storage-prefix');
const StorageBase = require('./storage/storage-base');
const Hkube = require('./storage/hkube');
const HkubeIndex = require('./storage/hkube-index');
const HkubeResults = require('./storage/hkube-results');
const HkubeStore = require('./storage/hkube-store');
const HkubeMetadata = require('./storage/hkube-metadata');
const HkubeExecutions = require('./storage/hkube-execution');
const HkubeBuilds = require('./storage/hkube-builds');
const HkubeAlgoMetrics = require('./storage/hkube-algo-metrics');
const DataSource = require('./storage/hkube-datasource');

class StorageManager {
    constructor() {
        this._wasInit = false;
    }

    async init(config, log, bootstrap = false) {
        if (!this._wasInit) {
            const storage = config.storageAdapters[config.defaultStorage];
            this._info = {
                type: config.defaultStorage,
                encoding: storage.encoding,
                clusterName: config.clusterName,
                moduleName: storage.moduleName,
                storageResultsThreshold: config.storageResultsThreshold
            };
            this.moduleName = storage.moduleName;
            this._adapterPackage = require(storage.moduleName);  // eslint-disable-line
            this._adapter = new this._adapterPackage();
            this._storageResultsThreshold = config.storageResultsThreshold && unitsConverter.getMemoryInBytes(config.storageResultsThreshold);
            this.prefixesTypes = Object.values(STORAGE_PREFIX).map(x => `${config.clusterName}-${x}`);
            await this._adapter.init(storage.connection, this.prefixesTypes, bootstrap);
            this._wasInit = true;
            this.encoding = new Encoding({ type: storage.encoding });
            this.storage = new StorageBase(this._adapter, log, this.encoding);
            this.hkube = new Hkube(this._adapter, log, config, this.encoding);
            this.hkubeResults = new HkubeResults(this._adapter, log, config, this.encoding);
            this.hkubeStore = new HkubeStore(this._adapter, log, config, this.encoding);
            this.hkubeMetadata = new HkubeMetadata(this._adapter, log, config, this.encoding);
            this.hkubeExecutions = new HkubeExecutions(this._adapter, log, config, this.encoding);
            this.hkubeIndex = new HkubeIndex(this._adapter, log, config, this.encoding);
            this.hkubeBuilds = new HkubeBuilds(this._adapter, log, config, this.encoding);
            this.hkubeAlgoMetrics = new HkubeAlgoMetrics(this._adapter, log, config, this.encoding);
            this.hkubeDataSource = new DataSource(this._adapter, log, config, this.encoding);
        }
    }

    getInfo() {
        return this._info;
    }

    checkDataSize(size) {
        const result = {};
        if (size >= this._storageResultsThreshold) {
            result.error = `data too large (${prettyBytes(size)}), use the stream api`;
        }
        return result;
    }

    async getCustomStream(options) {
        const { path } = options;
        const { size: totalLength, metadata, headerInData } = await this.getMetadata({ path });
        const { header } = metadata || {};
        const { headerLength } = this.encoding;
        let stream;

        if (!header) {
            stream = await this.getStream({ path, totalLength });
        }
        else {
            const { isCustomEncoding, isDataTypeEncoded } = this._readHeader(header);
            if (isCustomEncoding) { // check if hkube encoding is here
                const start = headerInData ? headerLength : 0;
                const end = totalLength;
                if (isDataTypeEncoded) { // check if this data is encoded
                    const result = this.checkDataSize(totalLength);
                    if (result.error) {
                        // currently we are not supporting large decoding
                        stream = await this.getStream({ path, start, end, totalLength });
                    }
                    else {
                        const { payload } = await this.getCustomData(options);
                        stream = new Readable();
                        stream.push(JSON.stringify(payload));
                        stream.push(null);
                    }
                }
                else {
                    stream = await this.getStream({ path, start, end, totalLength }); // this data is not encoded, we should stream it (without header)
                }
            }
            else {
                stream = await this.getStream({ path, totalLength });
            }
        }
        return stream;
    }

    _readHeader(header) {
        const { magicNumber } = this.encoding;
        const magicNum = header.slice(header.length - magicNumber.length, header.length).toString();
        const isCustomEncoding = magicNum === magicNumber;
        let isDataTypeEncoded = false;

        if (isCustomEncoding) {
            isDataTypeEncoded = this.encoding.isDataTypeEncoded(header[2]);
        }
        return { isCustomEncoding, isDataTypeEncoded };
    }

    async _createBytesRange(options) {
        const { start, end, totalLength } = options;
        if (!start && !end) {
            return options;
        }
        if (start < 0 || (start > 0 && end === 0)) {
            throw new Error('invalid range');
        }
        let from = start || 0;
        let to = end || 0;
        const absEnd = Math.abs(to);
        let totalSize;

        if (totalLength == null) {
            const stats = await this.getMetadata(options);
            totalSize = stats.size;
        }

        if (start > totalSize || absEnd > totalSize) {
            throw new Error('invalid range');
        }
        if (start === 0 && end < 0) {
            to = totalSize - absEnd;
        }
        else if (start == null && end < 0) {
            from = totalSize - absEnd;
            to = totalSize;
        }
        else if (start >= 0 && end == null) {
            from = start;
            to = totalSize;
        }
        return { start: from, end: to };
    }

    async put(options, tracer) {
        return this.storage.put(options, tracer);
    }

    async get(options, tracer) {
        return this.storage.get(options, tracer);
    }

    async getCustomData(options) {
        return this.storage.getCustomData(options);
    }

    async getMetadata(options, tracer) {
        return this.storage.getMetadata(options, tracer);
    }

    async seek(options, tracer) {
        const { start, end } = await this._createBytesRange(options);
        return this.storage.seek({ ...options, start, end }, tracer);
    }

    async list(options) {
        return this.storage.list(options);
    }

    async delete(options) {
        return this.storage.delete(options);
    }

    async getStream(options) {
        const { start, end } = await this._createBytesRange(options);
        return this.storage.getStream({ ...options, start, end });
    }

    async putStream(options) {
        return this.storage.putStream(options);
    }
}

module.exports = new StorageManager();
module.exports.StorageManager = StorageManager;
