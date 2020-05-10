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

class StorageManager {
    constructor() {
        this._wasInit = false;
    }

    async init(config, log, bootstrap = false) {
        if (!this._wasInit) {
            const storage = config.storageAdapters[config.defaultStorage];
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
            return { message: `initialized ${config.defaultStorage} storage client with ${storage.encoding} encoding` };
        }
        return { message: 'storage client not initialized' };
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
        const metadata = await this.getMetadata({ path });
        const totalLength = metadata.size;
        const { headerLength } = this.encoding;
        let stream;

        if (totalLength <= headerLength) {
            stream = await this.getStream({ path });
        }
        else {
            const { isCustomFormat, isDataTypeEncoded } = await this._readFormat({ path });
            if (isCustomFormat) { // check if hkube encoding is here
                if (isDataTypeEncoded) { // check if this data is encoded
                    const result = this.checkDataSize(totalLength);
                    if (result.error) {
                        // currently we are not supporting huge decoding
                        stream = await this.getStream({ path, start: headerLength, end: totalLength - 1 });
                    }
                    else {
                        const data = await this.get({ ...options, encodeOptions: { customEncode: true } });
                        stream = new Readable();
                        stream.push(JSON.stringify(data));
                        stream.push(null);
                    }
                }
                else {
                    stream = await this.getStream({ path, start: headerLength, end: totalLength - 1 }); // this data is not encoded, we should stream it (without footer)
                }
            }
            else {
                stream = await this.getStream({ path });
            }
        }
        return stream;
    }

    async _readFormat(options) {
        const { path } = options;
        const { headerLength, magicNumber } = this.encoding;
        const header = await this.seek({ path, start: 0, end: headerLength }); // read first bytes to get header
        const magicNum = header.slice(header.length - magicNumber.length, header.length).toString();
        const isCustomFormat = magicNum === magicNumber;
        let isDataTypeEncoded = false;

        if (isCustomFormat) {
            isDataTypeEncoded = this.encoding.isDataTypeEncoded(header[2]);
        }
        return { isCustomFormat, isDataTypeEncoded };
    }

    async put(options, tracer) {
        return this.storage.put(options, tracer);
    }

    async get(options, tracer) {
        return this.storage.get(options, tracer);
    }

    async getMetadata(options, tracer) {
        return this.storage.getMetadata(options, tracer);
    }

    async seek(options, tracer) {
        return this.storage.seek(options, tracer);
    }

    async list(options) {
        return this.storage.list(options);
    }

    async delete(options) {
        return this.storage.delete(options);
    }

    async getStream(options) {
        return this.storage.getStream(options);
    }

    async putStream(options) {
        return this.storage.putStream(options);
    }
}

module.exports = new StorageManager();
module.exports.StorageManager = StorageManager;
