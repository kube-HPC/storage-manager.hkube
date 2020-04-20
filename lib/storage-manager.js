const { Encoding } = require('@hkube/encoding');
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
            this.prefixesTypes = Object.values(STORAGE_PREFIX).map(x => `${config.clusterName}-${x}`);
            await this._adapter.init(storage.connection, this.prefixesTypes, bootstrap);
            this._wasInit = true;
            const encoding = new Encoding({ type: storage.encoding });
            this.storage = new StorageBase(this._adapter, log, encoding);
            this.hkube = new Hkube(this._adapter, log, config, encoding);
            this.hkubeResults = new HkubeResults(this._adapter, log, config, encoding);
            this.hkubeStore = new HkubeStore(this._adapter, log, config, encoding);
            this.hkubeMetadata = new HkubeMetadata(this._adapter, log, config, encoding);
            this.hkubeExecutions = new HkubeExecutions(this._adapter, log, config, encoding);
            this.hkubeIndex = new HkubeIndex(this._adapter, log, config, encoding);
            this.hkubeBuilds = new HkubeBuilds(this._adapter, log, config, encoding);
            this.hkubeAlgoMetrics = new HkubeAlgoMetrics(this._adapter, log, config, encoding);
        }
    }

    async put(options, tracer) {
        return this.storage.put(options, tracer);
    }

    async get(options, tracer, decodeOptions) {
        return this.storage.get(options, tracer, decodeOptions);
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
