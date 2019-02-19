const { STORAGE_PREFIX } = require('../consts/storage-prefix');
const StorageBase = require('./storage/storage-base');
const Hkube = require('./storage/hkube');
const HkubeIndex = require('./storage/hkube-index');
const HkubeResults = require('./storage/hkube-results');
const HkubeStore = require('./storage/hkube-store');
const HkubeMetadata = require('./storage/hkube-metadata');
const HkubeExecutions = require('./storage/hkube-execution');
const HkubeBuilds = require('./storage/hkube-builds');

class StorageManager {
    constructor() {
        this._wasInit = false;
    }

    async init(config, bootstrap = false) {
        if (!this._wasInit) {
            const storage = config.storageAdapters[config.defaultStorage];
            this.moduleName = storage.moduleName;
            this._adapter = require(storage.moduleName);  // eslint-disable-line
            await this._adapter.init(storage.connection, STORAGE_PREFIX, bootstrap);
            this._wasInit = true;
        }
        this.storage = new StorageBase(this._adapter);
        this.hkube = new Hkube(this._adapter);
        this.hkubeResults = new HkubeResults(this._adapter);
        this.hkubeStore = new HkubeStore(this._adapter);
        this.hkubeMetadata = new HkubeMetadata(this._adapter);
        this.hkubeExecutions = new HkubeExecutions(this._adapter);
        this.hkubeIndex = new HkubeIndex(this._adapter);
        this.hkubeBuilds = new HkubeBuilds(this._adapter);
    }

    async put(options) {
        return this.storage.put(options);
    }

    async get(options, tracerStart) {
        return this.storage.get(options, tracerStart);
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
