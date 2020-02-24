const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class StorageHkubeStore extends StorageBase {
    constructor(adapter, log, config) {
        super(adapter, log);
        this.clusterName = config.clusterName;
    }

    async put({ type, name, data }, tracer) {
        return super.put({
            path: this.createPath({ type, name }),
            data,
        }, tracer);
    }

    async get({ type, name }, tracer) {
        return super.get({
            path: this.createPath({ type, name })
        }, tracer);
    }

    async list({ type }) {
        return super.list({
            path: this.createPath({ type })
        });
    }

    async delete({ type, name }) {
        return super.delete({
            path: this.createPath({ type, name })
        });
    }

    createPath({ type, name }) {
        const newName = (name && `${name}.json`) || '';
        return path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_STORE, type, newName);
    }
}

module.exports = StorageHkubeStore;
