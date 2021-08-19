const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class Persistency extends StorageBase {
    constructor(adapter, log, config, encoding) {
        super(adapter, log, encoding);
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

    createPath({ type, name = '' }) {
        return path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_PERSISTENCY, type, name);
    }
}

module.exports = Persistency;
