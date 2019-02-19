const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class StorageHkubeStore extends StorageBase {
    async put({ type, name, data }, tracer) {
        return super.put({
            path: path.join(STORAGE_PREFIX.HKUBE_STORE, type, `${name}.json`),
            data,
        }, tracer);
    }
    async get({ type, name }, tracer) {
        return super.get({
            path: path.join(STORAGE_PREFIX.HKUBE_STORE, type, `${name}.json`)
        }, tracer);
    }
    async list({ type }) {
        return super.list({
            path: path.join(STORAGE_PREFIX.HKUBE_STORE, type)
        });
    }
    async delete({ type, name = '' }) {
        return super.delete({
            path: path.join(STORAGE_PREFIX.HKUBE_STORE, type, `${name}.json`)
        });
    }
}

module.exports = StorageHkubeStore;
