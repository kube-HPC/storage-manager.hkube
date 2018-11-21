const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class StorageHkubeStore extends StorageBase {
    async put({ type, name, data }) {
        return super.put({
            Path: path.join(STORAGE_PREFIX.HKUBE_STORE, type, `${name}.json`),
            Data: data,
        });
    }
    async get({ type, name }) {
        return super.get({
            Path: path.join(STORAGE_PREFIX.HKUBE_STORE, type, `${name}.json`)
        });
    }
    async list({ type }) {
        return super.list({
            Path: path.join(STORAGE_PREFIX.HKUBE_STORE, type)
        });
    }
    async delete({ type, name }) {
        return super.delete({
            Path: path.join(STORAGE_PREFIX.HKUBE_STORE, type, `${name}.json`)
        });
    }
}

module.exports = StorageHkubeStore;
