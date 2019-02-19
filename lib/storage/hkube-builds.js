const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class HkubeBuilds extends StorageBase {
    async put({ buildId, data }, tracer) {
        return super.put({
            path: path.join(STORAGE_PREFIX.HKUBE_BUILDS, buildId),
            data
        }, tracer);
    }
    async get({ buildId }, tracer) {
        return super.get({
            path: path.join(STORAGE_PREFIX.HKUBE_BUILDS, buildId)
        }, tracer);
    }
    async list({ buildId }) {
        return super.list({
            path: path.join(STORAGE_PREFIX.HKUBE_BUILDS, buildId)
        });
    }
    async delete({ buildId }) {
        return super.delete({
            path: path.join(STORAGE_PREFIX.HKUBE_BUILDS, buildId)
        });
    }
    async getStream({ buildId }) {
        return super.getStream({
            path: path.join(STORAGE_PREFIX.HKUBE_BUILDS, buildId)
        });
    }
    async putStream({ buildId, data }) {
        return super.putStream({
            path: path.join(STORAGE_PREFIX.HKUBE_BUILDS, buildId),
            data
        });
    }
}

module.exports = HkubeBuilds;
