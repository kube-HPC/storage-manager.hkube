const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class StorageHkube extends StorageBase {
    async put({ jobId, taskId, data }) {
        return super.put({
            path: path.join(STORAGE_PREFIX.HKUBE, jobId, taskId),
            data
        });
    }
    async get({ jobId, taskId }) {
        return super.get({
            path: path.join(STORAGE_PREFIX.HKUBE, jobId, taskId)
        });
    }
    async list({ jobId }) {
        return super.list({
            path: path.join(STORAGE_PREFIX.HKUBE, jobId)
        });
    }
    async delete({ jobId, taskId = '' }) {
        return super.delete({
            path: path.join(STORAGE_PREFIX.HKUBE, jobId, taskId)
        });
    }
}

module.exports = StorageHkube;
