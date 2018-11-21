const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class StorageHkube extends StorageBase {
    async put({ jobId, taskId, data }) {
        return super.put({
            Path: path.join(STORAGE_PREFIX.HKUBE, jobId, taskId),
            Data: data
        });
    }
    async get({ jobId, taskId }) {
        return super.get({
            Path: path.join(STORAGE_PREFIX.HKUBE, jobId, taskId)
        });
    }
    async list({ jobId }) {
        return super.list({
            Path: path.join(STORAGE_PREFIX.HKUBE, jobId)
        });
    }
    async delete({ jobId, taskId }) {
        return super.delete({
            Path: path.join(STORAGE_PREFIX.HKUBE, jobId, taskId)
        });
    }
}

module.exports = StorageHkube;
