const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class StorageHkubeExecution extends StorageBase {
    async put({ jobId, data }) {
        return super.put({
            path: path.join(STORAGE_PREFIX.HKUBE_EXECUTION, jobId, 'descriptor.json'),
            data
        });
    }
    async get({ jobId }) {
        return super.get({
            path: path.join(STORAGE_PREFIX.HKUBE_EXECUTION, jobId, 'descriptor.json')
        });
    }
    async list({ jobId }) {
        return super.list({
            path: path.join(STORAGE_PREFIX.HKUBE_EXECUTION, jobId)
        });
    }
    async delete({ jobId }) {
        return super.delete({
            path: path.join(STORAGE_PREFIX.HKUBE_EXECUTION, jobId, 'descriptor.json')
        });
    }
}

module.exports = StorageHkubeExecution;
