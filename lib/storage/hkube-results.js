const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');
const RESULT_JSON = 'result.json';

class StorageHkubeResults extends StorageBase {
    async put({ jobId, data }) {
        return super.put({
            path: path.join(STORAGE_PREFIX.HKUBE_RESULTS, jobId, RESULT_JSON),
            data
        });
    }
    async get({ jobId }) {
        return super.get({
            path: path.join(STORAGE_PREFIX.HKUBE_RESULTS, jobId, RESULT_JSON)
        });
    }
    async list({ jobId }) {
        return super.list({
            path: path.join(STORAGE_PREFIX.HKUBE_RESULTS, jobId)
        });
    }
    async delete({ jobId }) {
        return super.delete({
            path: path.join(STORAGE_PREFIX.HKUBE_RESULTS, jobId)
        });
    }
}

module.exports = StorageHkubeResults;
