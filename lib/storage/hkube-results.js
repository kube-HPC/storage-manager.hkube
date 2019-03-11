const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');
const RESULT_JSON = 'result.json';

class StorageHkubeResults extends StorageBase {
    constructor(adapter, log, config) {
        super(adapter, log);
        this.clusterName = config.clusterName;
    }
    async put({ jobId, data }, tracer) {
        return super.put({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_RESULTS, jobId, RESULT_JSON),
            data
        }, tracer);
    }
    async get({ jobId }, tracer) {
        return super.get({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_RESULTS, jobId, RESULT_JSON)
        }, tracer);
    }
    async list({ jobId }) {
        return super.list({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_RESULTS, jobId)
        });
    }
    async delete({ jobId }) {
        return super.delete({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_RESULTS, jobId)
        });
    }
}

module.exports = StorageHkubeResults;
