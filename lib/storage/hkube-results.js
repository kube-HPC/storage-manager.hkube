const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');
const RESULT_JSON = 'result.json';

class StorageHkubeResults extends StorageBase {
    constructor(adapter, log, config, encoding) {
        super(adapter, log, encoding);
        this.clusterName = config.clusterName;
    }

    async put({ jobId, data }, tracer) {
        return super.put({
            path: this.createPath({ jobId, name: RESULT_JSON }),
            data
        }, tracer);
    }

    async get({ jobId }, tracer) {
        return super.get({
            path: this.createPath({ jobId, name: RESULT_JSON })
        }, tracer);
    }

    async list({ jobId }) {
        return super.list({
            path: this.createPath({ jobId })
        });
    }

    async delete({ jobId }) {
        return super.delete({
            path: this.createPath({ jobId })
        });
    }

    createPath({ jobId, name = '' }) {
        return path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_RESULTS, jobId, name);
    }
}

module.exports = StorageHkubeResults;
