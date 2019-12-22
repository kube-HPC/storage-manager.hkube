const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class StorageHkube extends StorageBase {
    constructor(adapter, log, config) {
        super(adapter, log);
        this.clusterName = config.clusterName;
    }

    async put({ jobId, taskId, data }, tracer) {
        return super.put({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE, jobId, taskId),
            data
        }, tracer);
    }

    async get({ jobId, taskId }, tracer) {
        return super.get({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE, jobId, taskId)
        }, tracer);
    }

    async list({ jobId }) {
        return super.list({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE, jobId)
        });
    }

    async delete({ jobId, taskId = '' }) {
        return super.delete({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE, jobId, taskId)
        });
    }
}

module.exports = StorageHkube;
