const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class StorageHkubeMetadata extends StorageBase {
    constructor(adapter, log, config) {
        super(adapter, log);
        this.clusterName = config.clusterName;
    }
    async put({ jobId, taskId, data }, tracer) { // eslint-disable-line
        return super.put({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_METADATA, jobId, taskId),
            data,
        }, tracer);
    }

    async get({ jobId, taskId }, tracer) {
        return super.get({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_METADATA, jobId, taskId)
        }, tracer);
    }

    async list({ jobId, nodeName = '' }) {
        return super.list({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_METADATA, jobId, nodeName)
        });
    }

    async delete({ jobId, taskId = '' }) {
        return super.delete({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_METADATA, jobId, taskId)
        });
    }
}

module.exports = StorageHkubeMetadata;
