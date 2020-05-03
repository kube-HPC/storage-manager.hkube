const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class StorageHkube extends StorageBase {
    constructor(adapter, log, config, encoding) {
        super(adapter, log, encoding);
        this.clusterName = config.clusterName;
    }

    async put({ jobId, taskId, data }, tracer) {
        return super.put({
            path: this.createPath({ jobId, taskId }),
            data,
        }, tracer);
    }

    async get({ jobId, taskId }, tracer) {
        return super.get({
            path: this.createPath({ jobId, taskId })
        }, tracer);
    }

    async list({ jobId }) {
        return super.list({
            path: this.createPath({ jobId })
        });
    }

    async delete({ jobId, taskId = '' }) {
        return super.delete({
            path: this.createPath({ jobId, taskId })
        });
    }

    createPath({ jobId, taskId = '' }) {
        return path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE, jobId, taskId);
    }
}

module.exports = StorageHkube;
