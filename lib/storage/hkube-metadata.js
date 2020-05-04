const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class StorageHkubeMetadata extends StorageBase {
    constructor(adapter, log, config, encoding) {
        super(adapter, log, encoding);
        this.clusterName = config.clusterName;
    }
    async put({ jobId, taskId, data }, tracer) { // eslint-disable-line
        return super.put({
            path: this.createPath({ jobId, key: taskId }),
            data,
        }, tracer);
    }

    async get({ jobId, taskId }, tracer) {
        return super.get({
            path: this.createPath({ jobId, key: taskId }),
        }, tracer);
    }

    async list({ jobId, nodeName = '' }) {
        return super.list({
            path: this.createPath({ jobId, key: nodeName }),
        });
    }

    async delete({ jobId, taskId = '' }) {
        return super.delete({
            path: this.createPath({ jobId, key: taskId }),
        });
    }

    createPath({ jobId, key }) {
        return path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_METADATA, jobId, key);
    }
}

module.exports = StorageHkubeMetadata;
