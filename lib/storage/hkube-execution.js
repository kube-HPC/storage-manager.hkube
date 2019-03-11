const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');
const DESCRIPTOR_JSON = 'descriptor.json';

class StorageHkubeExecution extends StorageBase {
    constructor(adapter, log, config) {
        super(adapter, log);
        this.clusterName = config.clusterName;
    }
    async put({ jobId, data }, tracer) {
        return super.put({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_EXECUTION, jobId, DESCRIPTOR_JSON),
            data
        }, tracer);
    }
    async get({ jobId }, tracer) {
        return super.get({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_EXECUTION, jobId, DESCRIPTOR_JSON)
        }, tracer);
    }
    async list({ jobId }) {
        return super.list({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_EXECUTION, jobId)
        });
    }
    async delete({ jobId }) {
        return super.delete({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_EXECUTION, jobId)
        });
    }
}

module.exports = StorageHkubeExecution;
