const path = require('path');
const moment = require('moment');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class StorageHkubeIndex extends StorageBase {
    constructor(adapter, log, config) {
        super(adapter, log);
        this.clusterName = config.clusterName;
    }
    async put({ jobId }, tracer) {
        return super.put({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_INDEX, moment().format(super.DateFormat), jobId),
            data: [],
        }, tracer);
    }
    async get({ date, jobId }, tracer) {
        return super.get({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_INDEX, moment(date).format(super.DateFormat), jobId)
        }, tracer);
    }
    async list({ date }) {
        if (date) {
            return super.list({
                path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_INDEX, moment(date).format(super.DateFormat))
            });
        }
        return super.list({ path: this.clusterName + '-' + STORAGE_PREFIX.HKUBE_INDEX });
    }

    async listPrefixes() {
        return super.listPrefixes({
            path: this.clusterName + '-' + STORAGE_PREFIX.HKUBE_INDEX
        });
    }

    async delete({ date, jobId = '' }) {
        return super.delete({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_INDEX, moment(date).format(super.DateFormat), jobId)
        });
    }
}

module.exports = StorageHkubeIndex;
