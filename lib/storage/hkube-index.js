const path = require('path');
const moment = require('moment');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class StorageHkubeIndex extends StorageBase {
    constructor(adapter, log, config, encoding) {
        super(adapter, log, encoding);
        this.clusterName = config.clusterName;
    }

    async put({ jobId }, tracer) {
        return super.put({
            path: this.createPath({ jobId }),
            data: [],
        }, tracer);
    }

    async get({ date, jobId }, tracer) {
        return super.get({
            path: this.createPath({ date, jobId })
        }, tracer);
    }

    async list({ date }) {
        if (date) {
            return super.list({
                path: this.createPath({ date })
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
            path: this.createPath({ date, jobId })
        });
    }

    createPath({ date, jobId = '' }) {
        const dateFormat = moment(date).format(super.DateFormat);
        return path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_INDEX, dateFormat, jobId);
    }
}

module.exports = StorageHkubeIndex;
