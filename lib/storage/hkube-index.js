const path = require('path');
const moment = require('moment');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class StorageHkubeIndex extends StorageBase {
    async put({ jobId }, tracer) {
        return super.put({
            path: path.join(STORAGE_PREFIX.HKUBE_INDEX, moment().format(super.DateFormat), jobId),
            data: [],
        }, tracer);
    }
    async get({ date, jobId }, tracer) {
        return super.get({
            path: path.join(STORAGE_PREFIX.HKUBE_INDEX, moment(date).format(super.DateFormat), jobId)
        }, tracer);
    }
    async list({ date }) {
        if (date) {
            return super.list({
                path: path.join(STORAGE_PREFIX.HKUBE_INDEX, moment(date).format(super.DateFormat))
            });
        }
        return super.list({ path: STORAGE_PREFIX.HKUBE_INDEX });
    }

    async listPrefixes() {
        return super.listPrefixes({
            path: STORAGE_PREFIX.HKUBE_INDEX
        });
    }

    async delete({ date, jobId = '' }) {
        return super.delete({
            path: path.join(STORAGE_PREFIX.HKUBE_INDEX, moment(date).format(super.DateFormat), jobId)
        });
    }
}

module.exports = StorageHkubeIndex;
