const path = require('path');
const moment = require('moment');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class StorageHkubeIndex extends StorageBase {
    async put({ jobId }) {
        return super.put({
            Path: path.join(STORAGE_PREFIX.HKUBE_INDEX, moment().format(super.DateFormat), jobId),
            Data: [],
        });
    }
    async get({ date, jobId }) {
        return super.get({
            Path: path.join(STORAGE_PREFIX.HKUBE_INDEX, moment(date).format(super.DateFormat), jobId)
        });
    }
    async list({ date }) {
        return super.list({
            Path: path.join(STORAGE_PREFIX.HKUBE_INDEX, moment(date).format(super.DateFormat))
        });
    }
    async delete({ date, jobId }) {
        return super.delete({
            Path: path.join(STORAGE_PREFIX.HKUBE_INDEX, moment(date).format(super.DateFormat), jobId)
        });
    }
}

module.exports = StorageHkubeIndex;
