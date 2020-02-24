const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class HkubeBuilds extends StorageBase {
    constructor(adapter, log, config) {
        super(adapter, log);
        this.clusterName = config.clusterName;
    }

    async put({ buildId, data }, tracer) {
        return super.put({
            path: this.createPath({ buildId }),
            data
        }, tracer);
    }

    async get({ buildId }, tracer) {
        return super.get({
            path: this.createPath({ buildId })
        }, tracer);
    }

    async list({ buildId }) {
        return super.list({
            path: this.createPath({ buildId })
        });
    }

    async delete({ buildId }) {
        return super.delete({
            path: this.createPath({ buildId })
        });
    }

    async getStream({ buildId }) {
        return super.getStream({
            path: this.createPath({ buildId })
        });
    }

    async putStream({ buildId, data }) {
        return super.putStream({
            path: this.createPath({ buildId }),
            data
        });
    }

    createPath({ buildId }) {
        return path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_BUILDS, buildId);
    }
}

module.exports = HkubeBuilds;
