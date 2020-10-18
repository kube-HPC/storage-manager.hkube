const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class DataSource extends StorageBase {
    constructor(adapter, log, config, encoding) {
        super(adapter, log, encoding);
        this.clusterName = config.clusterName;
    }

    async getStream({ dataSource, fileName }) {
        return super.getStream({
            path: this.createPath({ dataSource, fileName })
        });
    }

    async putStream({ dataSource, data, fileName }) {
        return super.putStream({
            path: this.createPath({ dataSource, fileName }),
            data
        });
    }

    async list({ dataSource }) {
        return super.list({
            path: this.createPath({ dataSource })
        });
    }

    async delete({ dataSource }) {
        return super.delete({
            path: this.createPath({ dataSource })
        });
    }

    createPath({ dataSource, fileName = '' }) {
        return path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_DATASOURCE, dataSource, fileName);
    }
}

module.exports = DataSource;
