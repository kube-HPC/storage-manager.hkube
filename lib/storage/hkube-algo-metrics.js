const path = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class HkubeAlgoMetrics extends StorageBase {
    constructor(adapter, log, config) {
        super(adapter, log);
        this.clusterName = config.clusterName;
    }

    async put({ pipelineName, nodeName, runName, fileName, data }, tracer) {// eslint-disable-line
        return super.put({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_ALGO_METRICS, pipelineName, nodeName, runName, fileName),
            data
        }, tracer);
    }

    async get({ pipelineName, nodeName, runName, fileName }, tracer) {// eslint-disable-line
        return super.get({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_ALGO_METRICS, pipelineName, nodeName, runName, fileName)
        }, tracer);
    }

    async list({ pipelineName, nodeName, runName, fileName }) {// eslint-disable-line
        return super.list({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_ALGO_METRICS, pipelineName, nodeName, runName, fileName)
        });
    }

    async delete({ pipelineName, nodeName, runName, fileName }) {// eslint-disable-line
        return super.delete({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_ALGO_METRICS, pipelineName, nodeName, runName, fileName)
        });
    }

    async getStream({ pipelineName, nodeName, runName, fileName }) {// eslint-disable-line
        return super.getStream({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_ALGO_METRICS, pipelineName, nodeName, runName, fileName)
        });
    }

    async putStream({ pipelineName, nodeName, runName, fileName, data }) {// eslint-disable-line
        return super.putStream({
            path: path.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_ALGO_METRICS, pipelineName, nodeName, runName, fileName),
            data
        });
    }
}

module.exports = HkubeAlgoMetrics;
