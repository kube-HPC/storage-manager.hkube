const pathLib = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class HkubeAlgoMetrics extends StorageBase {
    constructor(adapter, log, config) {
        super(adapter, log);
        this.adapter = adapter;
        this.clusterName = config.clusterName;
    }

    async put({ pipelineName, nodeName, jobId, taskId, fileName, data }, tracer) {// eslint-disable-line
        const path = await this._path({ pipelineName, nodeName, jobId, taskId, fileName });
        return super.put({
            path,
            data
        }, tracer);
    }

    async get({ pipelineName, nodeName, jobId, taskId, fileName }, tracer) {// eslint-disable-line
        const path = await this._path({ pipelineName, nodeName, jobId, taskId, fileName });
        return super.get({
            path
        }, tracer);
    }

    async list({ pipelineName, nodeName, jobId, taskId, fileName }) {// eslint-disable-line
        const path = await this._path({ pipelineName, nodeName, jobId, taskId, fileName });
        return super.list({
            path
        });
    }

    async delete({ pipelineName, nodeName, jobId, taskId, fileName }) {// eslint-disable-line
        const path = await this._path({ pipelineName, nodeName, jobId, taskId, fileName });
        return super.delete({
            path
        });
    }

    async getStream({ pipelineName, nodeName, jobId, taskId, fileName }) {// eslint-disable-line
        const path = await this._path({ pipelineName, nodeName, jobId, taskId, fileName });
        return super.getStream({
            path
        });
    }

    async putStream({ pipelineName, nodeName, jobId, taskId, formatedDate, fileName, data }) {// eslint-disable-line
        const path = await this._path({ pipelineName, nodeName, jobId, taskId, formatedDate, fileName });
        return super.putStream({
            path,
            data
        });
    }

    async getMetricsPath({ pipelineName, nodeName, jobId, taskId }) {
        const relativePath = await this._path({ pipelineName, nodeName, jobId, taskId });
        const absolutePath = this.adapter.getAbsolutePath(relativePath);
        return absolutePath;
    }

    async _path({ pipelineName, nodeName, jobId, taskId, formatedDate = '', fileName = '' }) {
        let shortTaskId = taskId && taskId.substring(taskId.length - 36);
        shortTaskId = shortTaskId || '';
        let shortJobId = jobId && jobId.substring(jobId.length - 36);
        shortJobId = shortJobId || '';
        shortTaskId = taskId || '';
        return pathLib.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_ALGO_METRICS, pipelineName, nodeName, shortJobId, shortTaskId, formatedDate, fileName);
    }
}

module.exports = HkubeAlgoMetrics;
