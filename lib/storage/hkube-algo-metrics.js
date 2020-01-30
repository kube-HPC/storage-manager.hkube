const pathLib = require('path');
const StorageBase = require('./storage-base');
const { STORAGE_PREFIX } = require('../../consts/storage-prefix');

class HkubeAlgoMetrics extends StorageBase {
    constructor(adapter, log, config) {
        super(adapter, log);
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

    async delete({ jobId }) {// eslint-disable-line
        const pipelines = await this.listPipelines();
        await Promise.all(pipelines.map(async (pipelineName) => {
            const nodes = await this.listNodes(pipelineName);
            await Promise.all(nodes.map(async nodeName => {
                const path = await this._path({ pipelineName, nodeName, jobId });
                const files = await super.list({
                    path
                });
                await Promise.all(files.map(async (file) => {
                    return super.delete({ path: file.path });
                }));
            }));
        }));
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

    async listPipelines() {
        return super.listPrefixes({
            path: this.clusterName + '-' + STORAGE_PREFIX.HKUBE_ALGO_METRICS
        });
    }

    async listNodes(pipeLineName) {
        return super.listDir({
            path: pathLib.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_ALGO_METRICS, pipeLineName)
        });
    }

    async listJobs(pipeLineName, nodeName) {
        return super.listDir({
            path: pathLib.join(this.clusterName + '-' + STORAGE_PREFIX.HKUBE_ALGO_METRICS, pipeLineName, nodeName)
        });
    }

    async getMetricsPath({ pipelineName, nodeName, jobId, taskId }) {
        const relativePath = await this._path({ pipelineName, nodeName, jobId, taskId });
        const absolutePath = this._adapter.getAbsolutePath(relativePath);
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
