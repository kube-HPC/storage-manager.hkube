const { expect } = require('chai');
const storageManager = require('../lib/storage-manager');

describe('metrics', () => {
    it('get pipelines and nodes', async () => {
        for (let i = 0; i < 2; i++) {
            for (let j = 0; j <= 2; j++) {
                const jobId = 'job' + i;
                const pipelineName = 'pl' + i;
                const nodeName = 'nn' + j;
                const taskId = 'task' + j;
                const fileName = 'fN';
                const data = { test: 'test' + j };
                await storageManager.hkubeAlgoMetrics.put({ jobId, taskId, pipelineName, nodeName, fileName, data })
            }
        }
        const res1 = await storageManager.hkubeAlgoMetrics.listPipelines();
        expect(res1.length).to.equal(2);
        expect(res1).to.contain('pl1');
        const res2 = await storageManager.hkubeAlgoMetrics.listNodes(res1[1]);
        expect(res2.length).to.equal(3);
        expect(res2).to.contain('nn1');

    });
});