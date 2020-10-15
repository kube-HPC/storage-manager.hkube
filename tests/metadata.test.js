const { expect } = require('chai');
const uuid = require('uuid/v4');
const storageManager = require('../lib/storage-manager');

describe('metadata', () => {
    it('get and put string', async () => {
        const jobId = uuid();
        const taskId = uuid();
        const storageInfo = await storageManager.hkubeMetadata.put({ jobId, taskId, data: 'gal-gadot' });
        const res1 = await storageManager.get(storageInfo);
        const res2 = await storageManager.hkubeMetadata.get({ jobId, taskId });
        expect('gal-gadot').to.equal(res1);
        expect('gal-gadot').to.equal(res2);
    });
    it('get and put object', async () => {
        const jobId = uuid();
        const taskId = uuid();
        const storageInfo = await storageManager.hkubeMetadata.put({ jobId, taskId, data: { test: 'gal-gadot' } });
        const res1 = await storageManager.get(storageInfo);
        const res2 = await storageManager.hkubeMetadata.get({ jobId, taskId });
        expect({ test: 'gal-gadot' }).to.deep.equal(res1);
        expect({ test: 'gal-gadot' }).to.deep.equal(res2);
    });
    it('get and put array', async () => {
        const jobId = uuid();
        const taskId = uuid();
        const storageInfo = await storageManager.hkubeMetadata.put({ jobId, taskId, data: [1, 2, 3] });
        const res1 = await storageManager.get(storageInfo);
        const res2 = await storageManager.hkubeMetadata.get({ jobId, taskId });
        expect([1, 2, 3]).to.deep.equal(res1);
        expect([1, 2, 3]).to.deep.equal(res2);
    });
    it('list', async () => {
        const jobId = uuid();
        await storageManager.hkubeMetadata.put({ jobId, taskId: 'green' + uuid(), data: [1, 2, 3] });
        await storageManager.hkubeMetadata.put({ jobId, taskId: 'green' + uuid(), data: [4, 5, 6] });
        await storageManager.hkubeMetadata.put({ jobId, taskId: 'green' + uuid(), data: [7, 8, 9] });
        await storageManager.hkubeMetadata.put({ jobId, taskId: 'green' + uuid(), data: [10, 11, 12] });
        const res = await storageManager.hkubeMetadata.list({ jobId, nodeName: 'green' });
        expect(res.length).to.equal(4);
    });
    it('delete', async () => {
        const jobId = uuid();
        const taskId = uuid();
        await storageManager.hkubeMetadata.put({ jobId, taskId: 'green' + taskId, data: 'gal-gadot' });
        const keys = await storageManager.hkubeMetadata.list({ jobId, nodeName: 'green' });
        expect(keys.length).to.equal(1);
        await storageManager.hkubeMetadata.delete({ jobId, taskId: 'green' + taskId });
        const keysAfter = await storageManager.hkubeMetadata.list({ jobId, nodeName: 'green' });
        expect(keysAfter.length).to.equal(0);
    });
    it('delete by prefix', async () => {
        const jobId = uuid();
        const taskId = uuid();
        await storageManager.hkubeMetadata.put({ jobId, taskId: 'green' + taskId, data: 'gal-gadot' });
        const keys = await storageManager.hkubeMetadata.list({ jobId, nodeName: 'green' });
        expect(keys.length).to.equal(1);
        await storageManager.hkubeMetadata.delete({ jobId });
        const keysAfter = await storageManager.hkubeMetadata.list({ jobId });
        expect(keysAfter.length).to.equal(0);
    });
});
