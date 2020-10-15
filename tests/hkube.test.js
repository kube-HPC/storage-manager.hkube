const { expect } = require('chai');
const uuid = require('uuid/v4');
const storageManager = require('../lib/storage-manager');

describe('hkube', () => {
    it('get and put string', async () => {
        const jobId = uuid();
        const taskId = uuid();
        const storageInfo = await storageManager.hkube.put({ jobId, taskId, data: 'gal-gadot' });
        const res1 = await storageManager.get(storageInfo);
        const res2 = await storageManager.hkube.get({ jobId, taskId });

        expect('gal-gadot').to.equal(res1);
        expect('gal-gadot').to.equal(res2);
    });
    it('get and put object', async () => {
        const jobId = uuid();
        const taskId = uuid();
        const storageInfo = await storageManager.hkube.put({ jobId, taskId, data: { test: 'gal-gadot' } });
        const res1 = await storageManager.get(storageInfo);
        const res2 = await storageManager.hkube.get({ jobId, taskId });
        expect({ test: 'gal-gadot' }).to.deep.equal(res1);
        expect({ test: 'gal-gadot' }).to.deep.equal(res2);
    });
    it('get and put array', async () => {
        const jobId = uuid();
        const taskId = uuid();
        const storageInfo = await storageManager.hkube.put({ jobId, taskId, data: [1, 2, 3] });
        const res1 = await storageManager.get(storageInfo);
        const res2 = await storageManager.hkube.get({ jobId, taskId });
        expect([1, 2, 3]).to.deep.equal(res1);
        expect([1, 2, 3]).to.deep.equal(res2);
    });
    it('list', async () => {
        const jobId = uuid();
        await storageManager.hkube.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.hkube.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.hkube.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.hkube.put({ jobId, taskId: uuid(), data: 'gal-gadot' });

        const res = await storageManager.hkube.list({ jobId });
        expect(res.length).to.equal(4);
    });
    it('delete', async () => {
        const jobId = uuid();
        const taskArray = [uuid(), uuid(), uuid(), uuid()];
        await storageManager.hkube.put({ jobId, taskId: taskArray[0], data: 'gal-gadot' });
        await storageManager.hkube.put({ jobId, taskId: taskArray[1], data: 'gal-gadot' });
        await storageManager.hkube.put({ jobId, taskId: taskArray[2], data: 'gal-gadot' });
        await storageManager.hkube.put({ jobId, taskId: taskArray[3], data: 'gal-gadot' });

        const keys = await storageManager.hkube.list({ jobId });
        expect(keys.length).to.equal(4);

        await storageManager.hkube.delete({ jobId, taskId: taskArray[0] });
        await storageManager.hkube.delete({ jobId, taskId: taskArray[1] });
        await storageManager.hkube.delete({ jobId, taskId: taskArray[2] });
        await storageManager.hkube.delete({ jobId, taskId: taskArray[3] });
        const keysAfter = await storageManager.hkube.list({ jobId });
        expect(keysAfter.length).to.equal(0);
    });
    it('delete by prefix', async () => {
        const jobId = uuid();
        const taskArray = [uuid(), uuid(), uuid(), uuid()];
        await storageManager.hkube.put({ jobId, taskId: taskArray[0], data: 'gal-gadot' });
        await storageManager.hkube.put({ jobId, taskId: taskArray[1], data: 'gal-gadot' });
        await storageManager.hkube.put({ jobId, taskId: taskArray[2], data: 'gal-gadot' });
        await storageManager.hkube.put({ jobId, taskId: taskArray[3], data: 'gal-gadot' });

        const keys = await storageManager.hkube.list({ jobId });
        expect(keys.length).to.equal(4);

        await storageManager.hkube.delete({ jobId });
        const keysAfter = await storageManager.hkube.list({ jobId });
        expect(keysAfter.length).to.equal(0);
    });
});
