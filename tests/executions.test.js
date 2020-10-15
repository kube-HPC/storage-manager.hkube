const { expect } = require('chai');
const uuid = require('uuid/v4');
const storageManager = require('../lib/storage-manager');

describe('executions', () => {
    it('get and put string', async () => {
        const jobId = uuid();
        const storageInfo = await storageManager.hkubeExecutions.put({ jobId, data: 'gal-gadot' });
        const res1 = await storageManager.get(storageInfo);
        const res2 = await storageManager.hkubeExecutions.get({ jobId });
        expect('gal-gadot').to.equal(res1);
        expect('gal-gadot').to.equal(res2);
    });
    it('get and put object', async () => {
        const jobId = uuid();
        const storageInfo = await storageManager.hkubeExecutions.put({ jobId, data: { test: 'gal-gadot' } });
        const res1 = await storageManager.get(storageInfo);
        const res2 = await storageManager.hkubeExecutions.get({ jobId });
        expect({ test: 'gal-gadot' }).to.deep.equal(res1);
        expect({ test: 'gal-gadot' }).to.deep.equal(res2);
    });
    it('get and put array', async () => {
        const jobId = uuid();
        const storageInfo = await storageManager.hkubeExecutions.put({ jobId, data: [1, 2, 3] });
        const res1 = await storageManager.get(storageInfo);
        const res2 = await storageManager.hkubeExecutions.get({ jobId });
        expect([1, 2, 3]).to.deep.equal(res1);
        expect([1, 2, 3]).to.deep.equal(res2);
    });
    it('list', async () => {
        const jobId = uuid();
        await storageManager.hkubeExecutions.put({ jobId, data: [1, 2, 3] });
        const res = await storageManager.hkubeExecutions.list({ jobId });
        expect(res.length).to.equal(1);
    });
    it('delete', async () => {
        const jobId = uuid();
        await storageManager.hkubeExecutions.put({ jobId, data: 'gal-gadot' });
        const keys = await storageManager.hkubeExecutions.list({ jobId });
        expect(keys.length).to.equal(1);
        await storageManager.hkubeExecutions.delete({ jobId });
        const keysAfter = await storageManager.hkubeExecutions.list({ jobId });
        expect(keysAfter.length).to.equal(0);
    });
});
