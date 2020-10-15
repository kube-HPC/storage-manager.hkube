const { expect } = require('chai');
const uuid = require('uuid/v4');
const moment = require('moment');
const storageManager = require('../lib/storage-manager');

describe('index', () => {
    it('put and get', async () => {
        const jobId = uuid();
        await storageManager.hkubeIndex.put({ jobId });
        const res = await storageManager.hkubeIndex.get({ date: Date.now(), jobId });
        expect(res).to.be.not.undefined;
    });
    it('list', async () => {
        const jobId = uuid();
        await storageManager.hkubeIndex.put({ jobId });
        const res = await storageManager.hkubeIndex.list({ date: Date.now() });
        expect(res).to.be.not.undefined;
    });
    it('list without date', async () => {
        const jobId = uuid();
        await storageManager.hkubeIndex.put({ jobId });
        const res = await storageManager.hkubeIndex.list({});
        expect(res).to.be.not.undefined;
    });
    it('list by prefixes', async () => {
        await storageManager.hkubeIndex.put({ jobId: 'delimiter-test' });
        const res = await storageManager.hkubeIndex.listPrefixes();
        expect(res.includes(moment().format('YYYY-MM-DD'))).to.be.true;
    });
    it('delete', async () => {
        const jobId = uuid();
        await storageManager.hkubeIndex.put({ jobId });
        await storageManager.hkubeIndex.delete({ date: Date.now(), jobId });
        try {
            await storageManager.hkubeIndex.get({ date: Date.now(), jobId });
        }
        catch (err) {
            expect(err).to.be.instanceOf(Error);
            return;
        }
        expect.fail(null, null, 'did not reject with an error');
    });
    it('delete by prefix', async () => {
        const jobId = uuid();
        await storageManager.hkubeIndex.put({ jobId });
        await storageManager.hkubeIndex.delete({ date: Date.now() });
        const keys = await storageManager.hkubeIndex.list({ date: Date.now() });
        expect(keys.length).to.equal(0);
    });
});
