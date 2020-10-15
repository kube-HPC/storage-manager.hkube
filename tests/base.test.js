const { expect } = require('chai');
const uuid = require('uuid/v4');
const path = require('path');
const config = require('./config');
const storageManager = require('../lib/storage-manager');

describe('base', () => {
    it('get and put string', async () => {
        const storageInfo = await storageManager.put({
            path: path.join(config.clusterName + '-hkube/', uuid(), uuid()),
            data: 'gal-gadot'
        });
        const res = await storageManager.get(storageInfo);
        expect('gal-gadot').to.equal(res);
    });
    it('get and put object', async () => {
        const storageInfo = await storageManager.put({
            path: path.join(config.clusterName + '-hkube/', uuid(), uuid()),
            data: { test: 'gal-gadot' }
        });
        const res = await storageManager.get(storageInfo);
        expect({ test: 'gal-gadot' }).to.deep.equal(res);
    });
    it('get and put array', async () => {
        const storageInfo = await storageManager.put({
            path: path.join(config.clusterName + '-hkube/', uuid(), uuid()),
            data: [1, 2, 3]
        });
        const res = await storageManager.get(storageInfo);
        expect([1, 2, 3]).to.deep.equal(res);
    });
    it('list', async () => {
        const jobId = uuid();
        await storageManager.put({ path: path.join(config.clusterName + '-hkube/', jobId, uuid()), data: 'gal-gadot' });
        await storageManager.put({ path: path.join(config.clusterName + '-hkube/', jobId, uuid()), data: 'gal-gadot' });
        await storageManager.put({ path: path.join(config.clusterName + '-hkube/', jobId, uuid()), data: 'gal-gadot' });
        await storageManager.put({ path: path.join(config.clusterName + '-hkube/', jobId, uuid()), data: 'gal-gadot' });

        const res = await storageManager.list({ path: path.join(config.clusterName + '-hkube', jobId) });
        expect(res.length).to.equal(4);
    });
    it('delete', async () => {
        const jobId = uuid();
        await storageManager.put({ path: path.join(config.clusterName + '-hkube/', jobId, uuid()), data: 'gal-gadot' });
        await storageManager.put({ path: path.join(config.clusterName + '-hkube/', jobId, uuid()), data: 'gal-gadot' });
        await storageManager.put({ path: path.join(config.clusterName + '-hkube/', jobId, uuid()), data: 'gal-gadot' });
        await storageManager.put({ path: path.join(config.clusterName + '-hkube/', jobId, uuid()), data: 'gal-gadot' });

        const keys = await storageManager.list({ path: path.join(config.clusterName + '-hkube/', jobId) });
        const promiessArray = [];
        keys.forEach((key) => {
            promiessArray.push(storageManager.delete(key));
        });
        await Promise.all(promiessArray);
        const keysAfter = await storageManager.list({ path: path.join(config.clusterName + '-hkube/', jobId) });
        expect(keysAfter.length).to.equal(0);
    });
    it('delete by prefix', async () => {
        const jobId = uuid();
        await storageManager.put({ path: path.join(config.clusterName + '-hkube/', jobId, uuid()), data: 'gal-gadot' });
        await storageManager.put({ path: path.join(config.clusterName + '-hkube/', jobId, uuid()), data: 'gal-gadot' });
        await storageManager.put({ path: path.join(config.clusterName + '-hkube/', jobId, uuid()), data: 'gal-gadot' });
        await storageManager.put({ path: path.join(config.clusterName + '-hkube/', jobId, uuid()), data: 'gal-gadot' });
        await storageManager.delete({ path: path.join(config.clusterName + '-hkube/', jobId) });

        const keys = await storageManager.list({ path: path.join(config.clusterName + '-hkube/', jobId) });
        expect(keys.length).to.equal(0);
    });
});

