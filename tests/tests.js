const { expect } = require('chai');
const uuid = require('uuid/v4');
const path = require('path');
const mockery = require('mockery');
const moment = require('moment');
const adapters = ['s3', 'fs', 'redis', 'etcd'];
const config = require('./config'); // eslint-disable-line
const fs = require('fs-extra');
const { STORAGE_PREFIX } = require('../consts/storage-prefix');
const baseDir = '';
let storageManager;

describe('storage-manager tests', () => {
    adapters.forEach((adapter) => {
        describe('put get delete list', () => {
            before(async () => {
                mockery.enable({
                    warnOnReplace: false,
                    warnOnUnregistered: false,
                    useCleanCache: true
                });
                mockery.resetCache();
                storageManager = require('../lib/storage-manager'); // eslint-disable-line
                config.defaultStorage = adapter;
                await storageManager.init(config, null, true);
            });
            describe(adapter + ':base', () => {
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
            describe(adapter + ':hkube-index', () => {
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
            describe(adapter + ':hkube', () => {
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
            describe(adapter + ':hkube-results', () => {
                it('get and put string', async () => {
                    const jobId = uuid();
                    const storageInfo = await storageManager.hkubeResults.put({ jobId, data: 'gal-gadot' });
                    const res1 = await storageManager.get(storageInfo);
                    const res2 = await storageManager.hkubeResults.get({ jobId });

                    expect('gal-gadot').to.equal(res1);
                    expect('gal-gadot').to.equal(res2);
                });
                it('get and put object', async () => {
                    const jobId = uuid();
                    const storageInfo = await storageManager.hkubeResults.put({ jobId, data: { test: 'gal-gadot' } });
                    const res1 = await storageManager.get(storageInfo);
                    const res2 = await storageManager.hkubeResults.get({ jobId });
                    expect({ test: 'gal-gadot' }).to.deep.equal(res1);
                    expect({ test: 'gal-gadot' }).to.deep.equal(res2);
                });
                it('get and put array', async () => {
                    const jobId = uuid();
                    const storageInfo = await storageManager.hkubeResults.put({ jobId, data: [1, 2, 3] });
                    const res1 = await storageManager.get(storageInfo);
                    const res2 = await storageManager.hkubeResults.get({ jobId });
                    expect([1, 2, 3]).to.deep.equal(res1);
                    expect([1, 2, 3]).to.deep.equal(res2);
                });
                it('list', async () => {
                    const jobId = uuid();
                    await storageManager.hkubeResults.put({ jobId, data: 'gal-gadot' });
                    const res = await storageManager.hkubeResults.list({ jobId });
                    expect(res.length).to.equal(1);
                });
                it('delete', async () => {
                    const jobId = uuid();
                    await storageManager.hkubeResults.put({ jobId, data: 'gal-gadot' });
                    const keys = await storageManager.hkubeResults.list({ jobId });
                    expect(keys.length).to.equal(1);

                    await storageManager.hkubeResults.delete({ jobId });
                    const keysAfter = await storageManager.hkubeResults.list({ jobId });
                    expect(keysAfter.length).to.equal(0);
                });
            });
            describe(adapter + ':hkube-store', () => {
                it('get and put string', async () => {
                    const type = uuid();
                    const name = uuid();
                    const storageInfo = await storageManager.hkubeStore.put({ type, name, data: 'gal-gadot' });
                    const res1 = await storageManager.get(storageInfo);
                    const res2 = await storageManager.hkubeStore.get({ type, name });
                    expect('gal-gadot').to.equal(res1);
                    expect('gal-gadot').to.equal(res2);
                });
                it('get and put object', async () => {
                    const type = uuid();
                    const name = uuid();
                    const storageInfo = await storageManager.hkubeStore.put({ type, name, data: { test: 'gal-gadot' } });
                    const res1 = await storageManager.get(storageInfo);
                    const res2 = await storageManager.hkubeStore.get({ type, name });
                    expect({ test: 'gal-gadot' }).to.deep.equal(res1);
                    expect({ test: 'gal-gadot' }).to.deep.equal(res2);
                });
                it('get and put array', async () => {
                    const type = uuid();
                    const name = uuid();
                    const storageInfo = await storageManager.hkubeStore.put({ type, name, data: [1, 2, 3] });
                    const res1 = await storageManager.get(storageInfo);
                    const res2 = await storageManager.hkubeStore.get({ type, name });
                    expect([1, 2, 3]).to.deep.equal(res1);
                    expect([1, 2, 3]).to.deep.equal(res2);
                });
                it('list', async () => {
                    const type = uuid();
                    await storageManager.hkubeStore.put({ type, name: uuid(), data: [1, 2, 3] });
                    await storageManager.hkubeStore.put({ type, name: uuid(), data: [1, 2, 3] });
                    await storageManager.hkubeStore.put({ type, name: uuid(), data: [1, 2, 3] });
                    await storageManager.hkubeStore.put({ type, name: uuid(), data: [1, 2, 3] });
                    const res = await storageManager.hkubeStore.list({ type });
                    expect(res.length).to.equal(4);
                });
                it('delete', async () => {
                    const type = uuid();
                    const taskArray = [uuid(), uuid(), uuid(), uuid()];
                    await storageManager.hkubeStore.put({ type, name: taskArray[0], data: 'gal-gadot' });
                    await storageManager.hkubeStore.put({ type, name: taskArray[1], data: 'gal-gadot' });
                    await storageManager.hkubeStore.put({ type, name: taskArray[2], data: 'gal-gadot' });
                    await storageManager.hkubeStore.put({ type, name: taskArray[3], data: 'gal-gadot' });

                    const keys = await storageManager.hkubeStore.list({ type });
                    expect(keys.length).to.equal(4);

                    await storageManager.hkubeStore.delete({ type, name: taskArray[0] });
                    await storageManager.hkubeStore.delete({ type, name: taskArray[1] });
                    await storageManager.hkubeStore.delete({ type, name: taskArray[2] });
                    await storageManager.hkubeStore.delete({ type, name: taskArray[3] });
                    const keysAfter = await storageManager.hkubeStore.list({ type });
                    expect(keysAfter.length).to.equal(0);
                });
            });
            describe(adapter + ':hkube-executions', () => {
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
            describe(adapter + ':hkube-metadata', () => {
                it('get and put string', async () => {
                    const jobId = uuid();
                    const taskId = uuid();
                    const storageInfo = await storageManager.hkubeMetadata.put({ jobId, taskId, data: 'gal-gadot' });// eslint-disable-line
                    const res1 = await storageManager.get(storageInfo);
                    const res2 = await storageManager.hkubeMetadata.get({ jobId, taskId });
                    expect('gal-gadot').to.equal(res1);
                    expect('gal-gadot').to.equal(res2);
                });
                it('get and put object', async () => {
                    const jobId = uuid();
                    const taskId = uuid();
                    const storageInfo = await storageManager.hkubeMetadata.put({ jobId, taskId, data: { test: 'gal-gadot' } });// eslint-disable-line
                    const res1 = await storageManager.get(storageInfo);
                    const res2 = await storageManager.hkubeMetadata.get({ jobId, taskId });
                    expect({ test: 'gal-gadot' }).to.deep.equal(res1);
                    expect({ test: 'gal-gadot' }).to.deep.equal(res2);
                });
                it('get and put array', async () => {
                    const jobId = uuid();
                    const taskId = uuid();
                    const storageInfo = await storageManager.hkubeMetadata.put({ jobId, taskId, data: [1, 2, 3] });// eslint-disable-line
                    const res1 = await storageManager.get(storageInfo);
                    const res2 = await storageManager.hkubeMetadata.get({ jobId, taskId });
                    expect([1, 2, 3]).to.deep.equal(res1);
                    expect([1, 2, 3]).to.deep.equal(res2);
                });
                it('list', async () => {
                    const jobId = uuid();
                    await storageManager.hkubeMetadata.put({ jobId, taskId: 'green' + uuid(), data: [1, 2, 3] });// eslint-disable-line
                    await storageManager.hkubeMetadata.put({ jobId, taskId: 'green' + uuid(), data: [4, 5, 6] });// eslint-disable-line
                    await storageManager.hkubeMetadata.put({ jobId, taskId: 'green' + uuid(), data: [7, 8, 9] });// eslint-disable-line
                    await storageManager.hkubeMetadata.put({ jobId, taskId: 'green' + uuid(), data: [10, 11, 12] });// eslint-disable-line
                    const res = await storageManager.hkubeMetadata.list({ jobId, nodeName: 'green' });
                    expect(res.length).to.equal(4);
                });
                it('delete', async () => {
                    const jobId = uuid();
                    const taskId = uuid();
                    await storageManager.hkubeMetadata.put({ jobId, taskId: 'green' + taskId, data: 'gal-gadot' });// eslint-disable-line
                    const keys = await storageManager.hkubeMetadata.list({ jobId, nodeName: 'green' });
                    expect(keys.length).to.equal(1);
                    await storageManager.hkubeMetadata.delete({ jobId, taskId: 'green' + taskId });
                    const keysAfter = await storageManager.hkubeMetadata.list({ jobId, nodeName: 'green' });
                    expect(keysAfter.length).to.equal(0);
                });
                it('delete by prefix', async () => {
                    const jobId = uuid();
                    const taskId = uuid();
                    await storageManager.hkubeMetadata.put({ jobId, taskId: 'green' + taskId, data: 'gal-gadot' });// eslint-disable-line
                    const keys = await storageManager.hkubeMetadata.list({ jobId, nodeName: 'green' });
                    expect(keys.length).to.equal(1);
                    await storageManager.hkubeMetadata.delete({ jobId });
                    const keysAfter = await storageManager.hkubeMetadata.list({ jobId });
                    expect(keysAfter.length).to.equal(0);
                });
            });
        });
    });
    after(() => {
        const config = require('./config'); // eslint-disable-line
        const sp = Object.values(STORAGE_PREFIX).map(x => config.clusterName + '-' + x);
        Object.values(sp).forEach(dir => fs.removeSync(path.join(baseDir, dir)));
    });
});
