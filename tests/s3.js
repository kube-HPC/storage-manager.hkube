const { expect } = require('chai');
const uuid = require('uuid/v4');
const path = require('path');
const mockery = require('mockery');
let storageManager;

describe('s3-adapter', () => {
    before(async () => {
        mockery.enable({
            warnOnReplace: false,
            warnOnUnregistered: false,
            useCleanCache: false
        });
        // mockery.resetCache();
        mockery.registerSubstitute('@hkube/logger', process.cwd() + '/tests/mock/log.js');
        storageManager = require('../lib/storage-manager'); // eslint-disable-line
        const config = {};
        config.defaultStorage = 's3';
        config.s3 = {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'AKIAIOSFODNN7EXAMPLE',
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            endpoint: process.env.S3_ENDPOINT_URL || 'http://127.0.0.1:9000'
        };
        config.storageAdapters = {
            s3: {
                connection: config.s3,
                moduleName: process.env.STORAGE_MODULE || '@hkube/s3-adapter'
            }
        };
        await storageManager.init(config, true);
    });
    describe('base', () => {
        it('get and put string', async () => {
            const storageInfo = await storageManager.put({
                Path: path.join('hkube/', uuid(), uuid()),
                Data: 'gal-gadot'
            });
            const res = await storageManager.get(storageInfo);
            expect('gal-gadot').to.equal(res);
        });
        it('get and put object', async () => {
            const storageInfo = await storageManager.put({
                Path: path.join('hkube/', uuid(), uuid()),
                Data: { test: 'gal-gadot' }
            });
            const res = await storageManager.get(storageInfo);
            expect({ test: 'gal-gadot' }).to.deep.equal(res);
        });
        it('get and put array', async () => {
            const storageInfo = await storageManager.put({
                Path: path.join('hkube/', uuid(), uuid()),
                Data: [1, 2, 3]
            });
            const res = await storageManager.get(storageInfo);
            expect([1, 2, 3]).to.deep.equal(res);
        });
        it('list', async () => {
            const jobId = uuid();
            await storageManager.put({ Path: path.join('hkube/', jobId, uuid()), Data: 'gal-gadot' });
            await storageManager.put({ Path: path.join('hkube/', jobId, uuid()), Data: 'gal-gadot' });
            await storageManager.put({ Path: path.join('hkube/', jobId, uuid()), Data: 'gal-gadot' });
            await storageManager.put({ Path: path.join('hkube/', jobId, uuid()), Data: 'gal-gadot' });

            const res = await storageManager.list({ Path: path.join('hkube', jobId) });
            expect(res.length).to.equal(4);
        });
        it('delete', async () => {
            const jobId = uuid();
            await storageManager.put({ Path: path.join('hkube/', jobId, uuid()), Data: 'gal-gadot' });
            await storageManager.put({ Path: path.join('hkube/', jobId, uuid()), Data: 'gal-gadot' });
            await storageManager.put({ Path: path.join('hkube/', jobId, uuid()), Data: 'gal-gadot' });
            await storageManager.put({ Path: path.join('hkube/', jobId, uuid()), Data: 'gal-gadot' });

            const keys = await storageManager.list({ Path: path.join('hkube/', jobId) });
            const promiessArray = [];
            keys.forEach((key) => {
                promiessArray.push(storageManager.delete(key));
            });
            await Promise.all(promiessArray);
            const keysAfter = await storageManager.list({ Path: path.join('hkube/', jobId) });

            expect(keysAfter.length).to.equal(0);
        });
    });
    describe('hkube-index', () => {
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
        it('delete', async () => {
            const jobId = uuid();
            await storageManager.hkubeIndex.put({ jobId });
            await storageManager.hkubeIndex.delete({ date: Date.now(), jobId });
            const o = await storageManager.hkubeIndex.get({ date: Date.now(), jobId });
            expect(o.error).to.be.not.undefined;
        });
    });
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
    });
    describe('hkube-results', () => {
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
    describe('hkube-store', () => {
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
    describe('hkube-executions', () => {
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
    describe('hkube-metadata', () => {
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
    });
});

