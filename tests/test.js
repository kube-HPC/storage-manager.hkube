const storageManager = require('../lib/storage-manager');
const { expect } = require('chai');
const uuid = require('uuid/v4');
const path = require('path');
const moment = require('moment');

describe('s3-adapter', () => {
    before(async () => {
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
    it('get and put string', async () => {
        const jobId = uuid();
        const taskId = uuid();
        const storageInfo = await storageManager.put({ jobId, taskId, data: 'gal-gadot' });
        const res = await storageManager.get(storageInfo);
        expect('gal-gadot').to.equal(res);
    });
    it('get and put object', async () => {
        const jobId = uuid();
        const taskId = uuid();
        const storageInfo = await storageManager.put({ jobId, taskId, data: { test: 'gal-gadot' } });
        const res = await storageManager.get(storageInfo);
        expect({ test: 'gal-gadot' }).to.deep.equal(res);
    });
    it('get and put array', async () => {
        const jobId = uuid();
        const taskId = uuid();
        const storageInfo = await storageManager.put({ jobId, taskId, data: [1, 2, 3] });
        const res = await storageManager.get(storageInfo);
        expect([1, 2, 3]).to.deep.equal(res);
    });
    it('list', async () => {
        const jobId = uuid();
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        const today = moment().format(storageManager.DateFormat);
        const res = await storageManager.list({ Path: path.join(today, jobId) });
        expect(res.length).to.equal(4);
    });
    it('delete', async () => {
        const jobId = uuid();
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        const today = moment().format(storageManager.DateFormat);
        const res = await storageManager.delete({ Path: path.join('hkube', today, jobId) });
        expect(res.Deleted.length).to.equal(4);
    });
});

describe('redis-adapter', () => {
    before(async () => {
    });
    it('get', async () => {
    });
    it('put', async () => {
    });
    it('delete', async () => {
    });
});

describe('etcd-adapter', () => {
    before(async () => {
    });
    it('get', async () => {
    });
    it('put', async () => {
    });
    it('delete', async () => {
    });
});

describe('fs-adapter', () => {
    before(async () => {
    });
    it('get', async () => {
    });
    it('put', async () => {
    });
    it('delete', async () => {
    });
});
