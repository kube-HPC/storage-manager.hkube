const { expect } = require('chai');
const uuid = require('uuid/v4');
const path = require('path');
const moment = require('moment');
const mockery = require('mockery');
let storageManager;

describe('redis-adapter', () => {
    before(async () => {
        mockery.enable({
            warnOnReplace: false,
            warnOnUnregistered: false,
            useCleanCache: true
        });
        mockery.resetCache();
        storageManager = require('../lib/storage-manager'); // eslint-disable-line
        const config = {};
        const useSentinel = !!process.env.REDIS_SENTINEL_SERVICE_HOST;

        config.defaultStorage = 'redis';
        config.redis = {
            host: useSentinel ? process.env.REDIS_SENTINEL_SERVICE_HOST : process.env.REDIS_SERVICE_HOST || 'localhost',
            port: useSentinel ? process.env.REDIS_SENTINEL_SERVICE_PORT : process.env.REDIS_SERVICE_PORT || 6379,
            sentinel: useSentinel,
        };
        config.storageAdapters = {
            redis: {
                connection: config.redis,
                moduleName: process.env.STORAGE_MODULE || '@hkube/redis-storage-adapter'
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
        await storageManager.delete({ Path: path.join('hkube', today, jobId) });
    });
});

