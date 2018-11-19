const { expect } = require('chai');
const uuid = require('uuid/v4');
const path = require('path');
const moment = require('moment');
const mockery = require('mockery');
let storageManager;

describe('etcd-adapter', () => {
    before(async () => {
        mockery.enable({
            warnOnReplace: false,
            warnOnUnregistered: false,
            useCleanCache: true
        });
        mockery.resetCache();
        storageManager = require('../lib/storage-manager'); // eslint-disable-line
        const config = {};
        config.defaultStorage = 'etcd';
        config.etcd = {
            protocol: 'http',
            host: process.env.ETCD_CLIENT_SERVICE_HOST || '127.0.0.1',
            port: process.env.ETCD_CLIENT_SERVICE_PORT || 4001
        };
        config.storageAdapters = {
            etcd: {
                connection: config.etcd,
                moduleName: process.env.STORAGE_MODULE || '@hkube/etcd-adapter'
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
    xit('list', async () => {
        const jobId = uuid();
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        const today = moment().format(storageManager.DateFormat);
        const res = await storageManager.list({ Path: path.join(today, jobId) });
        expect(res.length).to.equal(4);
    });
    xit('delete', async () => {
        const jobId = uuid();
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });
        const today = moment().format(storageManager.DateFormat);
        await storageManager.delete({ Path: path.join('hkube', today, jobId) });
    });
});

