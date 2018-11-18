const { expect } = require('chai');
const uuid = require('uuid/v4');
const path = require('path');
const moment = require('moment');
const fs = require('fs-extra');
const mockery = require('mockery');
const baseDir = '';

let storageManager;

describe('fs-adapter', () => {
    before(async () => {
        mockery.enable({
            warnOnReplace: false,
            warnOnUnregistered: false,
            useCleanCache: true
        });
        mockery.resetCache();

        storageManager = require('../lib/storage-manager'); // eslint-disable-line
        const config = {};
        config.defaultStorage = 'fs';
        config.fs = {
            baseDirectory: baseDir
        };
        config.storageAdapters = {
            fs: {
                connection: config.fs,
                moduleName: process.env.STORAGE_MODULE || '@hkube/fs-adapter'
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
        const link = await storageManager.put({ jobId, taskId: uuid(), data: 'gal-gadot' });

        const today = moment().format(storageManager.DateFormat);
        const res = await fs.pathExists(path.join(baseDir, link.Path));
        expect(res).to.equal(true);
        await storageManager.delete({ Path: path.join('hkube', today, jobId) });
        const res1 = await fs.pathExists(path.join(baseDir, link.Path));
        expect(res1).to.equal(false);
    });
});

