const { expect } = require('chai');
const uuid = require('uuid/v4');
const storageManager = require('../lib/storage-manager');

describe('Store', () => {
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

