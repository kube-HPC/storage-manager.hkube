const { expect } = require('chai');
const uuid = require('uuid/v4');
const fs = require('fs-extra');
const storageManager = require('../lib/storage-manager');

const readStreamAsString = (stream) => {
    return new Promise((res) => {
        let bufs = '';
        stream.on('data', (d) => {
            bufs += d.toString('utf8');
        });
        stream.on('end', () => {
            res(bufs);
        });
    });
}

describe('Builds', () => {
    it('get and put string', async () => {
        const storageInfo = await storageManager.hkubeBuilds.put({ buildId: uuid(), data: 'gal-gadot' });
        const res = await storageManager.get(storageInfo);
        expect('gal-gadot').to.equal(res);
    });
    it('get and put object', async () => {
        const storageInfo = await storageManager.hkubeBuilds.put({
            buildId: uuid(),
            data: { test: 'gal-gadot' }
        });
        const res = await storageManager.get(storageInfo);
        expect({ test: 'gal-gadot' }).to.deep.equal(res);
    });
    it('get and put array', async () => {
        const storageInfo = await storageManager.hkubeBuilds.put({
            buildId: uuid(),
            data: [1, 2, 3]
        });
        const res = await storageManager.get(storageInfo);
        expect([1, 2, 3]).to.deep.equal(res);
    });
    it('delete put get stream', async () => {
        const buildId = uuid();
        const fileContent = await fs.readFile('tests/mocks/stream.yml', 'utf-8');
        const readStream = fs.createReadStream('tests/mocks/stream.yml');
        await storageManager.hkubeBuilds.putStream({ buildId, data: readStream });
        const stream = await storageManager.hkubeBuilds.getStream({ buildId });
        const res = await readStreamAsString(stream);
        expect(res).to.deep.equal(fileContent);
    });
});

