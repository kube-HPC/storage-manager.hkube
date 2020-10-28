const { expect } = require('chai');
const fs = require('fs-extra');
const storageManager = require('../lib/storage-manager');
const uuid = require('uuid');

const readStreamAsString = (stream) =>
    new Promise((res, rej) => {
        let bufs = '';
        stream.on('data', (d) => {
            bufs += d.toString('utf8');
        });
        stream.on('end', () => {
            res(bufs);
        });
        stream.on('error', () => rej(new Error('expected error')));
    });

describe('Datasource', () => {
    it('should get the storage prefix', () => {
        expect(storageManager.hkubeDataSource.prefix).to.match(/hkube-datasource/i);
    });
    it('get stream from non existing datasource', async () => {
        const dataSource = uuid.v4();
        const fileName = 'my-file';
        return expect(storageManager.hkubeDataSource
            .getStream({ dataSource, fileName })
            .then(readStreamAsString)
        ).to.eventually.be.rejected;
    });
    it('put and get stream', async () => {
        const dataSource = 'my-datasource';
        const fileName = 'my-file';
        const fileContent = await fs.readFile('tests/mocks/stream.yml', 'utf-8');
        const readStream = fs.createReadStream('tests/mocks/stream.yml');
        await storageManager.hkubeDataSource.putStream({ dataSource, fileName, data: readStream });
        const stream = await storageManager.hkubeDataSource.getStream({ dataSource, fileName });
        const res = await readStreamAsString(stream);
        expect(res).to.deep.equal(fileContent);
    });
    it('should return list with stats', async () => {
        const dataSource = uuid.v4();
        const fileNames = new Array(5).fill(0).map(() => uuid.v4()).sort();
        const readStream = fs.createReadStream('tests/mocks/stream.yml');
        await Promise.all(
            fileNames.map(fileName =>
                storageManager.hkubeDataSource.putStream({ dataSource, fileName, data: readStream })
            )
        );
        const list = await storageManager.hkubeDataSource.listWithStats({ dataSource });
        expect(list.length).to.be.gte(5);
        list.forEach(entry => {
            expect(entry).to.haveOwnProperty('path');
            expect(entry).to.haveOwnProperty('size');
            expect(entry).to.haveOwnProperty('mtime');
        });
    });
    it('list all the files under a datasource', async () => {
        const dataSource = uuid.v4();
        const fileNames = new Array(5).fill(0).map(() => uuid.v4()).sort();
        const readStream = fs.createReadStream('tests/mocks/stream.yml');
        await Promise.all(
            fileNames.map(fileName =>
                storageManager.hkubeDataSource.putStream({ dataSource, fileName, data: readStream })
            )
        );
        const list = await storageManager.hkubeDataSource.list({ dataSource });
        const listedNames = list.map(item => item.path).map(item => item.replace(/(.+)\//, '')).sort();
        expect(listedNames).to.eql(fileNames);
    });
    it('should return an empty list when non existing datasource is listed', async () => {
        const dataSource = uuid.v4();
        const list = await storageManager.hkubeDataSource.list({ dataSource });
        expect(list).to.be.empty;
    });
    it('delete a datasource', async () => {
        const dataSource = uuid.v4();
        const fileNames = new Array(5).fill(0).map(() => uuid.v4()).sort();
        const readStream = fs.createReadStream('tests/mocks/stream.yml');
        await Promise.all(
            fileNames.map(fileName =>
                storageManager.hkubeDataSource.putStream({ dataSource, fileName, data: readStream })
            )
        );
        const fullList = await storageManager.hkubeDataSource.list({ dataSource });
        expect(fullList).to.have.lengthOf(5);
        await storageManager.hkubeDataSource.delete({ dataSource });
        const emptyList = await storageManager.hkubeDataSource.list({ dataSource });
        expect(emptyList).to.have.lengthOf(0);
    });
});

