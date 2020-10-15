const { expect } = require('chai');
const uuid = require('uuid/v4');
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

describe('CustomStream', () => {
    it('should custom encode: true and value: object', async () => {
        // this test doesn't support non binary encoding
        if (!encoding.isBinary) {
            return
        }
        const jobId = `jobId-${uuid()}`;
        const taskId = `taskId-${uuid()}`;
        const data = { mydata: 'myData', myProp: 'myProp', value: "newstrvalue" };
        const path = storageManager.hkube.createPath({ jobId, taskId });
        const { header, payload } = encoding.encodeHeaderPayload(data);
        const result = await storageManager.storage.put({ path, header, data: payload, encodeOptions: { ignoreEncode: true } });
        const stream = await storageManager.getCustomStream(result);
        const res = await readStreamAsString(stream);
        expect(res).to.eql(JSON.stringify(data));
    });
});

