const encoding = require('@hkube/encoding/lib/encoding');
const { expect } = require('chai');
const uuid = require('uuid/v4');
const storageManager = require('../lib/storage-manager');

describe('CustomData', () => {
    it('should custom encode: true and value: object', async () => {
        // this test doesn't support non binary encoding
        if (!encoding.isBinary) {
            return
        }
        const jobId = `jobId-${uuid()}`;
        const taskId = `taskId-${uuid()}`;
        const object = { mydata: 'myData', myProp: 'myProp', value: "newstrvalue" };
        const path = storageManager.hkube.createPath({ jobId, taskId });
        const { header, payload: pay } = encoding.encodeHeaderPayload(object);
        const result = await storageManager.storage.put({ path, header, data: pay, encodeOptions: { ignoreEncode: true } });
        const { payload } = await storageManager.getCustomData(result);
        expect(payload).to.eql(object);
    });
});
