const fs = require('fs-extra');
const config = require('./config');
const storageManager = require('../lib/storage-manager');
const { Encoding } = require('@hkube/encoding');

before(async function () {
    this.timeout(15000)
    await storageManager.init(config, null, true);
    global.encoding = new Encoding({ type: config.storageEncoding });
});
after(() => {
    fs.removeSync(config.fs.baseDirectory);
});
