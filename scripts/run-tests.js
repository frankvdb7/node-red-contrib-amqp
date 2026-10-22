#!/usr/bin/env node
require('@swc/register')({ module: { type: 'commonjs' } });

require('source-map-support/register');
const Mocha = require('mocha');
const glob = require('glob');

const mocha = new Mocha();
const integration = process.argv.includes('--integration');
const files = integration
  ? glob.sync('test/integration/**/*.spec.ts')
  : glob.sync('test/**/*.spec.ts', { ignore: 'test/integration/**' });
if (!files.length) throw new Error('No test files discovered');
files.forEach(file => mocha.addFile(file));

mocha.run(failures => {
  process.exit(failures ? 1 : 0);
});
