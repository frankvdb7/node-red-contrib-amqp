#!/usr/bin/env node
import './register-tests.cjs';
import 'source-map-support/register.js';
import Mocha from 'mocha';
import glob from 'glob';

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
