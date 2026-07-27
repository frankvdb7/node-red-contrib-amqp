#!/usr/bin/env node
require('@swc/register')({ module: { type: 'commonjs' } });

require('source-map-support/register');
const Mocha = require('mocha');
const glob = require('glob');

const mocha = new Mocha();
const files = glob.sync('test/**/*.spec.ts');
files.forEach(file => mocha.addFile(file));

mocha.run(failures => {
  process.exit(failures ? 1 : 0);
});
