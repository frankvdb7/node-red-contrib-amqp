#!/usr/bin/env node
import './register-tests.cjs';
import 'source-map-support/register.js';
import Mocha from 'mocha';
import { readdirSync } from 'node:fs';
import { join, sep } from 'node:path';

const mocha = new Mocha();
const integration = process.argv.includes('--integration');
const directory = integration ? 'test/integration' : 'test';
const files = readdirSync(directory, { recursive: true })
  .filter(file => file.endsWith('.spec.ts') && (integration || !file.startsWith(`integration${sep}`)))
  .map(file => join(directory, file))
  .sort();
if (!files.length) throw new Error('No test files discovered');
files.forEach(file => mocha.addFile(file));

mocha.run(failures => {
  process.exit(failures ? 1 : 0);
});
