'use strict'

module.exports = {
  require: ['ts-node/register', 'source-map-support/register'],
  diff: true,
  ui: 'bdd',
  spec: 'test/**/*.spec.ts',
  'node-option': ['experimental-specifier-resolution=node'],
  // 'watch-files': ['lib/**/*.js', 'test/**/*.js'],
  // 'watch-ignore': ['lib/vendor']
}
