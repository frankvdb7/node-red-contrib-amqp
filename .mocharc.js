'use strict'

module.exports = {
  require: ['ts-node/register', 'source-map-support/register'],
  diff: true,
  ui: 'bdd',
  spec: 'test/**/*.spec.ts',
  // 'watch-files': ['lib/**/*.js', 'test/**/*.js'],
  // 'watch-ignore': ['lib/vendor']
}
