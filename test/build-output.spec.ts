/* eslint-disable @typescript-eslint/no-var-requires */
export {}
const { expect } = require('chai')
const fs = require('fs')
const path = require('path')

describe('build output', () => {
  const base = path.join(__dirname, '..', 'build')
  const nodeDir = path.join(base, 'src', 'nodes')
  const nodes = ['amqp-in', 'amqp-out', 'amqp-broker', 'amqp-in-manual-ack']

  it('does not create top-level build/nodes directory', () => {
    expect(fs.existsSync(path.join(base, 'nodes'))).to.be.false
  })

  nodes.forEach(name => {
    it(`should generate ${name}.js`, () => {
      expect(fs.existsSync(path.join(nodeDir, `${name}.js`))).to.be.true
    })
  })
})
