export {}
const { expect } = require('chai')
const sinon = require('sinon')
const Amqp = require('../../src/Amqp').default
const { NodeType } = require('../../src/types')

const helper = require('node-red-node-test-helper')
const amqpIn = require('../../src/nodes/amqp-in')

helper.init(require.resolve('node-red'))

describe('amqp-in Node', () => {
  beforeEach(function (done) {
    sinon.stub(Amqp.prototype, 'connect').resolves()
    helper.startServer(done)
  })

  afterEach(function (done) {
    helper.unload()
    helper.stopServer(done)
    sinon.restore()
  })

  it('should be loaded', done => {
    const flow = [{ id: 'n1', type: NodeType.AmqpIn, name: 'test name' }]
    helper.load(amqpIn, flow, () => {
      const n1 = helper.getNode('n1')
      n1.should.have.property('name', 'test name')
      done()
    })
  })
})
