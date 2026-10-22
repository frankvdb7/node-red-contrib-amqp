import { expect } from 'chai'
import sinon from 'sinon'
import Amqp from '../../src/Amqp'
import { NodeType } from '../../src/types'
import helper from 'node-red-node-test-helper'
import amqpIn from '../../src/nodes/amqp-in'

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
