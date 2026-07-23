/* eslint-disable @typescript-eslint/no-var-requires */
export {}
const { expect } = require('chai')
const sinon = require('sinon')
const Amqp = require('../../src/Amqp').default
const { credentialsFixture } = require('../doubles')
const helper = require('node-red-node-test-helper')
const amqpInManualAck = require('../../src/nodes/amqp-in-manual-ack')
const amqpOut = require('../../src/nodes/amqp-out')
const amqpBroker = require('../../src/nodes/amqp-broker')

helper.init(require.resolve('node-red'))

describe('mixed AMQP flow', () => {
  beforeEach(function (done) {
    helper.startServer(done)
  })

  afterEach(function (done) {
    helper.unload()
    helper.stopServer(done)
    sinon.restore()
  })

  it('reconnects both manual-ack input and output nodes after their shared connection closes', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const sharedConnection = {
        on: sinon.stub(),
        off: sinon.stub(),
        close: sinon.stub(),
      }
      const channel = {
        on: sinon.stub(),
        off: sinon.stub(),
      }
      const connectStub = sinon
        .stub(Amqp.prototype, 'connect')
        .resolves(sharedConnection as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves(channel as any)
      const consumeStub = sinon.stub(Amqp.prototype, 'consume').resolves()
      const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()
      const flow = [
        {
          id: 'manual-in',
          type: 'amqp-in-manual-ack',
          broker: 'broker',
          queueName: 'manual-ack-queue',
          reconnectOnError: false,
          wires: [],
        },
        {
          id: 'out',
          type: 'amqp-out',
          broker: 'broker',
          exchangeName: 'events',
          exchangeType: 'topic',
          exchangeRoutingKey: 'events.test',
          exchangeRoutingKeyType: 'str',
          amqpProperties: '{}',
          reconnectOnError: false,
          wires: [],
        },
        {
          id: 'broker',
          type: 'amqp-broker',
          host: 'localhost',
          port: '5672',
        },
      ]

      await helper.load(
        [amqpInManualAck, amqpOut, amqpBroker],
        flow,
        credentialsFixture,
      )

      const closeHandlers = sharedConnection.on
        .withArgs('close')
        .getCalls()
        .map(call => call.args[1])
      expect(connectStub.callCount).to.equal(2)
      expect(closeHandlers).to.have.length(2)
      expect(consumeStub.calledOnce).to.be.true

      await Promise.all(closeHandlers.map(handler => handler()))
      expect(closeStub.calledTwice).to.be.true

      await clock.tickAsync(2001)
      expect(connectStub.callCount).to.equal(4)
      expect(consumeStub.calledTwice).to.be.true
    } finally {
      clock.restore()
    }
  })
})
