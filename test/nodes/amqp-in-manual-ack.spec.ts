/* eslint-disable @typescript-eslint/no-var-requires */
/* eslint-disable @typescript-eslint/ban-ts-comment */
export {}
const { expect } = require('chai')
const sinon = require('sinon')
const Amqp = require('../../src/Amqp').default
const { ErrorType, ManualAckType, NodeType } = require('../../src/types')
const {
  CustomError,
  amqpInManualAckFlowFixture,
  credentialsFixture,
} = require('../doubles')
const { NODE_STATUS } = require('../../src/constants')
const helper = require('node-red-node-test-helper')
const amqpInManualAck = require('../../src/nodes/amqp-in-manual-ack')
const amqpBroker = require('../../src/nodes/amqp-broker')

helper.init(require.resolve('node-red'))

describe('amqp-in-manual-ack Node', () => {
  beforeEach(function (done) {
    helper.startServer(done)
  })

  afterEach(function (done) {
    helper.unload()
    helper.stopServer(done)
    sinon.restore()
  })

  it('should be loaded', done => {
    sinon.stub(Amqp.prototype, 'connect')
    const flow = [
      { id: 'n1', type: NodeType.AmqpInManualAck, name: 'test name' },
    ]
    helper.load(amqpInManualAck, flow, () => {
      const n1 = helper.getNode('n1')
      n1.should.have.property('name', 'test name')
      done()
    })
  })

  it('should connect to the server', function (done) {
    // @ts-ignore
    Amqp.prototype.channel = {
      unbindQueue: (): null => null,
      close: (): null => null,
      ack: (): null => null,
    }
    // @ts-ignore
    Amqp.prototype.connection = {
      close: (): null => null,
    }
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      // @ts-ignore
      .resolves(true)
    const initializeStub = sinon.stub(Amqp.prototype, 'initialize')

    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      async function () {
        expect(connectStub.calledOnce).to.be.true

        // FIXME: Figure out why this isn't working:
        // expect(initializeStub.calledOnce).to.be.true

        const amqpInManualAckNode = helper.getNode('n1')

        // FIXME: these tests are essentially meaningless.
        // For some reason the node is not being properly loaded by the helper
        // They are not executing code
        amqpInManualAckNode.receive({ payload: 'foo', routingKey: 'bar' })
        amqpInManualAckNode.receive({
          payload: 'foo',
          routingKey: 'bar',
          manualAck: {
            ackMode: ManualAckType.Ack,
          },
        })
        amqpInManualAckNode.receive({
          payload: 'foo',
          routingKey: 'bar',
          manualAck: {
            ackMode: ManualAckType.AckAll,
          },
        })
        amqpInManualAckNode.receive({
          payload: 'foo',
          routingKey: 'bar',
          manualAck: {
            ackMode: ManualAckType.Nack,
          },
        })
        amqpInManualAckNode.receive({
          payload: 'foo',
          routingKey: 'bar',
          manualAck: {
            ackMode: ManualAckType.NackAll,
          },
        })
        amqpInManualAckNode.receive({
          payload: 'foo',
          routingKey: 'bar',
          manualAck: {
            ackMode: ManualAckType.Reject,
          },
        })
        amqpInManualAckNode.on('input', () => {
          console.warn('this is input?')
          done()
        })
        amqpInManualAckNode.close(true)
        done()
      },
    )
  })

  it('tries to connect but the broker is down', function (done) {
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .throws(new CustomError(ErrorType.ConnectionRefused))
    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      function () {
        expect(connectStub).to.throw()
        done()
      },
    )
  })

  it('catches an invalid login exception', function (done) {
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .throws(new CustomError(ErrorType.InvalidLogin))
    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      function () {
        expect(connectStub).to.throw()
        done()
      },
    )
  })

  it('detects invalid login from message text', function (done) {
    const err = new CustomError(ErrorType.ConnectionRefused, 'ACCESS_REFUSED - Login failed')
    const connectStub = sinon.stub(Amqp.prototype, 'connect').throws(err)
    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      function () {
        const amqpInManualAckNode = helper.getNode('n1')
        amqpInManualAckNode.on('call:status', call => {
          if (call.args[0].text === NODE_STATUS.Invalid.text) {
            try {
              expect(call.args[0]).to.deep.equal(NODE_STATUS.Invalid)
              expect(connectStub).to.throw()
              done()
            } catch (err) {
              done(err)
            }
          }
        })
      },
    )
  })

  it('handles dns lookup failures', function (done) {
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .throws(new CustomError(ErrorType.DnsResolve))

    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      function () {
        expect(connectStub).to.throw()
        done()
      },
    )
  })

  it('catches a generic exception', function (done) {
    const connectStub = sinon.stub(Amqp.prototype, 'connect').throws()
    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      function () {
        expect(connectStub).to.throw()
        done()
      },
    )
  })

  it('does not register flows:stopped listener', function (done) {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon
      .stub(Amqp.prototype, 'connect')
      .resolves(connectionMock as any)
    sinon
      .stub(Amqp.prototype, 'initialize')
      .resolves(channelMock as any)

    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      function () {
        expect(helper._events.listenerCount('flows:stopped')).to.equal(0)
        const node = helper.getNode('n1')
        node.close()
        done()
      },
    )
  })
})
