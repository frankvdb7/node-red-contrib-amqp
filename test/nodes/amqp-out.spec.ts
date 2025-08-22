/* eslint-disable @typescript-eslint/no-var-requires */
/* eslint-disable @typescript-eslint/ban-ts-comment */
import { expect } from 'chai'
import * as sinon from 'sinon'
import Amqp from '../../src/Amqp'
import { ErrorType, NodeType } from '../../src/types'
import { CustomError, amqpOutFlowFixture, credentialsFixture } from '../doubles'
import { NODE_STATUS } from '../../src/constants'

const helper = require('node-red-node-test-helper')
const amqpOut = require('../../src/nodes/amqp-out')
const amqpBroker = require('../../src/nodes/amqp-broker')

helper.init(require.resolve('node-red'))

describe('amqp-out Node', () => {
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
    const flow = [{ id: 'n1', type: NodeType.AmqpOut, name: 'test name' }]
    helper.load(amqpOut, flow, () => {
      const n1 = helper.getNode('n1')
      n1.should.have.property('name', 'test name')
      done()
    })
  })

  it('should connect to the server and send some messages', function (done) {
    // @ts-ignore
    Amqp.prototype.channel = {
      unbindQueue: (): null => null,
      close: (): null => null,
    }
    // @ts-ignore
    Amqp.prototype.connection = {
      close: (): null => null,
    }
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      // @ts-ignore
      .resolves(true)
    sinon.stub(Amqp.prototype, 'initialize')

    helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
      async function () {
        expect(connectStub.calledOnce).to.be.true
        // TODO: Figure out why this isn't working:
        // expect(initializeStub.calledOnce).to.be.true

        const amqpOutNode = helper.getNode('n1')
        amqpOutNode.receive({ payload: 'foo', routingKey: 'bar' })
        amqpOutNode.receive({ payload: 'foo' })
        amqpOutNode.close()

        done()
      },
    )
  })

  it('does not register flows:stopped listener', function (done) {
    sinon.stub(Amqp.prototype, 'connect').resolves(true as any)
    sinon.stub(Amqp.prototype, 'initialize')

    helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
      function () {
        expect(helper._events.listenerCount('flows:stopped')).to.equal(0)
        const n1 = helper.getNode('n1')
        n1.close()
        done()
      },
    )
  })

  it('should connect to the server and send some messages with a dynamic routing key from `msg`', function (done) {
    // @ts-ignore
    Amqp.prototype.channel = {
      unbindQueue: (): null => null,
      close: (): null => null,
    }
    // @ts-ignore
    Amqp.prototype.connection = {
      close: (): null => null,
    }
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      // @ts-ignore
      .resolves(true)
    sinon.stub(Amqp.prototype, 'initialize')

    const flowFixture = [...amqpOutFlowFixture]
    // @ts-ignore
    flowFixture[0].exchangeRoutingKeyType = 'msg'

    helper.load(
      [amqpOut, amqpBroker],
      flowFixture,
      credentialsFixture,
      async function () {
        expect(connectStub.calledOnce).to.be.true
        // TODO: Figure out why this isn't working:
        // expect(initializeStub.calledOnce).to.be.true

        const amqpOutNode = helper.getNode('n1')
        amqpOutNode.receive({ payload: 'foo' })
        amqpOutNode.close()

        done()
      },
    )
  })

  it('should connect to the server and send some messages with a dynamic jsonata routing key', function (done) {
    // @ts-ignore
    Amqp.prototype.channel = {
      unbindQueue: (): null => null,
      close: (): null => null,
    }
    // @ts-ignore
    Amqp.prototype.connection = {
      close: (): null => null,
    }
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      // @ts-ignore
      .resolves(true)
    sinon.stub(Amqp.prototype, 'initialize')

    const flowFixture = [...amqpOutFlowFixture]
    // @ts-ignore
    flowFixture[0].exchangeRoutingKeyType = 'jsonata'

    helper.load(
      [amqpOut, amqpBroker],
      flowFixture,
      credentialsFixture,
      async function () {
        expect(connectStub.calledOnce).to.be.true
        // TODO: Figure out why this isn't working:
        // expect(initializeStub.calledOnce).to.be.true

        const amqpOutNode = helper.getNode('n1')
        amqpOutNode.receive({ payload: 'foo' })
        amqpOutNode.close()

        done()
      },
    )
  })

  it('switches vhost dynamically from msg', function (done) {
    // @ts-ignore
    Amqp.prototype.channel = {
      unbindQueue: (): null => null,
      close: (): null => null,
    }
    // @ts-ignore
    Amqp.prototype.connection = {
      close: (): null => null,
    }
    sinon.stub(Amqp.prototype, 'connect').resolves(true as any)
    sinon.stub(Amqp.prototype, 'initialize')
    const setVhostStub = sinon.stub(Amqp.prototype, 'setVhost').resolves()

    helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
      function () {
        const amqpOutNode = helper.getNode('n1')
        amqpOutNode.receive({ payload: 'foo', vhost: 'vh2' })
        setTimeout(() => {
          expect(setVhostStub.calledWith('vh2')).to.be.true
          amqpOutNode.close()
          done()
        }, 0)
      },
    )
  })

  it('removes listeners before switching vhost', function (done) {
    const connectionMock = {
      removeAllListeners: sinon.spy(),
      close: (): null => null,
    }
    const channelMock = {
      removeAllListeners: sinon.spy(),
      unbindQueue: (): null => null,
      close: (): null => null,
    }

    sinon
      .stub(Amqp.prototype, 'connect')
      .callsFake(async function () {
        // @ts-ignore
        this.connection = connectionMock
        return connectionMock as any
      })

    sinon
      .stub(Amqp.prototype, 'initialize')
      .callsFake(async function () {
        // @ts-ignore
        this.channel = channelMock
        return channelMock as any
      })

    const setVhostStub = sinon.stub(Amqp.prototype, 'setVhost').resolves()

    helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
      function () {
        const amqpOutNode = helper.getNode('n1')
        amqpOutNode.receive({ payload: 'foo', vhost: 'vh2' })
        setTimeout(() => {
          sinon.assert.callOrder(
            connectionMock.removeAllListeners as any,
            setVhostStub,
          )
          sinon.assert.callOrder(
            channelMock.removeAllListeners as any,
            setVhostStub,
          )
          amqpOutNode.close()
          done()
        }, 0)
      },
    )
  })

  it('tries to connect but the broker is down', function (done) {
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .throws(new CustomError(ErrorType.ConnectionRefused))
    helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
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
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
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
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
      function () {
        const amqpOutNode = helper.getNode('n1')
        amqpOutNode.on('call:status', call => {
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
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
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
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
      function () {
        expect(connectStub).to.throw()
        done()
      },
    )
  })
})
