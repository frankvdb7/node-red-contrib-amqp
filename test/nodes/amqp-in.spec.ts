/* eslint-disable @typescript-eslint/no-var-requires */
/* eslint-disable @typescript-eslint/ban-ts-comment */
export {}
const { expect } = require('chai')
const sinon = require('sinon')
const Amqp = require('../../src/Amqp').default
const { ErrorType, NodeType } = require('../../src/types')
const { CustomError, amqpInFlowFixture, credentialsFixture } = require('../doubles')
const { NODE_STATUS } = require('../../src/constants')

const helper = require('node-red-node-test-helper')
const amqpIn = require('../../src/nodes/amqp-in')
const amqpBroker = require('../../src/nodes/amqp-broker')

helper.init(require.resolve('node-red'))

describe('amqp-in Node', () => {
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
    const flow = [{ id: 'n1', type: NodeType.AmqpIn, name: 'test name' }]
    helper.load(amqpIn, flow, () => {
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
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      async function () {
        expect(connectStub.calledOnce).to.be.true

        // TODO: Figure out why this isn't working:
        // expect(initializeStub.calledOnce).to.be.true

        const amqpInNode = helper.getNode('n1')
        amqpInNode.close()
        done()
      },
    )
  })

  it('does not register flows:stopped listener', function (done) {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub(), consume: sinon.stub() }
    sinon
      .stub(Amqp.prototype, 'connect')
      .resolves(connectionMock as any)
    sinon
      .stub(Amqp.prototype, 'initialize')
      .resolves(channelMock as any)

    helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      function () {
        expect(helper._events.listenerCount('flows:stopped')).to.equal(0)
        const amqpInNode = helper.getNode('n1')
        amqpInNode.close()
        done()
      },
    )
  })

  it('should reconnect to the server', function (done) {
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
    const initializeStub = sinon.stub(Amqp.prototype, 'initialize')

    helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      async function () {
        expect(connectStub.calledOnce).to.be.true

        // TODO: Figure out why this isn't working:
        // expect(initializeStub.calledOnce).to.be.true

        const amqpInNode = helper.getNode('n1')
        amqpInNode.close()
        done()
      },
    )
  })

  it('tries to connect but the broker is down', function (done) {
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .throws(new CustomError(ErrorType.ConnectionRefused))
    helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      function () {
        expect(connectStub).to.throw()
        // clock.tick(200)
        done()
      },
    )
  })

  it('catches an invalid login exception', function (done) {
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .throws(new CustomError(ErrorType.InvalidLogin))
    helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
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
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      function () {
        const amqpInNode = helper.getNode('n1')
        amqpInNode.on('call:status', call => {
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
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
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
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      function () {
        expect(connectStub).to.throw()
        done()
      },
    )
  })

  it('logs Error details when connection setup fails', function (done) {
    sinon.stub(Amqp.prototype, 'connect').rejects(new Error('socket exploded'))

    helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      function () {
        const amqpInNode = helper.getNode('n1')
        amqpInNode.on('call:error', call => {
          const message = String(call.args[0])
          if (message.startsWith('AmqpIn()')) {
            try {
              expect(message).to.include('Error: socket exploded')
              expect(message).to.not.include('{}')
              done()
            } catch (err) {
              done(err)
            }
          }
        })
      },
    )
  })

  it('handles non-object connect errors', function (done) {
    const connectStub = sinon.stub(Amqp.prototype, 'connect').throws('boom')
    helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      function () {
        const amqpInNode = helper.getNode('n1')
        amqpInNode.on('call:status', call => {
          if (call.args[0].text === NODE_STATUS.Error.text) {
            try {
              expect(call.args[0]).to.deep.equal(NODE_STATUS.Error)
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

  it('completes node close when amqp.close fails', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub(), consume: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    const closeStub = sinon.stub(Amqp.prototype, 'close').rejects(new Error('close failed'))

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
    )
    const amqpInNode = helper.getNode('n1')
    await amqpInNode.close()

    expect(closeStub.calledOnce).to.be.true
  })

  it('closes amqp when initialization fails after connect', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').rejects(new Error('init failed'))
    const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
    )

    expect(closeStub.called).to.be.true
  })

  it('does not register listeners or mark connected when node closes during initialization', async function () {
    let resolveInitialize: (value: unknown) => void
    const initializePromise = new Promise(resolve => {
      resolveInitialize = resolve
    })
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').returns(initializePromise as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    sinon.stub(Amqp.prototype, 'close').resolves()
    const markConnectedStub = sinon.stub(Amqp.prototype, 'markConnected')

    await helper.load([amqpIn, amqpBroker], amqpInFlowFixture, credentialsFixture)
    const amqpInNode = helper.getNode('n1')
    const closePromise = amqpInNode.close()
    resolveInitialize!(channelMock)
    await closePromise
    await new Promise(resolve => setTimeout(resolve, 0))

    expect(markConnectedStub.called).to.be.false
    expect(connectionMock.on.called).to.be.false
    expect(channelMock.on.called).to.be.false
  })

  it('does not set error or invalid status when initialization fails during shutdown', async function () {
    let rejectInitialize: (reason?: unknown) => void
    const initializePromise = new Promise((_, reject) => {
      rejectInitialize = reject
    })
    sinon.stub(Amqp.prototype, 'connect').resolves({ on: sinon.stub(), off: sinon.stub(), close: sinon.stub() } as any)
    sinon.stub(Amqp.prototype, 'initialize').returns(initializePromise as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()

    await helper.load([amqpIn, amqpBroker], amqpInFlowFixture, credentialsFixture)
    const amqpInNode = helper.getNode('n1')
    const statuses: unknown[] = []
    amqpInNode.on('call:status', call => statuses.push(call.args[0]))

    const closePromise = amqpInNode.close()
    rejectInitialize!(new Error('init failed during shutdown'))
    await closePromise
    await new Promise(resolve => setTimeout(resolve, 0))

    const hasErrorOrInvalid = statuses.some(status => {
      const text = status && typeof status === 'object' ? (status as { text?: string }).text : ''
      return text === NODE_STATUS.Error.text || text === NODE_STATUS.Invalid.text
    })
    expect(hasErrorOrInvalid).to.be.false
    expect(closeStub.called).to.be.true
  })

  it('does not register listeners or report connected when consume setup fails', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'assertQueue').rejects(new Error('assertQueue failed'))
    const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
    )

    expect(closeStub.called).to.be.true
    expect(connectionMock.on.called).to.be.false
    expect(channelMock.on.called).to.be.false
  })

  it('should reconnect on input message', done => {
    const connectStub = sinon.stub(Amqp.prototype, 'connect')
    const closeStub = sinon.stub(Amqp.prototype, 'close')
    const flow = [
      { id: 'n1', type: NodeType.AmqpIn, name: 'test name', broker: 'b1', wires: [[]] },
      { id: 'b1', type: 'amqp-broker', name: 'test broker' },
    ]
    helper.load([amqpIn, amqpBroker], flow, credentialsFixture, () => {
      const n1 = helper.getNode('n1')
      n1.receive({ payload: { reconnectCall: true }})
      expect(closeStub.calledOnce).to.be.true
      done()
    })
  })

  it('calls done with error when reconnect control fails', async function () {
    sinon.stub(Amqp.prototype, 'connect').resolves({ on: sinon.stub(), off: sinon.stub() } as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves({ on: sinon.stub(), off: sinon.stub() } as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    sinon.stub(Amqp.prototype, 'close').rejects(new Error('reconnect failed'))

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
    )

    const n1 = helper.getNode('n1')
    const callErrors: unknown[] = []
    n1.on('call:error', call => {
      callErrors.push(call.args[0])
    })

    n1.receive({ payload: { reconnectCall: true } })
    await new Promise(resolve => setTimeout(resolve, 50))

    const doneError = callErrors.find(arg => arg instanceof Error) as Error | undefined
    expect(doneError).to.not.equal(undefined)
    expect(doneError?.message).to.match(/reconnect failed/i)
  })

  it('allows repeated reconnect control attempts after close failure', async function () {
    sinon.stub(Amqp.prototype, 'connect').resolves({ on: sinon.stub(), off: sinon.stub() } as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves({ on: sinon.stub(), off: sinon.stub() } as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    const closeStub = sinon.stub(Amqp.prototype, 'close').rejects(new Error('reconnect failed'))

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
    )

    const n1 = helper.getNode('n1')
    n1.receive({ payload: { reconnectCall: true } })
    await new Promise(resolve => setTimeout(resolve, 50))
    n1.receive({ payload: { reconnectCall: true } })
    await new Promise(resolve => setTimeout(resolve, 50))

    expect(closeStub.calledTwice).to.be.true
  })

  it('does not emit unhandled rejection when reconnect fails in connection close handler', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    sinon.stub(Amqp.prototype, 'close').rejects(new Error('reconnect failed'))

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
    )

    const n1 = helper.getNode('n1')
    const callErrors: unknown[] = []
    n1.on('call:error', call => {
      callErrors.push(call.args[0])
    })

    let unhandledReason: unknown
    const onUnhandledRejection = (reason: unknown) => {
      unhandledReason = reason
    }
    process.once('unhandledRejection', onUnhandledRejection)
    try {
      const onConnClose = connectionMock.on.withArgs('close').getCall(0).args[1]
      onConnClose()
      await new Promise(resolve => setTimeout(resolve, 50))
    } finally {
      process.removeListener('unhandledRejection', onUnhandledRejection)
    }

    expect(unhandledReason).to.equal(undefined)
    expect(
      callErrors.some(error => /Reconnect failed after connection close/i.test(String(error))),
    ).to.be.true
  })

  it('handles reconnect control before listeners are assigned', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').returns(new Promise(() => undefined) as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
    )

    const n1 = helper.getNode('n1')
    const callErrors: unknown[] = []
    n1.on('call:error', call => {
      callErrors.push(call.args[0])
    })

    await new Promise(resolve => setTimeout(resolve, 0))
    n1.receive({ payload: { reconnectCall: true } })
    await new Promise(resolve => setTimeout(resolve, 50))

    expect(closeStub.calledOnce).to.be.true
    expect(callErrors.some(err => String(err).includes('ERR_INVALID_ARG_TYPE'))).to.be.false
  })

  it('does not create duplicate consumers when reconnect is requested during initialization', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      let resolveInitialConnect: (connection: unknown) => void = () => undefined
      const initialConnectPending = new Promise(resolve => {
        resolveInitialConnect = resolve
      })
      const initialConnection = {
        on: sinon.stub(),
        off: sinon.stub(),
        close: sinon.stub(),
      }
      const retryConnection = {
        on: sinon.stub(),
        off: sinon.stub(),
        close: sinon.stub(),
      }
      const channelMock = {
        on: sinon.stub(),
        off: sinon.stub(),
      }
      const connectStub = sinon.stub(Amqp.prototype, 'connect')
      connectStub.onFirstCall().returns(initialConnectPending as any)
      connectStub.onSecondCall().resolves(retryConnection as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
      const consumeStub = sinon.stub(Amqp.prototype, 'consume').resolves()
      sinon.stub(Amqp.prototype, 'close').resolves()

      await helper.load(
        [amqpIn, amqpBroker],
        amqpInFlowFixture,
        credentialsFixture,
      )

      const node = helper.getNode('n1')
      node.receive({ payload: { reconnectCall: true } })
      await clock.tickAsync(0)

      resolveInitialConnect(initialConnection)
      await clock.tickAsync(0)
      await clock.tickAsync(2001)

      expect(consumeStub.callCount).to.equal(1)
    } finally {
      clock.restore()
    }
  })

  it('should handle channel errors', function (done) {
    const flow = [
      { id: 'n1', type: 'amqp-in', name: 'test name', broker: 'b1' },
      { id: 'b1', type: 'amqp-broker', name: 'test broker' }
    ];
    helper.load([amqpIn, amqpBroker], flow, function () {
      const amqpInNode = helper.getNode('n1');
      const brokerNode = helper.getNode('b1');
      brokerNode.on('amqp-in-error', (err) => {
        try {
          expect(err.message).to.equal('Channel error');
          done();
        } catch (e) {
          done(e);
        }
      });
      brokerNode.emit('amqp-in-error', new Error('Channel error'));
    });
  });

  it('handles connection close', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon
      .stub(Amqp.prototype, 'connect')
      .resolves(connectionMock as any)
    sinon
      .stub(Amqp.prototype, 'initialize')
      .resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    const closeStub = sinon.stub(Amqp.prototype, 'close')

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture
    )
    // Get the 'on' callback for connection close
    const onCallback = connectionMock.on.withArgs('close').getCall(0).args[1]
    await onCallback('connection closed')
    expect(closeStub.calledOnce).to.be.true
  })

  it('reconnects on connection close without error argument', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon
      .stub(Amqp.prototype, 'connect')
      .resolves(connectionMock as any)
    sinon
      .stub(Amqp.prototype, 'initialize')
      .resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    const closeStub = sinon.stub(Amqp.prototype, 'close')

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture
    )
    const onCallback = connectionMock.on.withArgs('close').getCall(0).args[1]
    onCallback()
    expect(closeStub.calledOnce).to.be.true
  })

  it('does not reconnect after node close when late connection close event arrives', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture
    )
    const n1 = helper.getNode('n1')
    await n1.close()

    const onCallback = connectionMock.on.withArgs('close').getCall(0).args[1]
    await onCallback('late connection close')

    expect(closeStub.calledOnce).to.be.true
  })

  it('removes broker node state when delete close fails', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub(), consume: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    sinon.stub(Amqp.prototype, 'close').rejects(new Error('close failed'))
    const removeBrokerNodeStateStub = sinon.stub(Amqp.prototype, 'removeBrokerNodeState')

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
    )
    const amqpInNode = helper.getNode('n1')
    await amqpInNode.close(true)

    expect(removeBrokerNodeStateStub.calledOnce).to.be.true
  })

  it('removes broker node state when node is permanently deleted', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub(), consume: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()
    const removeBrokerNodeStateStub = sinon.stub(Amqp.prototype, 'removeBrokerNodeState')

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
    )
    const amqpInNode = helper.getNode('n1')
    await amqpInNode.close(true)

    expect(closeStub.calledOnceWith({ removeBindings: true })).to.be.true
    expect(removeBrokerNodeStateStub.calledOnce).to.be.true
  })

  it('removes durable bindings when the node configuration changes', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub(), consume: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
    )
    helper._events.emit('flows:stopping', {
      type: 'nodes',
      diff: { changed: ['n1'], removed: [] },
    })

    const node = helper.getNode('n1')
    await node.close(false)

    expect(closeStub.calledOnceWith({ removeBindings: true })).to.be.true
  })

  it('does not schedule reconnect when node closes while reconnect is closing resources', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
      const channelMock = { on: sinon.stub(), off: sinon.stub() }
      const connectStub = sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
      sinon.stub(Amqp.prototype, 'consume').resolves()
      let releaseClose: () => void = () => undefined
      const closeDuringReconnect = new Promise<void>(resolve => {
        releaseClose = resolve
      })
      const closeStub = sinon.stub(Amqp.prototype, 'close')
      closeStub.onFirstCall().returns(closeDuringReconnect)
      closeStub.onSecondCall().resolves()

      await helper.load(
        [amqpIn, amqpBroker],
        amqpInFlowFixture,
        credentialsFixture,
      )

      const n1 = helper.getNode('n1')
      const onConnClose = connectionMock.on.withArgs('close').getCall(0).args[1]
      const reconnectPromise = onConnClose()
      await n1.close()
      releaseClose()
      await reconnectPromise
      await clock.tickAsync(2001)

      expect(connectStub.calledOnce).to.be.true
    } finally {
      clock.restore()
    }
  })

  it('coalesces duplicate reconnect triggers into a single reconnect cycle', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    const connectStub = sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture
    )

    const onConnClose = connectionMock.on.withArgs('close').getCall(0).args[1]
    const onChannelClose = channelMock.on.withArgs('close').getCall(0).args[1]

    await onConnClose()
    await onChannelClose()
    expect(closeStub.calledOnce).to.be.true

    await clock.tickAsync(2001)
    expect(connectStub.callCount).to.equal(2)
    clock.restore()
  })

  it('does not queue duplicate initialization after the reconnect timer fires', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
      const channelMock = { on: sinon.stub(), off: sinon.stub() }
      const connectStub = sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
      const initializeStub = sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
      const consumeStub = sinon.stub(Amqp.prototype, 'consume').resolves()
      sinon
        .stub(Amqp.prototype, 'markConnected')
        .onFirstCall()
        .throws(new Error('hold initial initialization open'))

      let releaseInitialCleanup: () => void = () => undefined
      const initialCleanup = new Promise<void>(resolve => {
        releaseInitialCleanup = resolve
      })
      const closeStub = sinon.stub(Amqp.prototype, 'close')
      closeStub.onFirstCall().returns(initialCleanup)
      closeStub.resolves()

      await helper.load(
        [amqpIn, amqpBroker],
        amqpInFlowFixture,
        credentialsFixture,
      )

      const onConnClose = connectionMock.on.withArgs('close').getCall(0).args[1]
      await onConnClose()
      await clock.tickAsync(2001)

      await onConnClose()
      await clock.tickAsync(4001)

      releaseInitialCleanup()
      await clock.tickAsync(0)

      expect(connectStub.callCount).to.equal(2)
      expect(initializeStub.callCount).to.equal(2)
      expect(consumeStub.callCount).to.equal(2)
    } finally {
      clock.restore()
    }
  })

  it('does not start a queued reconnect initialization after shutdown', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      let releaseInitialConnect: (connection: unknown) => void = () => undefined
      const initialConnect = new Promise(resolve => {
        releaseInitialConnect = resolve
      })
      const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
      const connectStub = sinon.stub(Amqp.prototype, 'connect')
      connectStub.onFirstCall().returns(initialConnect as any)
      connectStub.onSecondCall().resolves(connectionMock as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves({ on: sinon.stub(), off: sinon.stub() } as any)
      sinon.stub(Amqp.prototype, 'consume').resolves()
      sinon.stub(Amqp.prototype, 'close').resolves()

      await helper.load(
        [amqpIn, amqpBroker],
        amqpInFlowFixture,
        credentialsFixture,
      )
      const n1 = helper.getNode('n1')

      await n1.receive({ payload: { reconnectCall: true } })
      await clock.tickAsync(2_001)
      await n1.close()

      releaseInitialConnect(connectionMock)
      await clock.tickAsync(0)

      expect(connectStub.calledOnce).to.be.true
    } finally {
      clock.restore()
    }
  })

  it('manual reconnect aborts a stalled startup connection', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
      const channelMock = { on: sinon.stub(), off: sinon.stub() }
      let firstAborted = false
      const connectStub = sinon.stub(Amqp.prototype, 'connect')
      connectStub.onFirstCall().callsFake(
        (options?: { signal?: AbortSignal }) =>
          new Promise((_resolve, reject) => {
            options?.signal?.addEventListener('abort', () => {
              firstAborted = true
              reject(new Error('startup connection cancelled'))
            }, { once: true })
          }) as any,
      )
      connectStub.onSecondCall().resolves(connectionMock as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
      sinon.stub(Amqp.prototype, 'consume').resolves()
      sinon.stub(Amqp.prototype, 'close').resolves()

      await helper.load(
        [amqpIn, amqpBroker],
        amqpInFlowFixture,
        credentialsFixture,
      )
      const n1 = helper.getNode('n1')

      await n1.receive({ payload: { reconnectCall: true } })
      expect(firstAborted).to.be.true
      await clock.tickAsync(2_001)

      expect(connectStub.calledTwice).to.be.true
    } finally {
      clock.restore()
    }
  })

  it('manual reconnect aborts stalled startup channel creation', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
      const channelMock = { on: sinon.stub(), off: sinon.stub() }
      sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
      let notifyInitializationStarted: () => void = () => undefined
      const initializationStarted = new Promise<void>(resolve => {
        notifyInitializationStarted = resolve
      })
      let firstAborted = false
      const initializeStub = sinon.stub(Amqp.prototype, 'initialize')
      initializeStub.onFirstCall().callsFake(
        (options?: { signal?: AbortSignal }) =>
          new Promise((_resolve, reject) => {
            notifyInitializationStarted()
            options?.signal?.addEventListener('abort', () => {
              firstAborted = true
              reject(new Error('startup channel cancelled'))
            }, { once: true })
          }) as any,
      )
      initializeStub.onSecondCall().resolves(channelMock as any)
      sinon.stub(Amqp.prototype, 'consume').resolves()
      sinon.stub(Amqp.prototype, 'close').resolves()

      await helper.load(
        [amqpIn, amqpBroker],
        amqpInFlowFixture,
        credentialsFixture,
      )
      await initializationStarted
      const n1 = helper.getNode('n1')

      await n1.receive({ payload: { reconnectCall: true } })
      expect(firstAborted).to.be.true
      await clock.tickAsync(2_001)

      expect(initializeStub.calledTwice).to.be.true
    } finally {
      clock.restore()
    }
  })

  it('handles channel close', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() };
    const channelMock = { on: sinon.stub(), off: sinon.stub() };
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any);
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any);
    sinon.stub(Amqp.prototype, 'consume').resolves();
    const closeStub = sinon.stub(Amqp.prototype, 'close');

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture
    );
    const onCallback = channelMock.on.withArgs('close').getCall(0).args[1];
    await onCallback('channel closed');
    expect(closeStub.calledOnce).to.be.true;
  });

  it('reconnects on channel close even when reconnectOnError is false', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() };
    const channelMock = { on: sinon.stub(), off: sinon.stub() };
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any);
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any);
    sinon.stub(Amqp.prototype, 'consume').resolves();
    const closeStub = sinon.stub(Amqp.prototype, 'close');

    const flow = JSON.parse(JSON.stringify(amqpInFlowFixture));
    flow[0].reconnectOnError = false;

    await helper.load(
      [amqpIn, amqpBroker],
      flow,
      credentialsFixture
    );
    const onCallback = channelMock.on.withArgs('close').getCall(0).args[1];
    await onCallback('channel closed');
    expect(closeStub.calledOnce).to.be.true;
  });

  it('reconnects when AMQP consumer is cancelled', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    const closeStub = sinon.stub(Amqp.prototype, 'close')

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture
    )
    const amqpInNode = helper.getNode('n1')
    amqpInNode.emit('amqp:consumer-cancelled')
    await new Promise(resolve => setImmediate(resolve))
    expect(closeStub.calledOnce).to.be.true
  })

  it('handles channel error', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() };
    const channelMock = { on: sinon.stub(), off: sinon.stub() };
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any);
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any);
    sinon.stub(Amqp.prototype, 'consume').resolves();
    const closeStub = sinon.stub(Amqp.prototype, 'close');

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      async function () {
        const amqpInNode = helper.getNode('n1');
        const onCallback = channelMock.on.withArgs('error').getCall(0).args[1];

        let errorCalled = false;
        amqpInNode.on('call:error', () => {
          errorCalled = true;
        });

        await onCallback('channel error');

        expect(errorCalled).to.be.true;
        expect(closeStub.calledOnce).to.be.true;
      }
    );
  });

  it('does not reconnect on connection error when reconnectOnError is false', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() };
    const channelMock = { on: sinon.stub(), off: sinon.stub() };
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any);
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any);
    sinon.stub(Amqp.prototype, 'consume').resolves();
    const closeStub = sinon.stub(Amqp.prototype, 'close');

    const flow = JSON.parse(JSON.stringify(amqpInFlowFixture));
    flow[0].reconnectOnError = false;

    await helper.load(
      [amqpIn, amqpBroker],
      flow,
      credentialsFixture
    );
    const amqpInNode = helper.getNode('n1');

    const errorPromise = new Promise<void>(resolve => {
      amqpInNode.on('call:error', () => {
        resolve();
      });
    });

    const onCallback = connectionMock.on.withArgs('error').getCall(0).args[1];
    onCallback('connection error');

    await errorPromise;

    expect(closeStub.notCalled).to.be.true;
  });

  it('retries initialization on unclassified connect errors when reconnectOnError is true', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const transientError = new Error('socket hang up')
      const connectStub = sinon.stub(Amqp.prototype, 'connect')
      connectStub.onFirstCall().rejects(transientError)
      connectStub.onSecondCall().resolves({ on: sinon.stub(), off: sinon.stub(), close: sinon.stub() } as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves({ on: sinon.stub(), off: sinon.stub() } as any)
      sinon.stub(Amqp.prototype, 'consume').resolves()
      sinon.stub(Amqp.prototype, 'close').resolves()

      const flow = JSON.parse(JSON.stringify(amqpInFlowFixture))
      flow[0].reconnectOnError = true

      await helper.load([amqpIn, amqpBroker], flow, credentialsFixture)
      expect(connectStub.callCount).to.equal(1)

      await clock.tickAsync(2001)

      expect(connectStub.callCount).to.equal(2)
    } finally {
      clock.restore()
    }
  })

  it('retries initialization on auth errors when reconnectOnError is true', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const connectStub = sinon.stub(Amqp.prototype, 'connect')
      connectStub.onFirstCall().rejects(new CustomError(ErrorType.InvalidLogin))
      connectStub.onSecondCall().resolves({ on: sinon.stub(), off: sinon.stub(), close: sinon.stub() } as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves({ on: sinon.stub(), off: sinon.stub() } as any)
      sinon.stub(Amqp.prototype, 'consume').resolves()
      sinon.stub(Amqp.prototype, 'close').resolves()

      const flow = JSON.parse(JSON.stringify(amqpInFlowFixture))
      flow[0].reconnectOnError = true

      await helper.load([amqpIn, amqpBroker], flow, credentialsFixture)
      expect(connectStub.callCount).to.equal(1)

      await clock.tickAsync(2001)

      expect(connectStub.callCount).to.equal(2)
    } finally {
      clock.restore()
    }
  })

  it('does not emit unhandled rejection when reconnect fails during initialization error handling', async function () {
    sinon.stub(Amqp.prototype, 'connect').rejects(new Error('initial connect failed'))
    sinon.stub(Amqp.prototype, 'initialize').resolves({ on: sinon.stub(), off: sinon.stub() } as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()
    sinon.stub(Amqp.prototype, 'close').rejects(new Error('close failed'))

    const flow = JSON.parse(JSON.stringify(amqpInFlowFixture))
    flow[0].reconnectOnError = true

    let unhandledReason: unknown
    const onUnhandledRejection = (reason: unknown) => {
      unhandledReason = reason
    }
    process.once('unhandledRejection', onUnhandledRejection)
    try {
      await helper.load([amqpIn, amqpBroker], flow, credentialsFixture)
      const amqpInNode = helper.getNode('n1')
      const callErrors: unknown[] = []
      const callStatuses: unknown[] = []
      amqpInNode.on('call:error', call => {
        callErrors.push(call.args[0])
      })
      amqpInNode.on('call:status', call => {
        callStatuses.push(call.args[0])
      })

      await new Promise(resolve => setTimeout(resolve, 50))

      expect(unhandledReason).to.equal(undefined)
      expect(
        callErrors.some(error => /Reconnect failed during initialization/i.test(String(error))),
      ).to.be.true
      expect(
        callStatuses.some(
          status => status && typeof status === 'object' && (status as { text?: string }).text === NODE_STATUS.Error.text,
        ),
      ).to.be.true
    } finally {
      process.removeListener('unhandledRejection', onUnhandledRejection)
    }
  })

  it('backs off repeated reconnect initialization failures', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const transientError = new Error('socket hang up')
      const connectStub = sinon.stub(Amqp.prototype, 'connect')
      connectStub.onFirstCall().rejects(transientError)
      connectStub.onSecondCall().rejects(transientError)
      connectStub.onThirdCall().resolves({ on: sinon.stub(), off: sinon.stub(), close: sinon.stub() } as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves({ on: sinon.stub(), off: sinon.stub() } as any)
      sinon.stub(Amqp.prototype, 'consume').resolves()
      sinon.stub(Amqp.prototype, 'close').resolves()

      const flow = JSON.parse(JSON.stringify(amqpInFlowFixture))
      flow[0].reconnectOnError = true

      await helper.load([amqpIn, amqpBroker], flow, credentialsFixture)
      expect(connectStub.callCount).to.equal(1)

      await clock.tickAsync(2001)
      expect(connectStub.callCount).to.equal(2)

      await clock.tickAsync(2000)
      expect(connectStub.callCount).to.equal(2)

      await clock.tickAsync(2000)
      expect(connectStub.callCount).to.equal(3)
    } finally {
      clock.restore()
    }
  })

  it('keeps retrying after broker-close recovery fails with reconnectOnError disabled', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const initialConnection = {
        on: sinon.stub(),
        off: sinon.stub(),
        close: sinon.stub(),
      }
      const recoveredConnection = {
        on: sinon.stub(),
        off: sinon.stub(),
        close: sinon.stub(),
      }
      const channelMock = { on: sinon.stub(), off: sinon.stub() }
      const connectStub = sinon.stub(Amqp.prototype, 'connect')
      connectStub.onFirstCall().resolves(initialConnection as any)
      connectStub
        .onSecondCall()
        .rejects(new Error('broker is still restarting'))
      connectStub.onThirdCall().resolves(recoveredConnection as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
      sinon.stub(Amqp.prototype, 'consume').resolves()
      sinon.stub(Amqp.prototype, 'close').resolves()

      const flow = JSON.parse(JSON.stringify(amqpInFlowFixture))
      flow[0].reconnectOnError = false

      await helper.load([amqpIn, amqpBroker], flow, credentialsFixture)
      const onConnectionClose = initialConnection.on
        .withArgs('close')
        .getCall(0).args[1]

      await onConnectionClose()
      await clock.tickAsync(2001)
      expect(connectStub.callCount).to.equal(2)

      await clock.tickAsync(4001)
      expect(connectStub.callCount).to.equal(3)
    } finally {
      clock.restore()
    }
  })

  it('does not auto-ack when noAck is false', async function () {
    const ackStub = sinon.stub(Amqp.prototype, 'ack');
    sinon.stub(Amqp.prototype, 'consume').callsFake(async function () {
      // @ts-ignore
      this.node.send({ payload: 'test' });
    });

    const flow = JSON.parse(JSON.stringify(amqpInFlowFixture));
    flow[1].noAck = false;
    flow.push({ id: 'n2', type: 'helper' });
    flow[0].wires = [['n2']];

    await helper.load([amqpIn, amqpBroker], flow, credentialsFixture, async function () {
      const n2 = helper.getNode('n2');
      await new Promise<void>(resolve => {
        n2.on('input', () => {
          expect(ackStub.calledOnce).to.be.true;
          resolve();
        });
      });
    });
  });
})
