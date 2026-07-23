/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/ban-ts-comment */
export {}
const { expect } = require('chai')
const { NODE_STATUS } = require('../src/constants')
const sinon = require('sinon')
const amqplib = require('amqplib')
const { util: redUtil } = require('@node-red/util')
const Amqp = require('../src/Amqp').default
const {
  nodeConfigFixture,
  nodeFixture,
  brokerConfigFixture,
} = require('./doubles')
const { ExchangeType, DefaultExchangeName, NodeType } = require('../src/types')
import type { GenericJsonObject, BrokerConfig } from '../src/types'

let RED: any
let amqp: any

describe('Amqp Class', () => {
  beforeEach(function (done) {
    RED = {
      nodes: {
        getNode: sinon.stub().returns(brokerConfigFixture),
      },
    }

    ;(Amqp as any).connectionPool.clear()

    // @ts-ignore
    amqp = new Amqp(RED, nodeFixture, nodeConfigFixture)
    done()
  })

  afterEach(function (done) {
    sinon.restore()
    done()
  })

  it('constructs with default Direct exchange', () => {
    // @ts-ignore
    amqp = new Amqp(RED, nodeFixture, {
      ...nodeConfigFixture,
      exchangeType: ExchangeType.Direct,
      exchangeName: DefaultExchangeName.Direct,
    })
    expect(amqp.config.exchange.name).to.eq(DefaultExchangeName.Direct)
  })

  it('constructs with default Fanout exchange', () => {
    // @ts-ignore
    amqp = new Amqp(RED, nodeFixture, {
      ...nodeConfigFixture,
      exchangeType: ExchangeType.Fanout,
      exchangeName: DefaultExchangeName.Fanout,
    })
    expect(amqp.config.exchange.name).to.eq(DefaultExchangeName.Fanout)
  })

  it('constructs with default Topic exchange', () => {
    // @ts-ignore
    amqp = new Amqp(RED, nodeFixture, {
      ...nodeConfigFixture,
      exchangeType: ExchangeType.Topic,
      exchangeName: DefaultExchangeName.Topic,
    })
    expect(amqp.config.exchange.name).to.eq(DefaultExchangeName.Topic)
  })

  it('constructs with default Headers exchange', () => {
    // @ts-ignore
    amqp = new Amqp(RED, nodeFixture, {
      ...nodeConfigFixture,
      exchangeType: ExchangeType.Headers,
      exchangeName: DefaultExchangeName.Headers,
    })
    expect(amqp.config.exchange.name).to.eq(DefaultExchangeName.Headers)
  })

  it('defaults auto-create queue and exchange bindings to false when omitted', () => {
    // @ts-ignore
    amqp = new Amqp(RED, nodeFixture, {
      ...nodeConfigFixture,
      autoCreateQueue: undefined,
      autoCreateExchangeBindings: undefined,
    })

    expect(amqp.config.queue.autoCreate).to.equal(false)
    expect(amqp.config.exchange.autoCreate).to.equal(false)
  })

  it('connect() logs attempts', async () => {
    const error = 'error!'
    const result = { on: sinon.stub() }

    // @ts-ignore
    const connectStub = sinon.stub(amqplib, 'connect').resolves(result)

    const logStub = sinon.stub()
    const warnStub = sinon.stub()
    amqp.node = { ...nodeFixture, log: logStub, warn: warnStub }

    const connection = await amqp.connect()

    expect(connection).to.eq(result)
    expect(connectStub.calledOnce).to.be.true
    expect(
      logStub.calledWithMatch(/Connecting to AMQP broker/),
    ).to.be.true
    expect(logStub.calledWithMatch(/Connected to AMQP broker/)).to.be.true
    expect(warnStub.called).to.be.false
  })

  it('connect() logs events', async () => {
    const events: { [key: string]: Function } = {}
    const result = { on: (ev: string, cb: Function): void => { events[ev] = cb } }

    // @ts-ignore
    sinon.stub(amqplib, 'connect').resolves(result)

    const logStub = sinon.stub()
    const warnStub = sinon.stub()
    amqp.node = { ...nodeFixture, log: logStub, warn: warnStub }

    await amqp.connect()

    events['error']('err')
    events['close']()

    expect(warnStub.calledWithMatch('AMQP connection error')).to.be.true
    expect(logStub.calledWithMatch('AMQP Connection closed')).to.be.true
  })

  it('connect() sets broker node state to errored on connection error event', async () => {
    const events: { [key: string]: Function } = {}
    const result = { on: (ev: string, cb: Function): void => { events[ev] = cb } }
    const broker = {
      ...brokerConfigFixture,
      vhost: 'vh1',
      nodeStates: {},
      lastError: {},
    }
    RED.nodes.getNode.returns(broker)
    amqp.node = { ...nodeFixture, id: 'n1', log: sinon.stub(), warn: sinon.stub(), status: sinon.stub() }
    sinon.stub(amqplib, 'connect').resolves(result as any)

    await amqp.connect()
    events['error'](new Error('simulated connection failure'))

    expect(broker.nodeStates.n1).to.equal('errored')
    expect(broker.lastError.n1?.message).to.equal('simulated connection failure')
  })

  it('connect() sets broker node state to disconnected and keeps cause on connection close event', async () => {
    const events: { [key: string]: Function } = {}
    const result = { on: (ev: string, cb: Function): void => { events[ev] = cb } }
    const broker = {
      ...brokerConfigFixture,
      vhost: 'vh1',
      nodeStates: {},
      lastError: {},
    }
    RED.nodes.getNode.returns(broker)
    amqp.node = { ...nodeFixture, id: 'n1', log: sinon.stub(), warn: sinon.stub(), status: sinon.stub() }
    sinon.stub(amqplib, 'connect').resolves(result as any)

    await amqp.connect()
    events['close']()

    expect(broker.nodeStates.n1).to.equal('disconnected')
    expect(broker.lastError.n1?.message).to.equal('AMQP connection closed')
  })

  it('connect() does not mark node connected before channel and consumer setup complete', async () => {
    const result = { on: sinon.stub() }
    const broker = {
      ...brokerConfigFixture,
      vhost: 'vh1',
      nodeStates: {},
      lastError: {},
    }
    const statusStub = sinon.stub()

    RED.nodes.getNode.returns(broker)
    amqp.node = { ...nodeFixture, id: 'n1', log: sinon.stub(), warn: sinon.stub(), status: statusStub }
    sinon.stub(amqplib, 'connect').resolves(result as any)

    await amqp.connect()

    expect(statusStub.calledWith(NODE_STATUS.Connected)).to.equal(false)
    expect(broker.nodeStates.n1).to.equal(undefined)
  })

  it('connect() errors when broker missing', async () => {
    // no broker node returned
    RED.nodes.getNode.returns(undefined)

    const errorStub = sinon.stub()
    amqp.node = { ...nodeFixture, error: errorStub }

    try {
      await amqp.connect()
      expect.fail('connect did not throw')
    } catch (err) {
      expect(errorStub.calledWithMatch('AMQP broker node not found')).to.be.true
    }
  })

  describe('getBrokerUrl()', () => {
    it('encodes credentials and vhost', () => {
      const broker = {
        host: 'localhost',
        port: 5672,
        vhost: 'foo/bar',
        tls: false,
        credsFromSettings: false,
        credentials: { username: 'user@name', password: 'p@ss/word' },
      }

      const url = (amqp as any).getBrokerUrl(broker)

      expect(url).to.equal(
        'amqp://user%40name:p%40ss%2Fword@localhost:5672/foo%2Fbar',
      )
    })

    it('falls back to root when vhost missing', () => {
      const broker = { ...brokerConfigFixture, vhost: undefined }

      const url = (amqp as any).getBrokerUrl(broker)

      expect(url).to.equal('amqp://username:password@host:222/')
    })

    it('uses credentials from settings when configured', () => {
      RED.settings = {
        MW_CONTRIB_AMQP_USERNAME: 'settings user',
        MW_CONTRIB_AMQP_PASSWORD: 'settings/password',
      }
      const broker = {
        ...brokerConfigFixture,
        vhost: 'vhost',
        credsFromSettings: true,
        credentials: { username: 'ignored', password: 'ignored' },
      }

      const url = (amqp as any).getBrokerUrl(broker)

      expect(url).to.equal('amqp://settings%20user:settings%2Fpassword@host:222/vhost')
    })
  })

  it('markConnected() initializes missing broker state maps', () => {
    const statusStub = sinon.stub()
    amqp.node = { ...nodeFixture, id: 'n1', status: statusStub }
    amqp.broker = { ...brokerConfigFixture }
    delete amqp.broker.nodeStates
    delete amqp.broker.lastError

    amqp.markConnected()

    expect(amqp.broker.nodeStates.n1).to.equal('connected')
    expect(amqp.broker.lastError).to.deep.equal({})
    expect(statusStub.calledWith(NODE_STATUS.Connected)).to.equal(true)
  })

  it('removeBrokerNodeState() removes node runtime state and last error', () => {
    amqp.node = { ...nodeFixture, id: 'n1' }
    amqp.broker = {
      ...brokerConfigFixture,
      nodeStates: { n1: 'disconnected', n2: 'connected' },
      lastError: {
        n1: { message: 'stale', at: new Date().toISOString() },
        n2: { message: 'active', at: new Date().toISOString() },
      },
    }

    amqp.removeBrokerNodeState()

    expect(amqp.broker.nodeStates.n1).to.equal(undefined)
    expect(amqp.broker.lastError.n1).to.equal(undefined)
    expect(amqp.broker.nodeStates.n2).to.equal('connected')
    expect(amqp.broker.lastError.n2?.message).to.equal('active')
  })

  it('setBrokerNodeState() preserves lastError when transitioning to disconnected without new error', () => {
    amqp.node = { ...nodeFixture, id: 'n1' }
    amqp.broker = {
      ...brokerConfigFixture,
      nodeStates: { n1: 'errored' },
      lastError: { n1: { message: 'connection dropped', at: new Date().toISOString() } },
    }

    ;(amqp as any).setBrokerNodeState('disconnected')

    expect(amqp.broker.nodeStates.n1).to.equal('disconnected')
    expect(amqp.broker.lastError.n1?.message).to.equal('connection dropped')
  })

  it('setBrokerNodeState() clears lastError when transitioning to connected', () => {
    amqp.node = { ...nodeFixture, id: 'n1' }
    amqp.broker = {
      ...brokerConfigFixture,
      nodeStates: { n1: 'disconnected' },
      lastError: { n1: { message: 'previous error', at: new Date().toISOString() } },
    }

    ;(amqp as any).setBrokerNodeState('connected')

    expect(amqp.broker.nodeStates.n1).to.equal('connected')
    expect(amqp.broker.lastError.n1).to.equal(undefined)
  })

  it('shares connection among instances for same vhost', async () => {
    const connectionStub = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub().resolves(),
    }
    const connectStub = sinon
      .stub(amqplib, 'connect')
      .resolves(connectionStub as any)

    const amqp1: any = new Amqp(RED, nodeFixture, nodeConfigFixture)
    const amqp2: any = new Amqp(RED, nodeFixture, nodeConfigFixture)

    await amqp1.connect()
    await amqp2.connect()

    expect(connectStub.calledOnce).to.be.true

    await amqp1.close()
    expect(connectionStub.close.called).to.be.false

    await amqp2.close()
    expect(connectionStub.close.calledOnce).to.be.true
  })

  it('shares one connection when instances connect concurrently for the same vhost', async () => {
    let releaseConnect: () => void = () => undefined
    const connectPending = new Promise<void>(resolve => {
      releaseConnect = resolve
    })
    const connectionStub = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub().resolves(),
      connection: { stream: { destroyed: false } },
    }
    const connectStub = sinon.stub(amqplib, 'connect').callsFake(async () => {
      await connectPending
      return connectionStub as any
    })
    const broker = { ...brokerConfigFixture, vhost: 'vh1' }
    RED.nodes.getNode.returns(broker)

    const amqp1: any = new Amqp(
      RED,
      { ...nodeFixture, id: 'n1' },
      nodeConfigFixture,
    )
    const amqp2: any = new Amqp(
      RED,
      { ...nodeFixture, id: 'n2' },
      nodeConfigFixture,
    )

    const firstConnection = amqp1.connect()
    const secondConnection = amqp2.connect()
    await Promise.resolve()
    releaseConnect()

    const connections = await Promise.all([firstConnection, secondConnection])
    const poolEntry = (Amqp as any).connectionPool.get('b1:vh1')

    expect(connectStub.calledOnce).to.be.true
    expect(connections).to.deep.equal([connectionStub, connectionStub])
    expect(poolEntry.connection).to.equal(connectionStub)
    expect(poolEntry.count).to.equal(2)

    await amqp1.close()
    expect(connectionStub.close.called).to.be.false

    await amqp2.close()
    expect(connectionStub.close.calledOnce).to.be.true
  })

  it('shares one connection but uses separate channels for manual-ack input and output instances', async () => {
    const manualAckChannel = {
      prefetch: sinon.stub(),
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub().resolves(),
    }
    const outputChannel = {
      prefetch: sinon.stub(),
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub().resolves(),
    }
    const createChannelStub = sinon.stub()
    createChannelStub.onFirstCall().resolves(manualAckChannel)
    createChannelStub.onSecondCall().resolves(outputChannel)
    const connectionStub = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub().resolves(),
      createChannel: createChannelStub,
      connection: { stream: { destroyed: false } },
    }
    const connectStub = sinon
      .stub(amqplib, 'connect')
      .resolves(connectionStub as any)
    const manualAckAmqp: any = new Amqp(
      RED,
      {
        ...nodeFixture,
        id: 'manual-in',
        type: NodeType.AmqpInManualAck,
      },
      nodeConfigFixture,
    )
    const outputAmqp: any = new Amqp(
      RED,
      {
        ...nodeFixture,
        id: 'out',
        type: NodeType.AmqpOut,
      },
      nodeConfigFixture,
    )

    await Promise.all([manualAckAmqp.connect(), outputAmqp.connect()])
    const channels = await Promise.all([
      manualAckAmqp.initialize(),
      outputAmqp.initialize(),
    ])

    expect(connectStub.calledOnce).to.be.true
    expect(channels).to.deep.equal([manualAckChannel, outputChannel])
    expect(createChannelStub.calledTwice).to.be.true

    await manualAckAmqp.close()
    expect(manualAckChannel.close.calledOnce).to.be.true
    expect(outputChannel.close.called).to.be.false
    expect(connectionStub.close.called).to.be.false

    await outputAmqp.close()
    expect(outputChannel.close.calledOnce).to.be.true
    expect(connectionStub.close.calledOnce).to.be.true
  })

  it('acknowledges the original delivery after Node-RED clones the message', () => {
    const node = {
      ...nodeFixture,
      id: 'manual-in',
      type: NodeType.AmqpInManualAck,
      error: sinon.stub(),
    }
    const manualAckAmqp: any = new Amqp(RED, node, nodeConfigFixture)
    const channel = { ack: sinon.stub() }
    manualAckAmqp.channel = channel
    const delivery = manualAckAmqp.assembleMessage({
      content: Buffer.from('payload'),
      fields: { deliveryTag: 42 },
      properties: {},
    })
    const clonedDelivery = redUtil.cloneMessage(delivery)

    manualAckAmqp.ack(clonedDelivery)

    expect(clonedDelivery).to.not.equal(delivery)
    expect(channel.ack.calledOnceWith(delivery, false)).to.be.true
  })

  it('does not settle a pre-restart delivery on the replacement channel', () => {
    const node = {
      ...nodeFixture,
      id: 'manual-in',
      type: NodeType.AmqpInManualAck,
      error: sinon.stub(),
    }
    const manualAckAmqp: any = new Amqp(RED, node, nodeConfigFixture)
    const originalChannel = {
      ack: sinon.stub(),
      ackAll: sinon.stub(),
      nack: sinon.stub(),
      nackAll: sinon.stub(),
      reject: sinon.stub(),
    }
    const replacementChannel = {
      ack: sinon.stub(),
      ackAll: sinon.stub(),
      nack: sinon.stub(),
      nackAll: sinon.stub(),
      reject: sinon.stub(),
    }
    manualAckAmqp.channel = originalChannel
    const delivery = manualAckAmqp.assembleMessage({
      content: Buffer.from('payload'),
      fields: { deliveryTag: 42 },
      properties: {},
    })

    manualAckAmqp.channel = replacementChannel
    manualAckAmqp.ack(delivery)
    manualAckAmqp.ackAll(delivery)
    manualAckAmqp.nack(delivery)
    manualAckAmqp.nackAll(delivery)
    manualAckAmqp.reject(delivery)

    expect(originalChannel.ack.called).to.be.false
    expect(originalChannel.ackAll.called).to.be.false
    expect(originalChannel.nack.called).to.be.false
    expect(originalChannel.nackAll.called).to.be.false
    expect(originalChannel.reject.called).to.be.false
    expect(replacementChannel.ack.called).to.be.false
    expect(replacementChannel.ackAll.called).to.be.false
    expect(replacementChannel.nack.called).to.be.false
    expect(replacementChannel.nackAll.called).to.be.false
    expect(replacementChannel.reject.called).to.be.false
  })

  it('does not acknowledge the same manual delivery twice', () => {
    const node = {
      ...nodeFixture,
      id: 'manual-in',
      type: NodeType.AmqpInManualAck,
      error: sinon.stub(),
    }
    const manualAckAmqp: any = new Amqp(RED, node, nodeConfigFixture)
    const channel = { ack: sinon.stub() }
    manualAckAmqp.channel = channel
    const delivery = manualAckAmqp.assembleMessage({
      content: Buffer.from('payload'),
      fields: { deliveryTag: 42 },
      properties: {},
    })

    manualAckAmqp.ack(delivery)
    manualAckAmqp.ack(delivery)

    expect(channel.ack.calledOnce).to.be.true
    expect(node.error.calledWithMatch('active AMQP channel')).to.be.true
  })

  it('reports a failed manual acknowledgement when the channel rejects it', () => {
    const node = {
      ...nodeFixture,
      id: 'manual-in',
      type: NodeType.AmqpInManualAck,
      error: sinon.stub(),
    }
    const manualAckAmqp: any = new Amqp(RED, node, nodeConfigFixture)
    const channel = {
      ack: sinon.stub().throws(new Error('channel is closed')),
    }
    manualAckAmqp.channel = channel
    const delivery = manualAckAmqp.assembleMessage({
      content: Buffer.from('payload'),
      fields: { deliveryTag: 42 },
      properties: {},
    })

    const acknowledged = manualAckAmqp.ack(delivery)

    expect(acknowledged).to.equal(false)
    expect(node.error.calledWithMatch('channel is closed')).to.be.true
  })

  it('does not reuse a dead pooled connection', async () => {
    const staleConnection = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub().resolves(),
      connection: { stream: { destroyed: true } },
    }
    const freshConnection = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub().resolves(),
      connection: { stream: { destroyed: false } },
    }
    ;(Amqp as any).connectionPool.set('b1:vh1', {
      connection: staleConnection,
      count: 1,
    })
    sinon.stub(amqplib, 'connect').resolves(freshConnection as any)

    amqp.broker = { ...brokerConfigFixture, vhost: 'vh1' }
    const connection = await amqp.connect()

    expect(connection).to.equal(freshConnection)
    expect(amqplib.connect.calledOnce).to.be.true
  })

  it('evicts pooled connection on close event', async () => {
    const events: { [key: string]: Function[] } = {}
    const connectionStub = {
      on: (event: string, cb: Function): void => {
        if (!events[event]) {
          events[event] = []
        }
        events[event].push(cb)
      },
      off: sinon.stub(),
      close: sinon.stub().resolves(),
      connection: { stream: { destroyed: false } },
    }
    sinon.stub(amqplib, 'connect').resolves(connectionStub as any)

    amqp.broker = { ...brokerConfigFixture, vhost: 'vh1' }
    await amqp.connect()

    expect((Amqp as any).connectionPool.size).to.equal(1)
    events.close.forEach(cb => cb())
    expect((Amqp as any).connectionPool.size).to.equal(0)
  })

  it('awaits connection close before removing from pool', async () => {
    let closed = false
    const connectionStub = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub().callsFake(
        () =>
          new Promise<void>(resolve =>
            setTimeout(() => {
              closed = true
              resolve()
            }, 5),
          ),
      ),
    }

    ;(Amqp as any).connectionPool.set('b1:vh1', {
      connection: connectionStub,
      count: 1,
    })

    amqp.connection = connectionStub as any
    amqp.broker = { ...brokerConfigFixture, vhost: 'vh1' }

    await (amqp as any).releaseConnection()

    expect(connectionStub.close.calledOnce).to.be.true
    expect(closed).to.be.true
    expect((Amqp as any).connectionPool.size).to.equal(0)
  })

  it('close() is idempotent', async () => {
    const connectionStub = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub().resolves(),
    }

    ;(Amqp as any).connectionPool.set('b1:vh1', {
      connection: connectionStub,
      count: 1,
    })

    amqp.connection = connectionStub as any
    amqp.broker = { ...brokerConfigFixture, vhost: 'vh1' }

    await amqp.close()
    await amqp.close()

    expect(connectionStub.close.calledOnce).to.be.true
    expect((Amqp as any).connectionPool.size).to.equal(0)
  })

  it('initialize()', async () => {
    const createChannelStub = sinon.stub()
    const assertExchangeStub = sinon.stub()

    amqp.createChannel = createChannelStub
    amqp.assertExchange = assertExchangeStub

    await amqp.initialize()
    expect(createChannelStub.calledOnce).to.equal(true)
    expect(assertExchangeStub.calledOnce).to.equal(false)
  })

  it('initialize() skips exchange assertion when auto-create exchange is disabled', async () => {
    amqp = new Amqp(RED, nodeFixture, {
      ...nodeConfigFixture,
      autoCreateExchangeBindings: false,
    })
    const createChannelStub = sinon.stub()
    const assertExchangeStub = sinon.stub()

    amqp.createChannel = createChannelStub
    amqp.assertExchange = assertExchangeStub

    await amqp.initialize()
    expect(createChannelStub.calledOnce).to.equal(true)
    expect(assertExchangeStub.called).to.equal(false)
  })

  it('initialize() asserts exchange when autoCreateExchangeBindings is true', async () => {
    amqp = new Amqp(RED, nodeFixture, {
      ...nodeConfigFixture,
      autoCreateExchangeBindings: true,
    })

    const createChannelStub = sinon.stub()
    const assertExchangeStub = sinon.stub()

    amqp.createChannel = createChannelStub
    amqp.assertExchange = assertExchangeStub

    await amqp.initialize()

    expect(createChannelStub.calledOnce).to.equal(true)
    expect(assertExchangeStub.calledOnce).to.equal(true)
  })

  it('initialize() skips exchange assertion by default', async () => {
    const createChannelStub = sinon.stub()
    const assertExchangeStub = sinon.stub()

    amqp.createChannel = createChannelStub
    amqp.assertExchange = assertExchangeStub

    await amqp.initialize()

    expect(createChannelStub.calledOnce).to.equal(true)
    expect(assertExchangeStub.calledOnce).to.equal(false)
  })


  it('nackAll() logs and delegates to channel', () => {
    const msg = { content: 'foo', manualAck: { requeue: true } }
    const nackAllStub = sinon.stub()
    const logStub = sinon.stub()
    amqp.channel = { nackAll: nackAllStub }
    amqp.node = { log: logStub }
    amqp.nackAll(msg as any)
    sinon.assert.calledOnce(nackAllStub)
    sinon.assert.calledWith(nackAllStub, true)
  })

  it('reject() logs and delegates to channel', () => {
    const msg = { content: 'foo', manualAck: { requeue: false } }
    const rejectStub = sinon.stub()
    const logStub = sinon.stub()
    amqp.channel = { reject: rejectStub }
    amqp.node = { log: logStub }
    amqp.reject(msg as any)
    sinon.assert.calledOnce(rejectStub)
    sinon.assert.calledWith(rejectStub, msg, false)
  })

  it('consume()', async () => {
    const assertQueueStub = sinon.stub()
    const bindQueueStub = sinon.stub()
    const messageContent = 'messageContent'
    const send = sinon.stub()
    const error = sinon.stub()
    const log = sinon.stub()
    const ack = sinon.stub()
    const node = { send, error, log }
    const channel = {
      consume: function (
        queue: string,
        cb: (arg0: any) => void,
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        config: GenericJsonObject,
      ): void {
        const amqpMessage = { content: messageContent, fields: { deliveryTag: 1 } }
        cb(amqpMessage)
      },
      ack,
    }
    amqp.channel = channel as any
    amqp.assertQueue = assertQueueStub
    amqp.bindQueue = bindQueueStub
    amqp.config.queue.name = 'queueName'
    amqp.config.queue.autoCreate = false
    amqp.config.exchange.autoCreate = false
    amqp.node = node as any

    await amqp.consume()
    expect(assertQueueStub.calledOnce).to.equal(false)
    expect(bindQueueStub.calledOnce).to.equal(false)
    expect(send.calledOnce).to.equal(true)
    expect(log.calledWithMatch('Received message')).to.equal(true)
    expect(ack.calledOnce).to.equal(true)
    expect(
      send.calledWith({
        content: messageContent,
        fields: { deliveryTag: 1 },
        payload: messageContent,
      }),
    ).to.equal(true)
  })

  it('consume() can use an existing queue without asserting queue or bindings', async () => {
    amqp = new Amqp(RED, nodeFixture, {
      ...nodeConfigFixture,
      autoCreateQueue: false,
      autoCreateExchangeBindings: false,
      queueName: 'existing.queue',
    })
    const assertQueueStub = sinon.stub()
    const bindQueueStub = sinon.stub()
    const checkQueueStub = sinon.stub()
    const consumeStub = sinon.stub()

    amqp.channel = {
      checkQueue: checkQueueStub,
      consume: consumeStub,
    } as any
    amqp.assertQueue = assertQueueStub
    amqp.bindQueue = bindQueueStub
    amqp.node = { send: sinon.stub(), error: sinon.stub(), log: sinon.stub() }

    await amqp.consume()

    expect(assertQueueStub.called).to.equal(false)
    expect(bindQueueStub.called).to.equal(false)
    expect(checkQueueStub.called).to.equal(false)
    expect(consumeStub.calledOnce).to.equal(true)
    expect(consumeStub.firstCall.args[0]).to.equal('existing.queue')
  })

  it('consume() can bind an existing queue when auto-create queue is disabled but exchange binding is enabled', async () => {
    amqp = new Amqp(RED, nodeFixture, {
      ...nodeConfigFixture,
      autoCreateQueue: false,
      autoCreateExchangeBindings: true,
      queueName: 'existing.queue',
    })
    const assertQueueStub = sinon.stub()
    const bindQueueStub = sinon.stub()
    const consumeStub = sinon.stub()

    amqp.channel = { consume: consumeStub } as any
    amqp.assertQueue = assertQueueStub
    amqp.bindQueue = bindQueueStub
    amqp.node = { send: sinon.stub(), error: sinon.stub(), log: sinon.stub() }

    await amqp.consume()

    expect(assertQueueStub.called).to.equal(false)
    expect(bindQueueStub.calledOnce).to.equal(true)
    expect(consumeStub.calledOnce).to.equal(true)
    expect(consumeStub.firstCall.args[0]).to.equal('existing.queue')
  })

  it('consume() uses existing queue by default', async () => {
    amqp = new Amqp(RED, nodeFixture, {
      ...nodeConfigFixture,
      queueName: 'existing.queue',
      autoCreateQueue: false,
      autoCreateExchangeBindings: false,
    })

    const assertQueueStub = sinon.stub()
    const bindQueueStub = sinon.stub()
    const consumeStub = sinon.stub()

    amqp.channel = { consume: consumeStub } as any
    amqp.assertQueue = assertQueueStub
    amqp.bindQueue = bindQueueStub
    amqp.node = { send: sinon.stub(), error: sinon.stub(), log: sinon.stub() }

    await amqp.consume()

    expect(assertQueueStub.called).to.equal(false)
    expect(bindQueueStub.called).to.equal(false)
    expect(consumeStub.calledOnce).to.equal(true)
    expect(consumeStub.firstCall.args[0]).to.equal('existing.queue')
  })

  it('consume() requires a named queue when auto-create queue is disabled', async () => {
    amqp = new Amqp(RED, nodeFixture, {
      ...nodeConfigFixture,
      autoCreateQueue: false,
      queueName: '',
    })
    const checkQueueStub = sinon.stub()
    const consumeStub = sinon.stub()
    const errorStub = sinon.stub()

    amqp.channel = { checkQueue: checkQueueStub, consume: consumeStub } as any
    amqp.node = { send: sinon.stub(), error: errorStub, log: sinon.stub() }

    try {
      await amqp.consume()
      expect.fail('consume should throw')
    } catch (e: any) {
      expect(String(e.message)).to.match(/Queue Name is required/)
    }

    expect(checkQueueStub.called).to.equal(false)
    expect(consumeStub.called).to.equal(false)
    expect(errorStub.calledWithMatch('Could not consume message')).to.equal(true)
  })

  it('consume() ignores null consumer delivery without throwing', async () => {
    const assertQueueStub = sinon.stub().resolves()
    const bindQueueStub = sinon.stub().resolves()
    const send = sinon.stub()
    const error = sinon.stub()
    const warn = sinon.stub()
    const status = sinon.stub()
    const emit = sinon.stub()
    const ack = sinon.stub()
    const node = { send, error, warn, status, emit, log: sinon.stub() }
    const channel = {
      consume: function (
        _queue: string,
        cb: (arg0: any) => void,
      ): void {
        cb(null)
      },
      ack,
    }
    amqp.channel = channel as any
    amqp.assertQueue = assertQueueStub
    amqp.bindQueue = bindQueueStub
    amqp.config.queue.name = 'queueName'
    amqp.config.queue.autoCreate = false
    amqp.config.exchange.autoCreate = false
    amqp.node = node as any

    await amqp.consume()

    expect(send.called).to.equal(false)
    expect(ack.called).to.equal(false)
    expect(error.called).to.equal(false)
    expect(warn.calledWithMatch('consumer was cancelled')).to.equal(true)
    expect(status.calledWith(NODE_STATUS.Disconnected)).to.equal(true)
    expect(emit.calledWith('amqp:consumer-cancelled')).to.equal(true)
  })

  it('assembleMessage retains reference and parses payload', () => {
    const amqpMessage: any = { content: Buffer.from('{"a":1}'), fields: { deliveryTag: 1 }, properties: {} }
    const result = (amqp as any).assembleMessage(amqpMessage)
    expect(result).to.equal(amqpMessage)
    expect(result).to.have.property('payload').that.deep.equals({ a: 1 })
  })

  it('assembleMessage logs error when payload is invalid JSON', () => {
    const amqpMessage: any = {
      content: Buffer.from('{invalid'),
      fields: { deliveryTag: 1 },
      properties: {},
    }
    const errorStub = sinon.stub()
    amqp.node = { ...nodeFixture, error: errorStub }
    const result = (amqp as any).assembleMessage(amqpMessage)
    expect(result).to.equal(amqpMessage)
    expect(result).to.have.property('payload', '{invalid')
    expect(errorStub.calledWithMatch('Invalid JSON payload')).to.be.true
  })

  it('ack() logs and delegates to channel', () => {
    const logStub = sinon.stub()
    const ackStub = sinon.stub()
    amqp.node = { ...nodeFixture, log: logStub, error: sinon.stub() }
    amqp.channel = { ack: ackStub } as any
    const msg: any = { fields: { deliveryTag: 5 } }
    amqp.ack(msg)
    expect(ackStub.calledOnceWith(msg, false)).to.be.true
    expect(logStub.calledWithMatch('Acking message')).to.be.true
  })

  describe('publish()', () => {
    it('publishes a message (topic)', async () => {
      const publishStub = sinon.stub()
      amqp.channel = {
        publish: publishStub,
      }
      await amqp.publish('a message')
      expect(publishStub.calledOnce).to.equal(true)
    })

    it('publishes a message (fanout)', async () => {
      // @ts-ignore
      amqp = new Amqp(RED, nodeFixture, {
        ...nodeConfigFixture,
        exchangeType: ExchangeType.Fanout,
      })
      const publishStub = sinon.stub()
      amqp.channel = {
        publish: publishStub,
      }
      await amqp.publish('a message')
      expect(publishStub.calledOnce).to.equal(true)
    })

    it('publishes a message (direct w/RPC)', async () => {
      // @ts-ignore
      amqp = new Amqp(RED, nodeFixture, {
        ...nodeConfigFixture,
        exchangeType: ExchangeType.Direct,
        outputs: 1,
      })
      const publishStub = sinon.stub()
      const assertQueueStub = sinon.stub()
      const consumeStub = sinon.stub()
      amqp.channel = {
        publish: publishStub,
        assertQueue: assertQueueStub,
        consume: consumeStub,
      }

      const routingKey = 'rpc-routingkey'
      amqp.config = {
        broker: '',
        exchange: { type: ExchangeType.Direct, routingKey },
        queue: {},
        amqpProperties: {},
        outputs: 1,
      }
      amqp.node = {
        error: sinon.stub(),
      }
      amqp.q = {}

      await amqp.publish('a message')

      // FIXME: we're losing `this` in here and can't assert on mocks.
      // So no assertions :(
      // expect(consumeStub.calledOnce).to.equal(true)
      // expect(publishStub.calledOnce).to.equal(true)
    })

    it('waits for confirms when enabled', async () => {
      const publishStub = sinon.stub()
      const waitForConfirmsStub = sinon.stub().resolves()
      amqp.channel = {
        publish: publishStub,
        waitForConfirms: waitForConfirmsStub,
      }
      amqp.config.waitForConfirms = true

      await amqp.publish('a message')

      expect(publishStub.calledOnce).to.equal(true)
      expect(waitForConfirmsStub.calledOnce).to.equal(true)
    })

    it('waits for channel drain when publish applies backpressure', async () => {
      const { EventEmitter } = require('events')
      const channel = new EventEmitter()
      channel.publish = sinon.stub().returns(false)
      amqp.channel = channel

      let completed = false
      const publishPromise = amqp.publish('a message').then(() => {
        completed = true
      })
      await new Promise(resolve => setImmediate(resolve))

      expect(completed).to.equal(false)

      channel.emit('drain')
      await publishPromise
      expect(completed).to.equal(true)
    })

    it('rejects a confirmed publish when the broker returns a mandatory message', async () => {
      const events: Record<string, (message?: unknown) => void> = {}
      let releaseConfirm: () => void = () => undefined
      const confirmPending = new Promise<void>(resolve => {
        releaseConfirm = resolve
      })
      const channel = {
        on: sinon.stub().callsFake(
          (event: string, callback: (message?: unknown) => void) => {
            events[event] = callback
          },
        ),
        prefetch: sinon.stub().resolves(),
        publish: sinon.stub().returns(true),
        waitForConfirms: sinon.stub().returns(confirmPending),
      }
      amqp.connection = {
        createConfirmChannel: sinon.stub().resolves(channel),
      }
      amqp.config.waitForConfirms = true
      await amqp.initialize()

      const publishResult = amqp
        .publish('a message', {
          mandatory: true,
          messageId: 'returned-message',
        })
        .then(
          () => undefined,
          (error: unknown) => error,
        )
      await Promise.resolve()

      const publishedOptions = channel.publish.firstCall.args[3]
      events.return({
        fields: {
          replyCode: 312,
          replyText: 'NO_ROUTE',
          exchange: 'events',
          routingKey: 'missing.route',
        },
        properties: publishedOptions,
        content: Buffer.from('a message'),
      })
      releaseConfirm()

      const publishError = await publishResult
      expect(publishError).to.be.instanceOf(Error)
    })

    it('reports successful and failed routing keys after partial mandatory delivery', async () => {
      const events: Record<string, (message?: unknown) => void> = {}
      const channel = {
        on: sinon.stub().callsFake(
          (event: string, callback: (message?: unknown) => void) => {
            events[event] = callback
          },
        ),
        prefetch: sinon.stub().resolves(),
        publish: sinon.stub().callsFake(
          (
            exchange: string,
            routingKey: string,
            content: Buffer,
            options: unknown,
          ) => {
            if (routingKey === 'route.missing') {
              events.return({
                fields: {
                  replyCode: 312,
                  replyText: 'NO_ROUTE',
                  exchange,
                  routingKey,
                },
                properties: options,
                content,
              })
            }
            return true
          },
        ),
        waitForConfirms: sinon.stub().resolves(),
      }
      amqp.connection = {
        createConfirmChannel: sinon.stub().resolves(channel),
      }
      amqp.config.exchange.routingKey = 'route.ok,route.missing'
      amqp.config.waitForConfirms = true
      await amqp.initialize()

      const publishError = await amqp
        .publish('a message', { mandatory: true })
        .then(
          () => undefined,
          (error: unknown) => error,
        )

      expect(publishError).to.be.instanceOf(Error)
      expect(publishError.successfulRoutingKeys).to.deep.equal(['route.ok'])
      expect(publishError.failedRoutingKeys).to.deep.equal(['route.missing'])
    })

    it('tries to publish an invalid message', async () => {
      const publishStub = sinon.stub().throws()
      const errorStub = sinon.stub()
      amqp.channel = {
        publish: publishStub,
      }
      amqp.node = {
        error: errorStub,
      }
      try {
        await amqp.publish('a message')
        expect.fail('publish should throw')
      } catch {
        // expected
      }
      expect(publishStub.calledOnce).to.equal(true)
      expect(errorStub.calledOnce).to.equal(true)
    })

    it('serializes object payloads before publishing', async () => {
      const publishStub = sinon.stub()
      amqp.channel = {
        publish: publishStub,
      }

      await amqp.publish({ a: 1 })

      expect(publishStub.calledOnce).to.equal(true)
      const publishedBuffer = publishStub.firstCall.args[2]
      expect(Buffer.isBuffer(publishedBuffer)).to.equal(true)
      expect(publishedBuffer.toString()).to.equal('{"a":1}')
    })

    it('serializes null payload to JSON null', async () => {
      const publishStub = sinon.stub()
      amqp.channel = {
        publish: publishStub,
      }

      await amqp.publish(null)

      expect(publishStub.calledOnce).to.equal(true)
      const publishedBuffer = publishStub.firstCall.args[2]
      expect(Buffer.isBuffer(publishedBuffer)).to.equal(true)
      expect(publishedBuffer.toString()).to.equal('null')
    })

    it('serializes undefined payload to an empty buffer', async () => {
      const publishStub = sinon.stub()
      amqp.channel = {
        publish: publishStub,
      }

      await amqp.publish(undefined)

      expect(publishStub.calledOnce).to.equal(true)
      const publishedBuffer = publishStub.firstCall.args[2]
      expect(Buffer.isBuffer(publishedBuffer)).to.equal(true)
      expect(publishedBuffer.length).to.equal(0)
    })

    it('publishes Buffer payloads without copying or serializing', async () => {
      const publishStub = sinon.stub()
      const payload = Buffer.from('raw-buffer')
      amqp.channel = {
        publish: publishStub,
      }

      await amqp.publish(payload)

      expect(publishStub.calledOnce).to.equal(true)
      expect(publishStub.firstCall.args[2]).to.equal(payload)
    })

    it('publishes Uint8Array payloads as buffers', async () => {
      const publishStub = sinon.stub()
      const payload = new Uint8Array([1, 2, 3])
      amqp.channel = {
        publish: publishStub,
      }

      await amqp.publish(payload)

      const publishedBuffer = publishStub.firstCall.args[2]
      expect(Buffer.isBuffer(publishedBuffer)).to.equal(true)
      expect([...publishedBuffer]).to.deep.equal([1, 2, 3])
    })

    it('serializes function payloads to an empty buffer', async () => {
      const publishStub = sinon.stub()
      amqp.channel = {
        publish: publishStub,
      }

      await amqp.publish(() => undefined)

      expect(publishStub.calledOnce).to.equal(true)
      expect(publishStub.firstCall.args[2].length).to.equal(0)
    })

    it('throws a clear error for non-serializable payloads', async () => {
      const publishStub = sinon.stub()
      const errorStub = sinon.stub()
      const circular: any = {}
      circular.self = circular
      amqp.channel = {
        publish: publishStub,
      }
      amqp.node = {
        ...nodeFixture,
        error: errorStub,
      }

      try {
        await amqp.publish(circular)
        expect.fail('publish should throw')
      } catch (err: any) {
        expect(String(err.message)).to.match(/Could not serialize payload/)
      }

      expect(publishStub.called).to.equal(false)
      expect(errorStub.calledOnce).to.equal(true)
    })
  })

  describe('close()', () => {
    it('orchestrates the closing of the connection', async () => {
      const unbindQueuesStub = sinon.stub(amqp, 'unbindQueues' as any).resolves()
      const closeChannelStub = sinon.stub(amqp, 'closeChannel' as any).resolves()
      const releaseConnectionStub = sinon.stub(amqp, 'releaseConnection' as any).resolves()

      await amqp.close()

      expect(unbindQueuesStub.calledOnce).to.be.true
      expect(closeChannelStub.calledOnce).to.be.true
      expect(releaseConnectionStub.calledOnce).to.be.true
    })

    it('does not unbind durable named queues when close() is called', async () => {
      const unbindQueueStub = sinon.stub()
      const closeChannelStub = sinon.stub(amqp, 'closeChannel' as any).resolves()
      const releaseConnectionStub = sinon.stub(amqp, 'releaseConnection' as any).resolves()

      amqp.channel = { unbindQueue: unbindQueueStub }
      amqp.q = { queue: 'durable-close-queue' }
      amqp.config.queue = {
        ...amqp.config.queue,
        name: 'durable-close-queue',
        durable: true,
        exclusive: false,
        autoDelete: false,
      }

      await amqp.close()

      expect(unbindQueueStub.called).to.be.false
      expect(closeChannelStub.calledOnce).to.be.true
      expect(releaseConnectionStub.calledOnce).to.be.true
    })
  })

  describe('unbindQueues()', () => {
    it('unbinds the queue from the exchange', async () => {
      const { exchangeName, exchangeRoutingKey } = nodeConfigFixture
      const queueName = 'queueName'
      const unbindQueueStub = sinon.stub()
      amqp.channel = { unbindQueue: unbindQueueStub }
      amqp.q = { queue: queueName }
      amqp.config.exchange.autoCreate = true

      await (amqp as any).unbindQueues()

      expect(unbindQueueStub.calledOnceWith(queueName, exchangeName, exchangeRoutingKey)).to.be.true
    })

    it('does not unbind long-lived named queues on close', async () => {
      const unbindQueueStub = sinon.stub()
      amqp.channel = { unbindQueue: unbindQueueStub }
      amqp.q = { queue: 'persistent-queue' }
      amqp.config.queue = {
        ...amqp.config.queue,
        name: 'persistent-queue',
        exclusive: false,
        autoDelete: false,
      }

      await (amqp as any).unbindQueues()

      expect(unbindQueueStub.called).to.be.false
    })

    it('does not unbind durable named queues on close', async () => {
      const unbindQueueStub = sinon.stub()
      amqp.channel = { unbindQueue: unbindQueueStub }
      amqp.q = { queue: 'durable-named-queue' }
      amqp.config.queue = {
        ...amqp.config.queue,
        name: 'durable-named-queue',
        durable: true,
        exclusive: false,
        autoDelete: false,
      }

      await (amqp as any).unbindQueues()

      expect(unbindQueueStub.called).to.be.false
    })

    it('does not unbind long-lived named queues even after routing key override', async () => {
      const unbindQueueStub = sinon.stub()
      amqp.channel = { unbindQueue: unbindQueueStub }
      amqp.q = { queue: 'persistent-queue' }
      amqp.config.queue = {
        ...amqp.config.queue,
        name: 'persistent-queue',
        exclusive: false,
        autoDelete: false,
      }
      amqp.setRoutingKey('runtime.override.key')

      await (amqp as any).unbindQueues()

      expect(unbindQueueStub.called).to.be.false
    })

    it('does not unbind durable named queues even with multiple routing keys', async () => {
      const unbindQueueStub = sinon.stub()
      amqp.channel = { unbindQueue: unbindQueueStub }
      amqp.q = { queue: 'durable-multi-routing-queue' }
      amqp.config.queue = {
        ...amqp.config.queue,
        name: 'durable-multi-routing-queue',
        durable: true,
        exclusive: false,
        autoDelete: false,
      }
      amqp.setRoutingKey('orders.created,orders.updated,orders.deleted')

      await (amqp as any).unbindQueues()

      expect(unbindQueueStub.called).to.be.false
    })

    it('does unbind for exclusive queues', async () => {
      const { exchangeName, exchangeRoutingKey } = nodeConfigFixture
      const unbindQueueStub = sinon.stub()
      amqp.channel = { unbindQueue: unbindQueueStub }
      amqp.q = { queue: 'exclusive-queue' }
      amqp.config.exchange.autoCreate = true
      amqp.config.queue = {
        ...amqp.config.queue,
        name: 'exclusive-queue',
        exclusive: true,
        autoDelete: false,
      }

      await (amqp as any).unbindQueues()

      expect(
        unbindQueueStub.calledOnceWith('exclusive-queue', exchangeName, exchangeRoutingKey),
      ).to.be.true
    })

    it('does unbind for auto-delete queues', async () => {
      const { exchangeName, exchangeRoutingKey } = nodeConfigFixture
      const unbindQueueStub = sinon.stub()
      amqp.channel = { unbindQueue: unbindQueueStub }
      amqp.q = { queue: 'autodelete-queue' }
      amqp.config.exchange.autoCreate = true
      amqp.config.queue = {
        ...amqp.config.queue,
        name: 'autodelete-queue',
        exclusive: false,
        autoDelete: true,
      }

      await (amqp as any).unbindQueues()

      expect(
        unbindQueueStub.calledOnceWith('autodelete-queue', exchangeName, exchangeRoutingKey),
      ).to.be.true
    })

    it('does unbind for server-named queues', async () => {
      const { exchangeName, exchangeRoutingKey } = nodeConfigFixture
      const unbindQueueStub = sinon.stub()
      amqp.channel = { unbindQueue: unbindQueueStub }
      amqp.q = { queue: 'amq.gen-random' }
      amqp.config.exchange.autoCreate = true
      amqp.config.queue = {
        ...amqp.config.queue,
        name: '',
        exclusive: false,
        autoDelete: false,
      }

      await (amqp as any).unbindQueues()

      expect(
        unbindQueueStub.calledOnceWith('amq.gen-random', exchangeName, exchangeRoutingKey),
      ).to.be.true
    })

    it('handles errors when unbinding', async () => {
      const unbindQueueStub = sinon.stub().rejects(new Error('unbind failed'))
      const errorStub = sinon.stub()
      amqp.channel = { unbindQueue: unbindQueueStub }
      amqp.q = { queue: 'queueName' }
      amqp.node = { error: errorStub }
      amqp.config.exchange.autoCreate = true

      await (amqp as any).unbindQueues()

      expect(errorStub.calledOnceWithMatch('Error unbinding queue for routing key')).to.be.true
    })
  })

  describe('closeChannel()', () => {
    it('closes the channel', async () => {
      const closeStub = sinon.stub().resolves()
      amqp.channel = { close: closeStub, off: sinon.stub() }

      await (amqp as any).closeChannel()

      expect(closeStub.calledOnce).to.be.true
    })

    it('handles errors when closing', async () => {
      const closeStub = sinon.stub().rejects(new Error('close failed'))
      const errorStub = sinon.stub()
      amqp.channel = { close: closeStub, off: sinon.stub() }
      amqp.node = { error: errorStub }

      await (amqp as any).closeChannel()

      expect(errorStub.calledOnceWithMatch('Error closing AMQP channel')).to.be.true
    })
  })

  it('createChannel()', async () => {
    const error = 'error!'
    const result = {
      on: sinon.stub(),
      prefetch: (): null => null,
    }
    const createChannelStub = sinon.stub().returns(result)
    amqp.connection = { createChannel: createChannelStub }

    await amqp.createChannel()
    expect(createChannelStub.calledOnce).to.equal(true)
    expect(amqp.channel).to.eq(result)
  })

  it('createChannel() uses confirm channel when configured', async () => {
    const result = {
      on: sinon.stub(),
      prefetch: sinon.stub(),
    }
    const createConfirmChannelStub = sinon.stub().resolves(result)
    const createChannelStub = sinon.stub()
    amqp.connection = {
      createConfirmChannel: createConfirmChannelStub,
      createChannel: createChannelStub,
    }
    amqp.config.waitForConfirms = true

    await amqp.createChannel()

    expect(createConfirmChannelStub.calledOnce).to.equal(true)
    expect(createChannelStub.called).to.equal(false)
    expect(amqp.channel).to.eq(result)
    expect(result.prefetch.calledOnce).to.equal(true)
  })

  it('createChannel() logs events', async () => {
    const events: { [key: string]: Function } = {}
    const result = {
      on: (ev: string, cb: Function): void => {
        events[ev] = cb
      },
      prefetch: sinon.stub(),
    }
    const createChannelStub = sinon.stub().resolves(result)
    amqp.connection = { createChannel: createChannelStub }

    const logStub = sinon.stub()
    const warnStub = sinon.stub()
    const errorStub = sinon.stub()
    const statusStub = sinon.stub()
    amqp.node = { ...nodeFixture, log: logStub, warn: warnStub, error: errorStub, status: statusStub }
    amqp.broker = { ...brokerConfigFixture, nodeStates: {}, lastError: {} }

    await amqp.createChannel()

    events['close']()
    events['return']({
      fields: {
        replyCode: 312,
        replyText: 'NO_ROUTE',
        exchange: 'events',
        routingKey: 'missing.route',
      },
      properties: { headers: {} },
    })
    events['error']('oops')

    expect(logStub.calledWithMatch('AMQP Channel closed')).to.be.true
    expect(warnStub.calledWithMatch('AMQP Message returned')).to.be.true
    expect(errorStub.calledWithMatch('AMQP Connection Error')).to.be.true
    expect(statusStub.calledWith(NODE_STATUS.Disconnected)).to.be.true
    expect(amqp.broker.nodeStates[nodeFixture.id]).to.equal('errored')
  })

  it('assertExchange()', async () => {
    const assertExchangeStub = sinon.stub()
    amqp.channel = { assertExchange: assertExchangeStub }
    const { exchangeName, exchangeType, exchangeDurable } = nodeConfigFixture

    await amqp.assertExchange()
    expect(assertExchangeStub.calledOnce).to.equal(true)
    expect(
      assertExchangeStub.calledWith(exchangeName, exchangeType, {
        durable: exchangeDurable,
      }),
    ).to.equal(true)
  })

  it('assertQueue()', async () => {
    const queue = 'queueName'
    const { queueName, queueExclusive, queueDurable, queueAutoDelete, queueType, queueArguments } =
      nodeConfigFixture
    const assertQueueStub = sinon.stub().resolves({ queue })
    amqp.channel = { assertQueue: assertQueueStub }

    await amqp.assertQueue()
    expect(assertQueueStub.calledOnce).to.equal(true)
    expect(
      assertQueueStub.calledWith(queueName, {
        exclusive: queueExclusive,
        durable: queueDurable,
        autoDelete: queueAutoDelete,
        arguments: { "x-queue-type": queueType, ...queueArguments },
      }),
    ).to.equal(true)
  })

  it('assertQueue() without queueArguments', async () => {
    const queue = 'queueName'
    const { queueName, queueExclusive, queueDurable, queueAutoDelete, queueType } =
      nodeConfigFixture
    const assertQueueStub = sinon.stub().resolves({ queue })
    amqp.channel = { assertQueue: assertQueueStub }

    await amqp.assertQueue({
      ...amqp.config,
      queue: { ...amqp.config.queue, queueArguments: undefined },
    } as any)
    expect(assertQueueStub.calledOnce).to.equal(true)
    expect(
      assertQueueStub.calledWith(queueName, {
        exclusive: queueExclusive,
        durable: queueDurable,
        autoDelete: queueAutoDelete,
        arguments: { "x-queue-type": queueType },
      }),
    ).to.equal(true)
  })

  it('useExistingQueue() selects an existing named queue without declaring it', () => {
    const queueName = amqp.useExistingQueue({
      ...amqp.config,
      queue: { ...amqp.config.queue, name: 'existing.queue' },
    } as any)

    expect(queueName).to.equal('existing.queue')
    expect(amqp.q.queue).to.equal('existing.queue')
  })

  it('bindQueue() topic exchange', () => {
    const queue = 'queueName'
    const bindQueueStub = sinon.stub()
    amqp.channel = { bindQueue: bindQueueStub }
    amqp.q = { queue }
    const { exchangeName, exchangeRoutingKey } = nodeConfigFixture

    amqp.bindQueue()
    expect(bindQueueStub.calledOnce).to.equal(true)
    expect(
      bindQueueStub.calledWith(queue, exchangeName, exchangeRoutingKey),
    ).to.equal(true)
  })

  it('bindQueue() direct exchange', () => {
    const config = {
      ...nodeConfigFixture,
      exchangeType: ExchangeType.Direct,
      exchangeRoutingKey: 'routing-key',
    }
    // @ts-ignore
    amqp = new Amqp(RED, nodeFixture, config)

    const queue = 'queueName'
    const bindQueueStub = sinon.stub()
    amqp.channel = { bindQueue: bindQueueStub }
    amqp.q = { queue }
    const { exchangeName, exchangeRoutingKey } = config

    amqp.bindQueue()
    // expect(bindQueueStub.calledOnce).to.equal(true)
    expect(
      bindQueueStub.calledWith(queue, exchangeName, exchangeRoutingKey),
    ).to.equal(true)
  })

  it('bindQueue() fanout exchange', () => {
    const config = {
      ...nodeConfigFixture,
      exchangeType: ExchangeType.Fanout,
      exchangeRoutingKey: '',
    }
    // @ts-ignore
    amqp = new Amqp(RED, nodeFixture, config)

    const queue = 'queueName'
    const bindQueueStub = sinon.stub()
    amqp.channel = { bindQueue: bindQueueStub }
    amqp.q = { queue }
    const { exchangeName } = config

    amqp.bindQueue()
    expect(bindQueueStub.calledOnce).to.equal(true)
    expect(bindQueueStub.calledWith(queue, exchangeName, '')).to.equal(true)
  })

  it('bindQueue() headers exchange', () => {
    const config = {
      ...nodeConfigFixture,
      exchangeType: ExchangeType.Headers,
      exchangeRoutingKey: '',
      headers: { some: 'headers' },
    }
    // @ts-ignore
    amqp = new Amqp(RED, nodeFixture, config)

    const queue = 'queueName'
    const bindQueueStub = sinon.stub()
    amqp.channel = { bindQueue: bindQueueStub }
    amqp.q = { queue }
    const { exchangeName, headers } = config

    amqp.bindQueue()
    expect(bindQueueStub.calledOnce).to.equal(true)
    expect(bindQueueStub.calledWith(queue, exchangeName, '', headers)).to.equal(
      true,
    )
  })

  it('bindQueue() rethrows errors', async () => {
    const queue = 'queueName'
    const error = new Error('bind fail')
    const bindQueueStub = sinon.stub().rejects(error)
    amqp.channel = { bindQueue: bindQueueStub }
    amqp.q = { queue }

    try {
      await amqp.bindQueue()
      expect.fail('bindQueue should throw')
    } catch (e) {
      expect(e).to.equal(error)
    }
  })

  it('bindQueue() rethrows binding errors so consume setup can retry', async () => {
    const queue = 'queueName'
    const error = new Error('bind fail')
    const bindQueueStub = sinon.stub().rejects(error)
    amqp.channel = { bindQueue: bindQueueStub }
    amqp.q = { queue }

    try {
      await amqp.bindQueue()
      expect.fail('bindQueue should throw')
    } catch (e) {
      expect(e).to.equal(error)
    }
  })

  it('consume() does not register a consumer when real queue binding fails', async () => {
    const assertQueueStub = sinon.stub().resolves({ queue: 'queueName' })
    const bindQueueStub = sinon.stub().rejects(new Error('bind fail'))
    const consumeStub = sinon.stub()
    const errorStub = sinon.stub()

    amqp.channel = {
      assertQueue: assertQueueStub,
      bindQueue: bindQueueStub,
      consume: consumeStub,
    }
    amqp.config.queue.name = 'queueName'
    amqp.config.queue.autoCreate = true
    amqp.config.exchange.autoCreate = true
    amqp.node = { send: sinon.stub(), error: errorStub }

    try {
      await amqp.consume()
      expect.fail('consume should throw')
    } catch (e: any) {
      expect(String(e.message)).to.match(/bind fail/)
    }

    expect(consumeStub.called).to.equal(false)
    expect(errorStub.calledWithMatch('Could not consume message')).to.equal(true)
  })

  it('consume() logs error when bindQueue fails', async () => {
    const assertQueueStub = sinon.stub().resolves()
    const bindQueueStub = sinon.stub().rejects(new Error('bind fail'))
    const consumeStub = sinon.stub()
    const errorStub = sinon.stub()

    amqp.assertQueue = assertQueueStub
    amqp.bindQueue = bindQueueStub
    amqp.channel = { consume: consumeStub }
    amqp.config.queue.name = 'queueName'
    amqp.config.queue.autoCreate = true
    amqp.config.exchange.autoCreate = true
    amqp.node = { send: sinon.stub(), error: errorStub }
    amqp.q = { queue: 'queueName' }

    try {
      await amqp.consume()
      expect.fail('consume should throw')
    } catch (e: any) {
      expect(String(e.message)).to.match(/bind fail/)
    }
    expect(consumeStub.called).to.equal(false)
    expect(errorStub.calledOnce).to.equal(true)
  })

  describe('setVhost()', () => {
    it('reconnects when vhost changes', async () => {
      amqp.broker = { ...brokerConfigFixture, vhost: 'vh1' }
      const closeStub = sinon.stub(amqp, 'close').resolves()
      const connectStub = sinon.stub(amqp, 'connect').resolves()
      const initStub = sinon.stub(amqp, 'initialize').resolves()

      await amqp.setVhost('vh2')

      expect(closeStub.calledOnce).to.equal(true)
      expect(connectStub.calledOnce).to.equal(true)
      expect(initStub.calledOnce).to.equal(true)
      expect((amqp.broker as BrokerConfig).vhost).to.equal('vh1')
      expect((amqp as any).vhostOverride).to.equal('vh2')
    })

    it('does nothing when vhost is unchanged', async () => {
      amqp.broker = { ...brokerConfigFixture, vhost: 'vh1' }
      const closeStub = sinon.stub(amqp, 'close').resolves()
      const connectStub = sinon.stub(amqp, 'connect').resolves()
      const initStub = sinon.stub(amqp, 'initialize').resolves()

      await amqp.setVhost('vh1')

      expect(closeStub.called).to.equal(false)
      expect(connectStub.called).to.equal(false)
      expect(initStub.called).to.equal(false)
      expect((amqp as any).vhostOverride).to.be.undefined
    })

    it('does not mutate shared broker config', async () => {
      const sharedBroker = { ...brokerConfigFixture, vhost: 'vh1' }
      amqp.broker = sharedBroker
      const closeStub = sinon.stub(amqp, 'close').resolves()
      const connectStub = sinon.stub(amqp, 'connect').resolves()
      const initStub = sinon.stub(amqp, 'initialize').resolves()

      await amqp.setVhost('vh2')

      expect(sharedBroker.vhost).to.equal('vh1')
      expect((amqp as any).vhostOverride).to.equal('vh2')
      expect(closeStub.calledOnce).to.equal(true)
      expect(connectStub.calledOnce).to.equal(true)
      expect(initStub.calledOnce).to.equal(true)
    })

    it('allows separate instances to target different vhosts', async () => {
      const sharedBroker = { ...brokerConfigFixture, vhost: 'vh1' }
      const amqp1: any = new Amqp(RED, nodeFixture, nodeConfigFixture)
      const amqp2: any = new Amqp(RED, nodeFixture, nodeConfigFixture)
      amqp1.broker = sharedBroker
      amqp2.broker = sharedBroker
      sinon.stub(amqp1, 'close').resolves()
      sinon.stub(amqp1, 'connect').resolves()
      sinon.stub(amqp1, 'initialize').resolves()
      sinon.stub(amqp2, 'close').resolves()
      sinon.stub(amqp2, 'connect').resolves()
      sinon.stub(amqp2, 'initialize').resolves()

      await amqp1.setVhost('vh2')
      await amqp2.setVhost('vh3')

      expect(amqp1.vhostOverride).to.equal('vh2')
      expect(amqp2.vhostOverride).to.equal('vh3')
      expect(sharedBroker.vhost).to.equal('vh1')
    })
  })

  describe('handleRemoteProcedureCall()', () => {
    let clock

    beforeEach(() => {
        clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
    });

    afterEach(() => {
        clock.restore();
    });

    it('handles RPC timeout', async () => {
        const sendStub = sinon.stub();
        const deleteQueueStub = sinon.stub().resolves();
        const consumeStub = sinon.stub();
        const assertQueueStub = sinon.stub().resolves('rpc-queue');

        amqp.node = { ...nodeFixture, send: sendStub, error: sinon.stub() };
        amqp.channel = {
            assertQueue: assertQueueStub,
            consume: consumeStub,
            deleteQueue: deleteQueueStub,
            publish: sinon.stub(),
        };

        amqp.config.outputs = 1; // Enable RPC
        amqp.config.rpcTimeout = 1000;

        await amqp.publish('a message');

        // Move time forward to trigger the timeout
        await clock.tickAsync(1001);

        expect(sendStub.calledOnce).to.be.true;
        expect(sendStub.firstCall.args[0].payload.message).to.match(/Timeout while waiting for RPC response/);
        expect(deleteQueueStub.calledOnce).to.be.true;
    });

    it('handles RPC timeout with mismatched correlation ID message', async () => {
        const sendStub = sinon.stub();
        const deleteQueueStub = sinon.stub().resolves();
        const assertQueueStub = sinon.stub().resolves('rpc-queue');
        let consumeCallback;
        const consumeStub = sinon.stub().callsFake((queue, cb) => {
            consumeCallback = cb;
            return Promise.resolve();
        });

        amqp.node = { ...nodeFixture, send: sendStub, error: sinon.stub() };
        amqp.channel = {
            assertQueue: assertQueueStub,
            consume: consumeStub,
            deleteQueue: deleteQueueStub,
            publish: sinon.stub(),
        };

        amqp.config.outputs = 1; // Enable RPC
        amqp.config.rpcTimeout = 1000;

        await amqp.publish('a message', { correlationId: 'test-correlation-id' });

        // Simulate receiving a message with the wrong correlation ID
        consumeCallback({
            properties: { correlationId: 'wrong-id' },
            content: Buffer.from('{"response": true}')
        });

        // Move time forward to trigger the timeout
        await clock.tickAsync(1001);

        expect(sendStub.calledOnce).to.be.true;
        expect(sendStub.firstCall.args[0].payload.message).to.match(/Correlation ids do not match/);
        expect(deleteQueueStub.calledOnce).to.be.true;
    });

    it('does not lose concurrent RPC responses that share a reply queue', async () => {
        const sendStub = sinon.stub();
        const deleteQueueStub = sinon.stub().resolves();
        const assertQueueStub = sinon.stub().resolves('shared-reply-queue');
        const consumeCallbacks: Array<(message: unknown) => void> = [];
        const consumeStub = sinon.stub().callsFake((queue, callback) => {
            consumeCallbacks.push(callback);
            return Promise.resolve({ consumerTag: `consumer-${consumeCallbacks.length}` });
        });

        amqp.node = { ...nodeFixture, send: sendStub, error: sinon.stub() };
        amqp.channel = {
            assertQueue: assertQueueStub,
            consume: consumeStub,
            deleteQueue: deleteQueueStub,
            publish: sinon.stub(),
        };
        amqp.config.outputs = 1;
        amqp.config.rpcTimeout = 1000;

        await Promise.all([
            amqp.publish('request-one', {
                correlationId: 'correlation-one',
                replyTo: 'shared-reply-queue',
            }),
            amqp.publish('request-two', {
                correlationId: 'correlation-two',
                replyTo: 'shared-reply-queue',
            }),
        ]);

        expect(consumeStub.calledOnce).to.equal(true);

        const sharedConsumer = consumeCallbacks[0];
        sharedConsumer({
            fields: { deliveryTag: 1 },
            properties: { correlationId: 'correlation-two' },
            content: Buffer.from('{"response":2}'),
        });
        sharedConsumer({
            fields: { deliveryTag: 2 },
            properties: { correlationId: 'correlation-one' },
            content: Buffer.from('{"response":1}'),
        });
        await Promise.resolve();

        expect(sendStub.callCount).to.equal(2);
        expect(sendStub.getCalls().map(call => call.args[0].payload)).to.deep.equal([
            { response: 2 },
            { response: 1 },
        ]);
    });

    it('does not lose RPC responses when separate instances share a reply queue', async () => {
        const firstSendStub = sinon.stub();
        const secondSendStub = sinon.stub();
        const firstCallbacks: Array<(message: unknown) => void> = [];
        const secondCallbacks: Array<(message: unknown) => void> = [];
        const firstDeleteQueueStub = sinon.stub().resolves();
        const secondDeleteQueueStub = sinon.stub().resolves();
        const sharedConnection = {};

        const secondAmqp: any = new Amqp(
            RED,
            { ...nodeFixture, id: 'rpc-node-two', send: secondSendStub, error: sinon.stub() },
            nodeConfigFixture,
        );
        amqp.node = {
            ...nodeFixture,
            id: 'rpc-node-one',
            send: firstSendStub,
            error: sinon.stub(),
        };
        amqp.connection = sharedConnection;
        secondAmqp.connection = sharedConnection;
        amqp.channel = {
            assertQueue: sinon.stub().resolves('shared-reply-queue'),
            consume: sinon.stub().callsFake((queue, callback) => {
                firstCallbacks.push(callback);
                return Promise.resolve({ consumerTag: 'consumer-one' });
            }),
            deleteQueue: firstDeleteQueueStub,
            publish: sinon.stub(),
        };
        secondAmqp.channel = {
            assertQueue: sinon.stub().resolves('shared-reply-queue'),
            consume: sinon.stub().callsFake((queue, callback) => {
                secondCallbacks.push(callback);
                return Promise.resolve({ consumerTag: 'consumer-two' });
            }),
            deleteQueue: secondDeleteQueueStub,
            publish: sinon.stub(),
        };
        amqp.config.outputs = 1;
        secondAmqp.config.outputs = 1;
        amqp.config.rpcTimeout = 1000;
        secondAmqp.config.rpcTimeout = 1000;

        await Promise.all([
            amqp.publish('request-one', {
                correlationId: 'correlation-one',
                replyTo: 'shared-reply-queue',
            }),
            secondAmqp.publish('request-two', {
                correlationId: 'correlation-two',
                replyTo: 'shared-reply-queue',
            }),
        ]);

        const brokerSelectedConsumer = firstCallbacks[0];
        brokerSelectedConsumer({
            fields: { deliveryTag: 1 },
            properties: { correlationId: 'correlation-one' },
            content: Buffer.from('{"response":1}'),
        });
        await Promise.resolve();

        expect(firstDeleteQueueStub.called).to.equal(false);
        expect(secondDeleteQueueStub.called).to.equal(false);

        brokerSelectedConsumer({
            fields: { deliveryTag: 2 },
            properties: { correlationId: 'correlation-two' },
            content: Buffer.from('{"response":2}'),
        });
        await Promise.resolve();

        expect(firstSendStub.calledOnce).to.equal(true);
        expect(secondSendStub.calledOnce).to.equal(true);
        expect(secondSendStub.firstCall.args[0].payload).to.deep.equal({
            response: 2,
        });
        expect(secondCallbacks).to.have.length(0);
    });

    it('waits for shared RPC consumer cleanup before recreating its reply queue', async () => {
        let finishDeleteQueue: () => void = () => undefined;
        const deleteQueuePending = new Promise<void>(resolve => {
            finishDeleteQueue = resolve;
        });
        const firstCallbacks: Array<(message: unknown) => void> = [];
        const sharedConnection = {};
        const secondAmqp: any = new Amqp(
            RED,
            { ...nodeFixture, id: 'rpc-node-two', send: sinon.stub(), error: sinon.stub() },
            nodeConfigFixture,
        );

        amqp.node = {
            ...nodeFixture,
            id: 'rpc-node-one',
            send: sinon.stub(),
            error: sinon.stub(),
        };
        amqp.connection = sharedConnection;
        secondAmqp.connection = sharedConnection;
        amqp.channel = {
            assertQueue: sinon.stub().resolves('shared-reply-queue'),
            consume: sinon.stub().callsFake((queue, callback) => {
                firstCallbacks.push(callback);
                return Promise.resolve({ consumerTag: 'consumer-one' });
            }),
            deleteQueue: sinon.stub().returns(deleteQueuePending),
            publish: sinon.stub(),
        };
        const secondConsumeStub = sinon.stub().resolves({
            consumerTag: 'consumer-two',
        });
        amqp.config.outputs = 1;
        secondAmqp.config.outputs = 1;
        amqp.config.rpcTimeout = 1000;
        secondAmqp.config.rpcTimeout = 1000;
        secondAmqp.channel = {
            assertQueue: sinon.stub().resolves('shared-reply-queue'),
            consume: secondConsumeStub,
            deleteQueue: sinon.stub().resolves(),
            publish: sinon.stub(),
        };

        await amqp.publish('request-one', {
            correlationId: 'correlation-one',
            replyTo: 'shared-reply-queue',
        });
        firstCallbacks[0]({
            fields: { deliveryTag: 1 },
            properties: { correlationId: 'correlation-one' },
            content: Buffer.from('{"response":1}'),
        });
        await Promise.resolve();

        const secondPublish = secondAmqp.publish('request-two', {
            correlationId: 'correlation-two',
            replyTo: 'shared-reply-queue',
        });
        await Promise.resolve();
        await Promise.resolve();

        expect(secondConsumeStub.called).to.equal(false);

        finishDeleteQueue();
        await secondPublish;

        expect(secondConsumeStub.calledOnce).to.equal(true);
    });

    it('handles error when deleting queue on timeout', async () => {
        const sendStub = sinon.stub();
        const deleteQueueStub = sinon.stub().rejects(new Error('delete failed'));
        const consumeStub = sinon.stub();
        const assertQueueStub = sinon.stub().resolves('rpc-queue');
        const errorStub = sinon.stub();

        amqp.node = { ...nodeFixture, send: sendStub, error: errorStub };
        amqp.channel = {
            assertQueue: assertQueueStub,
            consume: consumeStub,
            deleteQueue: deleteQueueStub,
            publish: sinon.stub(),
        };

        amqp.config.outputs = 1; // Enable RPC
        amqp.config.rpcTimeout = 1000;

        await amqp.publish('a message');

        // Move time forward to trigger the timeout
        await clock.tickAsync(1001);

        expect(sendStub.calledOnce).to.be.true;
        expect(deleteQueueStub.calledOnce).to.be.true;
        expect(errorStub.calledWithMatch('Error trying to cancel RPC consumer')).to.be.true;
    });

    it('handles error when deleting queue after receiving RPC reply', async () => {
        const sendStub = sinon.stub();
        const deleteQueueStub = sinon.stub().rejects(new Error('delete failed'));
        const cancelStub = sinon.stub().resolves();
        const assertQueueStub = sinon.stub().resolves('rpc-queue');
        const errorStub = sinon.stub();
        let consumeCallback;
        const consumeStub = sinon.stub().callsFake((queue, cb) => {
            consumeCallback = cb;
            return Promise.resolve({ consumerTag: 'rpc-consumer-tag' });
        });

        amqp.node = { ...nodeFixture, send: sendStub, error: errorStub };
        amqp.channel = {
            assertQueue: assertQueueStub,
            consume: consumeStub,
            deleteQueue: deleteQueueStub,
            cancel: cancelStub,
            publish: sinon.stub(),
        };

        amqp.config.outputs = 1;

        await amqp.publish('a message', {
            correlationId: 'test-correlation-id',
            replyTo: 'rpc-queue',
        });

        await consumeCallback({
            properties: { correlationId: 'test-correlation-id' },
            content: Buffer.from('{"response": true}'),
        });
        await Promise.resolve();

        expect(sendStub.calledOnce).to.be.true;
        expect(deleteQueueStub.calledOnce).to.be.true;
        expect(cancelStub.calledOnceWith('rpc-consumer-tag')).to.be.true;
        expect(errorStub.calledWithMatch('Error trying to cancel RPC consumer')).to.be.true;
    });

    it('emits only one output when RPC times out and a late reply arrives during cleanup', async () => {
        const sendStub = sinon.stub();
        const assertQueueStub = sinon.stub().resolves('rpc-queue');
        let consumeCallback;
        let resolveDelete;
        const deleteQueueStub = sinon.stub().callsFake(() =>
            new Promise(resolve => {
                resolveDelete = resolve;
            })
        );
        const consumeStub = sinon.stub().callsFake((queue, cb) => {
            consumeCallback = cb;
            return Promise.resolve();
        });

        amqp.node = { ...nodeFixture, send: sendStub, error: sinon.stub() };
        amqp.channel = {
            assertQueue: assertQueueStub,
            consume: consumeStub,
            deleteQueue: deleteQueueStub,
            publish: sinon.stub(),
        };

        amqp.config.outputs = 1;
        amqp.config.rpcTimeout = 1000;

        await amqp.publish('a message', { correlationId: 'test-correlation-id' });
        await clock.tickAsync(1001);

        consumeCallback({
            properties: { correlationId: 'test-correlation-id' },
            content: Buffer.from('{"response": true}'),
        });
        resolveDelete();
        await Promise.resolve();

        expect(sendStub.calledOnce).to.be.true;
        expect(sendStub.firstCall.args[0].payload.message).to.match(/Timeout while waiting for RPC response/);
    });

    it('clears RPC timeout when closed before timeout elapses', async () => {
        const sendStub = sinon.stub();
        const deleteQueueStub = sinon.stub().resolves();
        const consumeStub = sinon.stub().resolves();
        const closeChannelStub = sinon.stub().resolves();
        const poolConnection = {
            close: sinon.stub().resolves(),
            off: sinon.stub(),
        };

        sinon.stub(amqp, 'assertQueue').resolves('rpc-queue');

        amqp.node = { ...nodeFixture, send: sendStub, error: sinon.stub(), status: sinon.stub() };
        amqp.channel = {
            consume: consumeStub,
            deleteQueue: deleteQueueStub,
            publish: sinon.stub(),
            off: sinon.stub(),
            close: closeChannelStub,
        };
        amqp.connection = poolConnection;
        amqp.broker = { ...brokerConfigFixture, vhost: 'vh1', id: 'b1' };
        (Amqp as any).connectionPool.set('b1:vh1', { connection: poolConnection, count: 1 });

        amqp.config.outputs = 1;
        amqp.config.rpcTimeout = 1000;

        await amqp.publish('a message');
        await amqp.close();
        await clock.tickAsync(1001);

        expect(sendStub.called).to.be.false;
        expect(deleteQueueStub.called).to.be.false;
    });

    it('reports an in-flight RPC as failed when reconnect closes its channel', async () => {
        const sendStub = sinon.stub();
        const poolConnection = {
            close: sinon.stub().resolves(),
            off: sinon.stub(),
        };

        sinon.stub(amqp, 'assertQueue').resolves('rpc-queue');

        amqp.node = { ...nodeFixture, send: sendStub, error: sinon.stub(), status: sinon.stub() };
        amqp.channel = {
            consume: sinon.stub().resolves({ consumerTag: 'rpc-consumer' }),
            deleteQueue: sinon.stub().resolves(),
            publish: sinon.stub(),
            off: sinon.stub(),
            close: sinon.stub().resolves(),
        };
        amqp.connection = poolConnection;
        amqp.broker = { ...brokerConfigFixture, vhost: 'vh1', id: 'b1' };
        (Amqp as any).connectionPool.set('b1:vh1', { connection: poolConnection, count: 1 });

        amqp.config.outputs = 1;
        amqp.config.rpcTimeout = 1000;

        await amqp.publish('a message');
        await amqp.close(new Error('AMQP connection lost during RPC'));

        expect(sendStub.calledOnce).to.be.true;
        expect(sendStub.firstCall.args[0].payload.message).to.match(
            /connection lost|interrupted/i,
        );
    });

    it('handles error during RPC setup', async () => {
        const errorStub = sinon.stub();
        const assertQueueStub = sinon.stub().rejects(new Error('assert failed'));
        const publishStub = sinon.stub();

        amqp.node = { ...nodeFixture, error: errorStub };
        amqp.channel = {
            assertQueue: assertQueueStub,
            publish: publishStub,
        };

        amqp.config.outputs = 1; // Enable RPC

        try {
            await amqp.publish('a message');
            expect.fail('publish should throw');
        } catch (e) {
            expect(e.message).to.equal('assert failed');
        }

        expect(assertQueueStub.calledOnce).to.be.true;
        expect(publishStub.called).to.be.false;
        expect(errorStub.calledWithMatch('Could not consume RPC message')).to.be.true;
        expect(errorStub.calledWithMatch('Could not publish message')).to.be.true;
    });

    it('does not emit an RPC timeout when publish fails after RPC setup', async () => {
        const sendStub = sinon.stub();
        const errorStub = sinon.stub();
        const assertQueueStub = sinon.stub().resolves('rpc-queue');
        const consumeStub = sinon.stub().resolves({ consumerTag: 'rpc-consumer-tag' });
        const deleteQueueStub = sinon.stub().resolves();
        const publishStub = sinon.stub().throws(new Error('publish failed'));

        amqp.node = { ...nodeFixture, send: sendStub, error: errorStub };
        amqp.channel = {
            assertQueue: assertQueueStub,
            consume: consumeStub,
            deleteQueue: deleteQueueStub,
            publish: publishStub,
        };

        amqp.config.outputs = 1;
        amqp.config.rpcTimeout = 1000;

        try {
            await amqp.publish('a message', {
                correlationId: 'test-correlation-id',
                replyTo: 'rpc-queue',
            });
            expect.fail('publish should throw');
        } catch (e) {
            expect(e.message).to.equal('publish failed');
        }

        await clock.tickAsync(1001);

        expect(sendStub.called).to.be.false;
        expect(deleteQueueStub.calledOnceWith('rpc-queue')).to.be.true;
        expect(errorStub.calledWithMatch('Could not publish message')).to.be.true;
    });

    it('cleans up asserted RPC queue when consume setup fails', async () => {
        const errorStub = sinon.stub();
        const assertQueueStub = sinon.stub().resolves('rpc-queue');
        const consumeStub = sinon.stub().rejects(new Error('consume setup failed'));
        const deleteQueueStub = sinon.stub().resolves();
        const publishStub = sinon.stub();

        amqp.node = { ...nodeFixture, error: errorStub };
        amqp.channel = {
            assertQueue: assertQueueStub,
            consume: consumeStub,
            deleteQueue: deleteQueueStub,
            publish: publishStub,
        };

        amqp.config.outputs = 1;

        try {
            await amqp.publish('a message', {
                correlationId: 'test-correlation-id',
                replyTo: 'rpc-queue',
            });
            expect.fail('publish should throw');
        } catch (e) {
            expect(e.message).to.equal('consume setup failed');
        }

        expect(assertQueueStub.calledOnce).to.be.true;
        expect(consumeStub.calledOnce).to.be.true;
        expect(deleteQueueStub.calledOnceWith('rpc-queue')).to.be.true;
        expect(publishStub.called).to.be.false;
        expect(errorStub.calledWithMatch('Could not consume RPC message')).to.be.true;
    });

  });

  it('connect() handles connection failure', async () => {
    const connectStub = sinon.stub(amqplib, 'connect').rejects(new Error('connection failed'));
    const warnStub = sinon.stub();
    const broker = {
      ...brokerConfigFixture,
      vhost: 'vh1',
      nodeStates: {},
      lastError: {},
    }
    RED.nodes.getNode.returns(broker)
    amqp.node = { ...nodeFixture, id: 'n1', warn: warnStub, log: sinon.stub(), status: sinon.stub() };

    try {
        await amqp.connect();
        expect.fail('connect should have thrown');
    } catch (e) {
        expect(connectStub.calledOnce).to.be.true;
        expect(warnStub.calledWithMatch('Failed to connect to AMQP broker')).to.be.true;
        expect(e.message).to.equal('connection failed');
        expect(broker.nodeStates.n1).to.equal('errored');
        expect(broker.lastError.n1?.message).to.equal('connection failed');
    }
  });

  describe('releaseConnection()', () => {
    beforeEach(() => {
      amqp.node = { ...nodeFixture, status: sinon.stub(), error: sinon.stub() }
    })

    it('does not throw when broker node is unavailable during shutdown', async () => {
      const connectionStub = {
        on: sinon.stub(),
        off: sinon.stub(),
        close: sinon.stub().resolves(),
      }

      amqp.connection = connectionStub as any
      amqp.broker = undefined as any

      await (amqp as any).releaseConnection()

      expect(connectionStub.off.called).to.be.true
      expect(amqp.node.status.calledWith(NODE_STATUS.Disconnected)).to.be.true
    })

    it('decrements the connection count but does not close the connection', async () => {
      const connectionStub = {
        on: sinon.stub(),
        off: sinon.stub(),
        close: sinon.stub().resolves(),
      }

      ;(Amqp as any).connectionPool.set('b1:vh1', {
        connection: connectionStub,
        count: 2,
      })

      amqp.connection = connectionStub as any
      amqp.broker = {
        ...brokerConfigFixture,
        vhost: 'vh1',
        id: 'b1',
        nodeStates: { [nodeFixture.id]: 'errored' },
        lastError: { [nodeFixture.id]: { message: 'previous error', at: new Date().toISOString() } },
      }

      await (amqp as any).releaseConnection()

      const poolEntry = (Amqp as any).connectionPool.get('b1:vh1')
      expect(poolEntry.count).to.equal(1)
      expect(connectionStub.close.called).to.be.false
      expect(amqp.node.status.calledWith(NODE_STATUS.Disconnected)).to.be.true
      expect(amqp.broker.nodeStates[nodeFixture.id]).to.equal('disconnected')
      expect(amqp.broker.lastError[nodeFixture.id]?.message).to.equal('previous error')
    })

    it('removes the connection from the pool when count reaches zero', async () => {
      let closed = false
      const connectionStub = {
        on: sinon.stub(),
        off: sinon.stub(),
        close: sinon.stub().callsFake(
          () =>
            new Promise<void>(resolve =>
              setTimeout(() => {
                closed = true
                resolve()
              }, 5),
            ),
        ),
      }

      ;(Amqp as any).connectionPool.set('b1:vh1', {
        connection: connectionStub,
        count: 1,
      })

      amqp.connection = connectionStub as any
      amqp.broker = { ...brokerConfigFixture, vhost: 'vh1', id: 'b1' }

      await (amqp as any).releaseConnection()

      expect(connectionStub.close.calledOnce).to.be.true
      expect(closed).to.be.true
      expect((Amqp as any).connectionPool.size).to.equal(0)
      expect(amqp.node.status.calledWith(NODE_STATUS.Disconnected)).to.be.true
    })

    it('handles connection.close error', async () => {
      const connectionCloseStub = sinon.stub().rejects(new Error('close failed'));

      const connectionStub = {
          on: sinon.stub(),
          off: sinon.stub(),
          close: connectionCloseStub,
      };

      (Amqp as any).connectionPool.set('b1:vh1', {
          connection: connectionStub,
          count: 1,
      });

      amqp.connection = connectionStub as any;
      amqp.broker = { ...brokerConfigFixture, vhost: 'vh1', id: 'b1' };
      amqp.config.broker = 'b1';

      await (amqp as any).releaseConnection();

      expect(connectionCloseStub.calledOnce).to.be.true;
      expect(amqp.node.error.calledWithMatch('Error closing AMQP connection')).to.be.true;
      expect(amqp.node.status.calledWith(NODE_STATUS.Disconnected)).to.be.true
    });
  })
})
