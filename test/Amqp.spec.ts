/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/ban-ts-comment */
export {}
const { expect } = require('chai')
const sinon = require('sinon')
const amqplib = require('amqplib')
const Amqp = require('../src/Amqp').default
const {
  nodeConfigFixture,
  nodeFixture,
  brokerConfigFixture,
} = require('./doubles')
const { ExchangeType, DefaultExchangeName } = require('../src/types')
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
    expect(assertExchangeStub.calledOnce).to.equal(true)
  })

  it('consume()', async () => {
    const assertQueueStub = sinon.stub()
    const bindQueueStub = sinon.stub()
    const messageContent = 'messageContent'
    const send = sinon.stub()
    const error = sinon.stub()
    const node = { send, error }
    const channel = {
      consume: function (
        queue: string,
        cb: (arg0: any) => void,
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        config: GenericJsonObject,
      ): void {
        const amqpMessage = { content: messageContent }
        cb(amqpMessage)
      },
    }
    amqp.channel = channel
    amqp.assertQueue = assertQueueStub
    amqp.bindQueue = bindQueueStub
    amqp.q = { queue: 'queueName' }
    amqp.node = node

    await amqp.consume()
    expect(assertQueueStub.calledOnce).to.equal(true)
    expect(bindQueueStub.calledOnce).to.equal(true)
    expect(send.calledOnce).to.equal(true)
    expect(
      send.calledWith({
        content: messageContent,
        payload: messageContent,
      }),
    ).to.equal(true)
  })

  describe('publish()', () => {
    it('publishes a message (topic)', () => {
      const publishStub = sinon.stub()
      amqp.channel = {
        publish: publishStub,
      }
      amqp.publish('a message')
      expect(publishStub.calledOnce).to.equal(true)
    })

    it('publishes a message (fanout)', () => {
      // @ts-ignore
      amqp = new Amqp(RED, nodeFixture, {
        ...nodeConfigFixture,
        exchangeType: ExchangeType.Fanout,
      })
      const publishStub = sinon.stub()
      amqp.channel = {
        publish: publishStub,
      }
      amqp.publish('a message')
      expect(publishStub.calledOnce).to.equal(true)
    })

    it('publishes a message (direct w/RPC)', () => {
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

      amqp.publish('a message')

      // FIXME: we're losing `this` in here and can't assert on mocks.
      // So no assertions :(
      // expect(consumeStub.calledOnce).to.equal(true)
      // expect(publishStub.calledOnce).to.equal(true)
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
      await amqp.publish('a message')
      expect(publishStub.calledOnce).to.equal(true)
      expect(errorStub.calledOnce).to.equal(true)
    })
  })

  it('close()', async () => {
    const { exchangeName, exchangeRoutingKey } = nodeConfigFixture
    const queueName = 'queueName'

    const unbindQueueStub = sinon.stub()
    const channelCloseStub = sinon.stub()
    const connectionCloseStub = sinon.stub().resolves()
    const assertQueueStub = sinon.stub().resolves({ queue: queueName })

    amqp.channel = {
      unbindQueue: unbindQueueStub,
      close: channelCloseStub,
      assertQueue: assertQueueStub,
      off: sinon.stub(),
    }
    amqp.connection = { close: connectionCloseStub, off: sinon.stub() }
    ;(Amqp as any).connectionPool.set('b1:undefined', {
      connection: amqp.connection,
      count: 1,
    })
    await amqp.assertQueue()

    await amqp.close()

    expect(unbindQueueStub.calledOnce).to.equal(true)
    expect(
      unbindQueueStub.calledWith(queueName, exchangeName, exchangeRoutingKey),
    ).to.equal(true)
    expect(channelCloseStub.calledOnce).to.equal(true)
    expect(connectionCloseStub.calledOnce).to.equal(true)
  })

  it('close() logs error if channel.close fails but still closes connection', async () => {
    const queueName = 'queueName'
    const unbindQueueStub = sinon.stub()
    const channelCloseStub = sinon.stub().rejects(new Error('channel fail'))
    const connectionCloseStub = sinon.stub().resolves()
    const errorStub = sinon.stub()
    const assertQueueStub = sinon.stub().resolves({ queue: queueName })

    amqp.channel = {
      unbindQueue: unbindQueueStub,
      close: channelCloseStub,
      assertQueue: assertQueueStub,
      off: sinon.stub(),
    }
    amqp.connection = { close: connectionCloseStub, off: sinon.stub() }
    ;(Amqp as any).connectionPool.set('b1:undefined', {
      connection: amqp.connection,
      count: 1,
    })
    amqp.node = { error: errorStub }
    await amqp.assertQueue()

    await amqp.close()

    expect(channelCloseStub.calledOnce).to.equal(true)
    expect(connectionCloseStub.calledOnce).to.equal(true)
    expect(errorStub.calledWithMatch('Error closing AMQP channel')).to.equal(true)
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
    amqp.node = { ...nodeFixture, log: logStub, warn: warnStub, error: errorStub }

    await amqp.createChannel()

    events['close']()
    events['return']()
    events['error']('oops')

    expect(logStub.calledWithMatch('AMQP Channel closed')).to.be.true
    expect(warnStub.calledWithMatch('AMQP Message returned')).to.be.true
    expect(errorStub.calledWithMatch('AMQP Connection Error')).to.be.true
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

  it('bindQueue() handles errors', async () => {
    const queue = 'queueName'
    const error = new Error('bind fail')
    const bindQueueStub = sinon.stub().rejects(error)
    const errorStub = sinon.stub()
    amqp.channel = { bindQueue: bindQueueStub }
    amqp.q = { queue }
    amqp.node = { error: errorStub }

    await amqp.bindQueue()
    expect(errorStub.calledOnce).to.equal(true)
  })

  it('consume() logs error when bindQueue fails', async () => {
    const assertQueueStub = sinon.stub().resolves()
    const bindQueueStub = sinon.stub().rejects(new Error('bind fail'))
    const consumeStub = sinon.stub()
    const errorStub = sinon.stub()

    amqp.assertQueue = assertQueueStub
    amqp.bindQueue = bindQueueStub
    amqp.channel = { consume: consumeStub }
    amqp.node = { send: sinon.stub(), error: errorStub }
    amqp.q = { queue: 'queueName' }

    await amqp.consume()
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
})
