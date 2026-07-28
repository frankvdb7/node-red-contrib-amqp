/* eslint-disable @typescript-eslint/no-explicit-any */
export {}

const { expect } = require('chai')
const sinon = require('sinon')
const amqplib = require('amqplib')
const Amqp = require('../src/Amqp').default
const { nodeConfigFixture, nodeFixture, brokerConfigFixture } = require('./doubles')

describe('amqplib native recovery', () => {
  let RED: any
  let amqp: any

  beforeEach(() => {
    RED = {
      nodes: { getNode: sinon.stub().returns(brokerConfigFixture) },
    }
    amqp = new Amqp(RED, nodeFixture, {
      ...nodeConfigFixture,
      queueName: 'existing-queue',
    })
  })

  afterEach(() => sinon.restore())

  const channel = () => ({
    prefetch: sinon.stub().resolves(),
    on: sinon.stub(),
    off: sinon.stub(),
    assertExchange: sinon.stub().resolves(),
    assertQueue: sinon.stub().resolves({ queue: 'declared-queue' }),
    bindQueue: sinon.stub(),
    consume: sinon.stub().resolves(),
    close: sinon.stub().resolves(),
  })

  const model = (createdChannel: any) => ({
    createChannel: sinon.stub().resolves(createdChannel),
    createConfirmChannel: sinon.stub().resolves(createdChannel),
  })

  it('uses amqplib recovery and rebuilds the consumer on every connection', async () => {
    const initialChannel = channel()
    const recoveredChannel = channel()
    const initialModel = model(initialChannel)
    const recoveredModel = model(recoveredChannel)
    const manager = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub().resolves(),
    }
    let recovery: any

    sinon.stub(amqplib, 'connect').callsFake(async (_url: string, options: any) => {
      recovery = options.recovery
      await recovery.setup(initialModel)
      return manager
    })

    amqp.onRecovery(async () => {
      await amqp.initialize()
      await amqp.consume()
    })

    await amqp.connect()
    await amqp.initialize()
    await amqp.consume()
    await recovery.setup(recoveredModel)

    expect(recovery.setup).to.be.a('function')
    expect(initialModel.createChannel.calledOnce).to.equal(true)
    expect(recoveredModel.createChannel.calledOnce).to.equal(true)
    expect(initialChannel.consume.calledOnce).to.equal(true)
    expect(recoveredChannel.consume.calledOnce).to.equal(true)
    expect(initialChannel.assertExchange.called).to.equal(false)
    expect(initialChannel.assertQueue.called).to.equal(false)
    expect(initialChannel.bindQueue.called).to.equal(false)
    expect(recoveredChannel.assertExchange.called).to.equal(false)
    expect(recoveredChannel.assertQueue.called).to.equal(false)
    expect(recoveredChannel.bindQueue.called).to.equal(false)
  })

  it('recreates opted-in topology after recovery', async () => {
    const initialChannel = channel()
    const recoveredChannel = channel()
    const initialModel = model(initialChannel)
    const recoveredModel = model(recoveredChannel)
    const manager = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub().resolves(),
    }
    let recovery: any
    amqp = new Amqp(RED, nodeFixture, {
      ...nodeConfigFixture,
      queueName: 'recovery-queue',
      autoCreateQueue: true,
      autoCreateExchangeBindings: true,
    })
    sinon.stub(amqplib, 'connect').callsFake(async (_url: string, options: any) => {
      recovery = options.recovery
      await recovery.setup(initialModel)
      return manager
    })

    amqp.onRecovery(async () => {
      await amqp.initialize()
      await amqp.consume()
    })

    await amqp.connect()
    await amqp.initialize()
    await amqp.consume()
    await recovery.setup(recoveredModel)

    expect(recoveredChannel.assertExchange.calledOnce).to.equal(true)
    expect(recoveredChannel.assertQueue.calledOnce).to.equal(true)
    expect(recoveredChannel.bindQueue.calledOnce).to.equal(true)
    expect(recoveredChannel.consume.calledOnce).to.equal(true)
  })

  it('propagates recovery setup failures to amqplib', async () => {
    const initialModel = model(channel())
    const recoveredModel = model(channel())
    const manager = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub().resolves(),
    }
    let recovery: any
    sinon.stub(amqplib, 'connect').callsFake(async (_url: string, options: any) => {
      recovery = options.recovery
      await recovery.setup(initialModel)
      return manager
    })
    const setupFailure = new Error('queue recovery failed')
    amqp.onRecovery(async () => {
      throw setupFailure
    })

    await amqp.connect()

    try {
      await recovery.setup(recoveredModel)
      expect.fail('recovery setup should reject')
    } catch (error) {
      expect(error).to.equal(setupFailure)
    }
  })

  it('closes the recovery manager on node shutdown', async () => {
    const manager = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub().resolves(),
    }
    sinon.stub(amqplib, 'connect').resolves(manager)

    await amqp.connect()
    await amqp.close()

    expect(manager.close.calledOnce).to.equal(true)
  })

  it('closes a recovery manager that resolves after node shutdown', async () => {
    const manager = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub().resolves(),
    }
    let resolveConnection: (value: typeof manager) => void
    sinon.stub(amqplib, 'connect').returns(
      new Promise(resolve => {
        resolveConnection = resolve
      }),
    )

    const connecting = amqp.connect().catch(error => error)
    await amqp.close()
    resolveConnection!(manager)

    const error = await connecting
    expect(String(error)).to.match(/closed before it completed/)
    expect(manager.close.calledOnce).to.equal(true)
  })

  it('propagates failed initial recovery after the configured retry limit', async () => {
    let recovery: any
    sinon.stub(amqplib, 'connect').callsFake(async (_url: string, options: any) => {
      recovery = options.recovery
      throw new Error('getaddrinfo ENOTFOUND broker')
    })

    try {
      await amqp.connect()
      expect.fail('connect should reject after the configured recovery attempts')
    } catch (error) {
      expect(String(error)).to.match(/ENOTFOUND/)
    }

    expect(recovery.maxRetries).to.equal(5)
  })

  it('reports an exhausted reconnection attempt as an error', async () => {
    const status = sinon.stub()
    const error = sinon.stub()
    amqp.node = { ...nodeFixture, status, error }
    const listeners: Record<string, (error: Error) => void> = {}
    const manager = {
      on: sinon.stub().callsFake((event: string, listener: (error: Error) => void) => {
        listeners[event] = listener
      }),
      off: sinon.stub(),
      close: sinon.stub().resolves(),
    }
    sinon.stub(amqplib, 'connect').resolves(manager)

    await amqp.connect()
    listeners['reconnect-failed'](new Error('broker unavailable'))

    expect(status.calledWithMatch({ text: 'Error' })).to.equal(true)
    expect(error.calledWithMatch(/recovery failed/)).to.equal(true)
  })
})
