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
    assertExchange: sinon.stub(),
    assertQueue: sinon.stub(),
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
})
