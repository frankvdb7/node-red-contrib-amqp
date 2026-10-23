import { expect } from 'chai'
import sinon from 'sinon'
import { EventEmitter, once } from 'node:events'
import amqplib from 'amqplib'
import helper from 'node-red-node-test-helper'
import Amqp from '../../src/Amqp'
import amqpBroker from '../../src/nodes/amqp-broker'
import amqpIn from '../../src/nodes/amqp-in'
import amqpInManualAck from '../../src/nodes/amqp-in-manual-ack'
import amqpOut from '../../src/nodes/amqp-out'
const modules = [amqpBroker, amqpIn, amqpInManualAck, amqpOut]
helper.init(require.resolve('node-red'))

describe('Node-RED native recovery lifecycle', () => {
  let wrapper, options
  beforeEach(async () => {
    wrapper = Object.assign(new EventEmitter(), { close: sinon.stub().resolves() })
    sinon.stub(amqplib, 'connect').callsFake(async (_url, opts) => {
      options = opts.recovery
      return wrapper
    })
    await new Promise(resolve => helper.startServer(resolve))
  })
  afterEach(async () => {
    await helper.unload()
    await new Promise(resolve => helper.stopServer(resolve))
    sinon.restore()
  })
  async function load(type, legacy) {
    await helper.load(modules, [
      { id: 'broker', type: 'amqp-broker', host: 'localhost', port: 5672, vhost: '/' },
      { id: 'node', type, broker: 'broker', queueName: 'test', reconnectOnError: legacy },
      { id: 'completion', type: 'complete', scope: ['node'], wires: [['completed']] },
      { id: 'completed', type: 'helper' },
    ], { broker: { username: 'user', password: 'pass' } })
  }
  function rawModel() {
    const channel = Object.assign(new EventEmitter(), {
      prefetch: sinon.stub().resolves(), consume: sinon.stub().resolves(), close: sinon.stub().resolves(),
    })
    return Object.assign(new EventEmitter(), {
      createChannel: sinon.stub().resolves(channel), close: sinon.stub().resolves(),
    })
  }
  for (const type of ['amqp-out', 'amqp-in', 'amqp-in-manual-ack']) {
    for (const legacy of [false, true, undefined]) {
      it(`${type} recovers regardless of legacy option ${legacy}`, async () => {
        await load(type, legacy)
        expect((await helper.request().get('/amqp-broker/health')).status).to.equal(503)
        wrapper.emit('connect-failed', new Error('ECONNREFUSED'))
        expect((await helper.request().get('/amqp-broker/health')).status).to.equal(503)
        const raw = rawModel()
        await options.setup(raw)
        wrapper.emit('connect', raw)
        expect((await helper.request().get('/amqp-broker/health')).status).to.equal(200)
        wrapper.emit('disconnect', new Error('connection lost'))
        expect((await helper.request().get('/amqp-broker/health')).status).to.equal(503)
        await helper.unload()
        expect(wrapper.close.callCount).to.equal(1)
      })
    }
  }
  it('isolates publisher blocking from consumer connections', async () => {
    const publisherWrapper = Object.assign(new EventEmitter(), { close: sinon.stub().resolves() })
    const consumerWrapper = Object.assign(new EventEmitter(), { close: sinon.stub().resolves() })
    const recoveries = []
    amqplib.connect.callsFake(async (_url, opts) => {
      options = opts.recovery
      recoveries.push(opts.recovery)
      return recoveries.length === 1 ? publisherWrapper : consumerWrapper
    })
    await helper.load(modules, [
      { id: 'broker', type: 'amqp-broker', name: 'RabbitMQ', host: 'localhost', port: 5672, vhost: '/' },
      { id: 'publisher1', type: 'amqp-out', broker: 'broker' },
      { id: 'publisher2', type: 'amqp-out', broker: 'broker' },
      { id: 'consumer', type: 'amqp-in', broker: 'broker', queueName: 'test' },
    ], { broker: { username: 'user', password: 'pass' } })
    const publishers = ['publisher1', 'publisher2'].map(id => helper.getNode(id))
    const publisherStatuses = publishers.map(() => [])
    const publisherWarnings = publishers.map(() => [])
    publishers.forEach((node, index) => {
      node.on('call:status', call => publisherStatuses[index].push(call.args[0]))
      node.on('call:warn', call => publisherWarnings[index].push(call.args[0]))
    })
    const consumerStatuses = []
    helper.getNode('consumer').on('call:status', call => consumerStatuses.push(call.args[0]))

    expect(recoveries).to.have.length(2)
    const publisherRaw = rawModel(), consumerRaw = rawModel()
    await recoveries[0].setup(publisherRaw)
    publisherWrapper.emit('connect', publisherRaw)
    await recoveries[1].setup(consumerRaw)
    consumerWrapper.emit('connect', consumerRaw)
    await new Promise(resolve => setImmediate(resolve))
    expect((await helper.request().get('/amqp-broker/health')).status).to.equal(200)
    expect(publisherWrapper.listenerCount('blocked')).to.equal(1)
    expect(publisherWrapper.listenerCount('unblocked')).to.equal(1)

    publisherWrapper.emit('blocked', 'low on memory')
    publisherWrapper.emit('blocked', 'low on memory')
    const blocked = await helper.request().get('/amqp-broker/health')
    expect(blocked.status).to.equal(503)
    expect(blocked.body).to.deep.equal({
      overallStatus: 'degraded',
      brokers: [{
        id: 'broker',
        name: 'RabbitMQ',
        status: 'connected',
        publishBlocked: { publisher1: 'low on memory', publisher2: 'low on memory' },
      }],
    })
    expect(publisherStatuses.map(statuses => statuses.at(-1).text)).to.deep.equal([
      'Publishing blocked', 'Publishing blocked',
    ])
    expect(publisherWarnings.map(warnings => warnings)).to.deep.equal([
      ['AMQP publishing blocked: low on memory'],
      ['AMQP publishing blocked: low on memory'],
    ])
    expect(consumerStatuses.some(status => status.text === 'Connected')).to.equal(true)
    expect(consumerStatuses.some(status => status.text === 'Publishing blocked')).to.equal(false)

    publisherWrapper.emit('unblocked')
    expect((await helper.request().get('/amqp-broker/health')).status).to.equal(200)
    expect(publisherStatuses.map(statuses => statuses.at(-1).text)).to.deep.equal([
      'Connected', 'Connected',
    ])

    await helper.unload()
    expect(publisherWrapper.listenerCount('blocked')).to.equal(0)
    expect(publisherWrapper.listenerCount('unblocked')).to.equal(0)
    expect(publisherWrapper.close.calledOnce).to.equal(true)
    expect(consumerWrapper.close.calledOnce).to.equal(true)
  })
  it('clears blocked publisher state on disconnect and does not restore health before recovery', async () => {
    await load('amqp-out', false)
    const publisher = helper.getNode('node'), statuses = []
    publisher.on('call:status', call => statuses.push(call.args[0]))
    const first = rawModel()
    await options.setup(first)
    wrapper.emit('connect', first)
    await new Promise(resolve => setImmediate(resolve))

    wrapper.emit('blocked', 'disk alarm')
    expect((await helper.request().get('/amqp-broker/health')).body.overallStatus).to.equal('degraded')
    wrapper.emit('disconnect', new Error('connection lost'))
    const disconnected = await helper.request().get('/amqp-broker/health')
    expect(disconnected.status).to.equal(503)
    expect(disconnected.body.overallStatus).to.equal('unhealthy')
    expect(disconnected.body.brokers[0].status).to.equal('disconnected')
    expect(disconnected.body.brokers[0]).not.to.have.property('publishBlocked')
    expect(disconnected.body.brokers[0].lastError.node.message).to.equal('connection lost')
    expect(statuses.at(-1).text).to.equal('Disconnected')
    wrapper.emit('unblocked')
    expect((await helper.request().get('/amqp-broker/health')).status).to.equal(503)

    const recovered = rawModel()
    await options.setup(recovered)
    wrapper.emit('connect', recovered)
    await new Promise(resolve => setImmediate(resolve))
    const recoveredHealth = await helper.request().get('/amqp-broker/health')
    expect(recoveredHealth.status).to.equal(200)
    expect(recoveredHealth.body.overallStatus).to.equal('healthy')
    expect(recoveredHealth.body.brokers[0].status).to.equal('connected')
    expect(recoveredHealth.body.brokers[0]).not.to.have.property('publishBlocked')
    expect(statuses.at(-1).text).to.equal('Connected')
  })
  for (const type of ['amqp-in', 'amqp-in-manual-ack']) {
    it(`${type} completes a successful reconnect control message`, async () => {
      await load(type, false)
      const raw = rawModel()
      await options.setup(raw)
      wrapper.emit('connect', raw)
      await new Promise(resolve => setImmediate(resolve))
      const completed = once(helper.getNode('completed'), 'input', { signal: AbortSignal.timeout(1000) })
      helper.getNode('node').receive({ payload: { reconnectCall: true } })
      expect((await completed)[0].payload.reconnectCall).to.equal(true)
      expect(raw.close.calledOnce).to.equal(true)
    })
    it(`${type} treats reconnectCall as control and reports its failure through done`, async () => {
      await load(type, false)
      const raw = rawModel()
      await options.setup(raw)
      wrapper.emit('connect', raw)
      await new Promise(resolve => setImmediate(resolve))
      raw.close.rejects(new Error('manual close failed'))
      const node = helper.getNode('node'), errors = []
      node.on('call:error', call => errors.push(call.args[0]))
      node.receive({ payload: { reconnectCall: true }, manualAck: { ackMode: 'ack' } })
      await new Promise(resolve => setTimeout(resolve, 20))
      expect(errors.some(error => /manual close failed/.test(String(error)))).to.equal(true)
    })
  }
  it('completes ordinary amqp-in inputs without restarting the connection', async () => {
    await load('amqp-in', false)
    const raw = rawModel()
    await options.setup(raw)
    wrapper.emit('connect', raw)
    for (const message of [{}, { payload: { reconnectCall: false } }]) {
      const completed = once(helper.getNode('completed'), 'input', { signal: AbortSignal.timeout(1000) })
      helper.getNode('node').receive(message)
      await completed
    }
    expect(raw.close.called).to.equal(false)
  })
  for (const closing of [false, true]) {
    it(`${closing ? 'ignores' : 'reports'} startup failure ${closing ? 'after node close' : 'while active'}`, async () => {
      let failStartup
      sinon.stub(Amqp.prototype, 'connect').returns(new Promise((_, reject) => { failStartup = reject }))
      await load('amqp-out', false)
      const node = helper.getNode('node'), errors = [], statuses = []
      node.on('call:error', call => errors.push(call.args[0]))
      node.on('call:status', call => statuses.push(call.args[0]))
      if (closing) await node.close()
      failStartup(new Error('startup failed'))
      await new Promise(resolve => setImmediate(resolve))
      expect(errors.some(error => /Could not initialize AMQP:.*startup failed/.test(String(error)))).to.equal(!closing)
      expect(statuses.some(status => status.text === 'Error')).to.equal(!closing)
    })
  }
  it('recovers a consumer channel without interrupting publisher confirmations', async () => {
    const consumerWrapper = Object.assign(new EventEmitter(), { close: sinon.stub().resolves() })
    const publisherWrapper = Object.assign(new EventEmitter(), { close: sinon.stub().resolves() })
    const recoveries = []
    amqplib.connect.callsFake(async (_url, opts) => {
      options = opts.recovery
      recoveries.push(opts.recovery)
      return recoveries.length === 1 ? consumerWrapper : publisherWrapper
    })
    await helper.load(modules, [
      { id: 'broker', type: 'amqp-broker', host: 'localhost', port: 5672, vhost: '/' },
      { id: 'consumer', type: 'amqp-in', broker: 'broker', queueName: 'test' },
      { id: 'publisher', type: 'amqp-out', broker: 'broker', waitForConfirms: true },
    ], { broker: { username: 'user', password: 'pass' } })
    const consumerRaw = rawModel()
    const consumer = await consumerRaw.createChannel()
    const publisherRaw = rawModel()
    let resolveConfirmation
    const confirm = Object.assign(new EventEmitter(), {
      prefetch: sinon.stub().resolves(), close: sinon.stub().resolves(), publish: sinon.stub(),
      waitForConfirms: sinon.stub().callsFake(() => new Promise(resolve => { resolveConfirmation = resolve })),
    })
    publisherRaw.createConfirmChannel = sinon.stub().resolves(confirm)
    consumerRaw.close.callsFake(async () => {
      consumerWrapper.emit('disconnect', new Error('consumer channel restarted'))
    })
    expect(recoveries).to.have.length(2)
    await recoveries[0].setup(consumerRaw)
    consumerWrapper.emit('connect', consumerRaw)
    await recoveries[1].setup(publisherRaw)
    publisherWrapper.emit('connect', publisherRaw)
    await new Promise(resolve => setImmediate(resolve))
    const node = helper.getNode('publisher'), errors = []
    node.on('call:error', call => errors.push(call.args[0]))
    node.receive({ payload: 'uncertain' })
    await new Promise(resolve => setImmediate(resolve))
    expect(confirm.waitForConfirms.calledOnce).to.equal(true)
    expect(errors).to.have.length(0)
    consumer.emit('close')
    await new Promise(resolve => setImmediate(resolve))
    expect(consumerRaw.close.calledOnce).to.equal(true)
    expect(publisherRaw.close.called).to.equal(false)
    expect(helper.getNode('broker').nodeStates).to.include({ consumer: 'disconnected', publisher: 'connected' })
    expect(errors).to.have.length(0)
    const replacementConsumer = rawModel()
    await recoveries[0].setup(replacementConsumer)
    consumerWrapper.emit('connect', replacementConsumer)
    await new Promise(resolve => setImmediate(resolve))
    expect(helper.getNode('broker').nodeStates).to.include({ consumer: 'connected', publisher: 'connected' })
    resolveConfirmation()
    await new Promise(resolve => setImmediate(resolve))
    expect(errors).to.have.length(0)
    expect(confirm.publish.calledOnce).to.equal(true)
    expect(amqplib.connect.calledTwice).to.equal(true)
  })
  it('completes amqp-out input with an error when the destination vhost stays unavailable', async () => {
    await load('amqp-out', false)
    const raw = rawModel()
    await options.setup(raw)
    wrapper.emit('connect', raw)
    await new Promise(resolve => setImmediate(resolve))
    const publish = sinon.stub(Amqp.prototype, 'publish').resolves()
    const node = helper.getNode('node'), errors = []
    node.on('call:error', call => errors.push(call.args[0]))
    const clock = sinon.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
    try {
      node.receive({ payload: 'not sent', vhost: 'unavailable' })
      await new Promise(resolve => setImmediate(resolve))
      await clock.tickAsync(10000)
      const doneErrors = errors.filter(error => error instanceof Error)
      expect(doneErrors).to.have.length(1)
      expect(doneErrors[0].message).to.include('Timed out')
      expect(publish.called).to.equal(false)
      expect(clock.countTimers()).to.equal(0)
    } finally {
      clock.restore()
    }
  })
})
