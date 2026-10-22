export {}
const { expect } = require('chai')
const sinon = require('sinon')
const { EventEmitter } = require('node:events')
const amqplib = require('amqplib')
const helper = require('node-red-node-test-helper')
const modules = ['amqp-broker', 'amqp-in', 'amqp-in-manual-ack', 'amqp-out']
  .map(name => require(`../../src/nodes/${name}`))
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
  for (const type of ['amqp-in', 'amqp-in-manual-ack']) {
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
  it('reports an interrupted peer publisher confirmation when a channel failure restarts the shared transport', async () => {
    await helper.load(modules, [
      { id: 'broker', type: 'amqp-broker', host: 'localhost', port: 5672, vhost: '/' },
      { id: 'consumer', type: 'amqp-in', broker: 'broker', queueName: 'test' },
      { id: 'publisher', type: 'amqp-out', broker: 'broker', waitForConfirms: true },
    ], { broker: { username: 'user', password: 'pass' } })
    const raw = rawModel()
    const consumer = await raw.createChannel()
    let rejectConfirmation
    const confirm = Object.assign(new EventEmitter(), {
      prefetch: sinon.stub().resolves(), close: sinon.stub().resolves(), publish: sinon.stub(),
      waitForConfirms: sinon.stub().callsFake(() => new Promise((_, reject) => { rejectConfirmation = reject })),
    })
    raw.createConfirmChannel = sinon.stub().resolves(confirm)
    raw.close.callsFake(async () => {
      rejectConfirmation(new Error('channel closed before confirmation'))
      confirm.emit('close')
      wrapper.emit('disconnect', new Error('shared transport restarted'))
    })
    await options.setup(raw)
    wrapper.emit('connect', raw)
    await new Promise(resolve => setImmediate(resolve))
    const node = helper.getNode('publisher'), errors = []
    node.on('call:error', call => errors.push(call.args[0]))
    node.receive({ payload: 'uncertain' })
    await new Promise(resolve => setImmediate(resolve))
    expect(confirm.waitForConfirms.calledOnce).to.equal(true)
    expect(errors).to.have.length(0)
    consumer.emit('close')
    await new Promise(resolve => setImmediate(resolve))
    expect(raw.close.calledOnce).to.equal(true)
    const doneErrors = errors.filter(error => error instanceof Error)
    expect(doneErrors).to.have.length(1)
    expect(doneErrors[0].message).to.include('before confirmation')
    expect(helper.getNode('broker').nodeStates).to.include({ consumer: 'disconnected', publisher: 'disconnected' })
    const replacement = rawModel()
    const restoredConfirm = Object.assign(new EventEmitter(), {
      prefetch: sinon.stub().resolves(), close: sinon.stub().resolves(), publish: sinon.stub(),
      waitForConfirms: sinon.stub().resolves(),
    })
    replacement.createConfirmChannel = sinon.stub().resolves(restoredConfirm)
    await options.setup(replacement)
    wrapper.emit('connect', replacement)
    await new Promise(resolve => setImmediate(resolve))
    expect(helper.getNode('broker').nodeStates).to.include({ consumer: 'connected', publisher: 'connected' })
    expect(confirm.publish.calledOnce).to.equal(true)
    expect(restoredConfirm.publish.called).to.equal(false)
    expect(amqplib.connect.calledOnce).to.equal(true)
  })
  it('completes amqp-out input with an error when the destination vhost stays unavailable', async () => {
    await load('amqp-out', false)
    const raw = rawModel()
    await options.setup(raw)
    wrapper.emit('connect', raw)
    await new Promise(resolve => setImmediate(resolve))
    const publish = sinon.stub(require('../../src/Amqp').default.prototype, 'publish').resolves()
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
