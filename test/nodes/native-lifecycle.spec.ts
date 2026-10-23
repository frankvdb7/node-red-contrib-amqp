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
