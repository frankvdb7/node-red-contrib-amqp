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
})
