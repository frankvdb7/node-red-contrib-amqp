export {}
const { expect } = require('chai')
const sinon = require('sinon')
const { EventEmitter } = require('node:events')
const amqplib = require('amqplib')
const Amqp = require('../src/Amqp').default
const { nodeConfigFixture, brokerConfigFixture } = require('./doubles')

describe('native recovery ownership', () => {
  let wrapper, options, connect, broker, clients
  const channel = () => Object.assign(new EventEmitter(), {
    prefetch: sinon.stub().resolves(), close: sinon.stub().resolves(),
    publish: sinon.stub(), waitForConfirms: sinon.stub().resolves(),
    assertExchange: sinon.stub().resolves(), assertQueue: sinon.stub().resolves({ queue: 'generated' }),
    bindQueue: sinon.stub().resolves(), unbindQueue: sinon.stub().resolves(),
    consume: sinon.stub().resolves({ consumerTag: 'consumer' }),
  })
  const model = () => Object.assign(new EventEmitter(), {
    createChannel: sinon.stub().callsFake(async () => channel()),
    createConfirmChannel: sinon.stub().callsFake(async () => channel()), close: sinon.stub().resolves(),
  })
  function client(id, config = {}) {
    const node = { id, type: config.type || 'amqp-out', status: sinon.stub(),
      log: sinon.stub(), warn: sinon.stub(), error: sinon.stub(), send: sinon.stub() }
    const amqp = new Amqp({ nodes: { getNode: () => broker } }, node,
      { ...nodeConfigFixture, ...config })
    clients.push(amqp)
    return { amqp, node }
  }
  beforeEach(() => {
    broker = { ...brokerConfigFixture, nodeStates: {}, lastError: {} }
    clients = []
    wrapper = Object.assign(new EventEmitter(), { close: sinon.stub().resolves() })
    connect = sinon.stub(amqplib, 'connect').callsFake(async (_url, opts) => {
      options = opts.recovery
      return wrapper
    })
  })
  afterEach(async () => {
    for (const amqp of clients) await amqp.close()
    sinon.restore()
  })
  async function recover(raw = model()) {
    await options.setup(raw)
    wrapper.emit('connect', raw)
    return raw
  }
  it('enables cancellable startup, unlimited retries, exponential backoff and jitter', async () => {
    await client('one').amqp.connect()
    expect(options).to.include({ waitForConnect: false, maxRetries: Infinity, factor: 2 })
    expect(options.initialDelay).to.be.greaterThan(0)
    expect(options.maxDelay).to.be.greaterThan(options.initialDelay)
    expect(options.jitter).to.be.greaterThan(0)
    expect(connect.firstCall.args[0]).to.equal('amqp://username:password@host:222/')
  })
  it('shares concurrent startup and restores every owner including confirm channels and consumers', async () => {
    const out = client('out', { waitForConfirms: true })
    const input = client('in', { type: 'amqp-in', autoCreateQueue: true, autoCreateExchangeBindings: true })
    await Promise.all([out.amqp.connect(), input.amqp.connect()])
    expect(connect.callCount).to.equal(1)
    expect(broker.nodeStates.out).not.to.equal('connected')
    const first = await recover()
    expect(first.createConfirmChannel.callCount).to.equal(1)
    expect(input.amqp.channel.consume.callCount).to.equal(1)
    wrapper.emit('disconnect', new Error('outage'))
    expect(broker.nodeStates).to.deep.equal({ out: 'disconnected', in: 'disconnected' })
    wrapper.emit('connect-failed', new Error('still unavailable'))
    expect(broker.nodeStates.out).to.equal('errored')
    const second = await recover()
    expect(second.createConfirmChannel.callCount).to.equal(1)
    expect(input.amqp.channel.bindQueue.callCount).to.equal(1)
    expect(input.amqp.channel.consume.callCount).to.equal(1)
    await out.amqp.publish('after recovery')
    expect(out.amqp.channel.waitForConfirms.callCount).to.equal(1)
    expect(broker.nodeStates).to.deep.equal({ out: 'connected', in: 'connected' })
    expect(broker.lastError).to.deep.equal({})
  })
  it('retains a recovering connection until its final owner closes', async () => {
    const a = client('a').amqp, b = client('b').amqp
    await Promise.all([a.connect(), b.connect()])
    wrapper.emit('connect-failed', new Error('startup unavailable'))
    await a.close()
    expect(wrapper.close.called).to.equal(false)
    await recover()
    expect(broker.nodeStates.a).not.to.equal('connected')
    expect(broker.nodeStates.b).to.equal('connected')
    await b.close()
    expect(wrapper.close.callCount).to.equal(1)
  })
  for (const switching of [false, true]) {
    it(`unbinds an exclusive non-auto-delete queue on ${switching ? 'vhost switching' : 'shutdown'} without disturbing its shared peer`, async () => {
      wrapper.waitForConnect = sinon.stub().resolves()
      const ephemeral = client('ephemeral', { type: 'amqp-in', queueName: 'exclusive',
        autoCreateQueue: true, autoCreateExchangeBindings: true,
        exchangeRoutingKey: 'first,second', queueExclusive: true, queueAutoDelete: false }).amqp
      const durable = client('durable', { type: 'amqp-in', queueName: 'durable',
        autoCreateQueue: true, autoCreateExchangeBindings: true,
        queueExclusive: false, queueDurable: true, queueAutoDelete: false }).amqp
      await Promise.all([ephemeral.connect(), durable.connect()])
      const raw = await recover()
      const old = ephemeral.channel, peer = durable.channel
      if (switching) await ephemeral.setVhost('another')
      else await ephemeral.close()
      expect(old.unbindQueue.args.map(args => args.slice(0, 3))).to.deep.equal([
        ['generated', 'exchangeName', 'first'], ['generated', 'exchangeName', 'second'],
      ])
      expect(old.unbindQueue.calledBefore(old.close)).to.equal(true)
      expect(peer.unbindQueue.called).to.equal(false)
      expect(peer.close.called).to.equal(false)
      expect(wrapper.close.called).to.equal(false)
      expect(raw.close.called).to.equal(false)
      await durable.close()
      expect(peer.unbindQueue.called).to.equal(false)
    })
  }
  it('does not unbind ephemeral queues during automatic recovery', async () => {
    const { amqp } = client('in', { type: 'amqp-in', autoCreateQueue: true,
      autoCreateExchangeBindings: true, queueExclusive: true })
    await amqp.connect()
    await recover()
    const old = amqp.channel
    wrapper.emit('disconnect', new Error('outage'))
    await recover()
    expect(old.unbindQueue.called).to.equal(false)
    expect(amqp.channel.bindQueue.calledOnce).to.equal(true)
  })
  for (const config of [
    { queueName: '', queueExclusive: false },
    { queueName: 'auto-delete', queueExclusive: false, queueAutoDelete: true },
    { exchangeType: 'fanout', exchangeRoutingKey: 'ignored' },
    { exchangeType: 'headers', exchangeRoutingKey: 'ignored', headers: '{"x-match":"all","kind":"test"}' },
  ]) {
    it(`cleans up temporary bindings with options ${JSON.stringify(config)}`, async () => {
      const { amqp } = client('temporary', { type: 'amqp-in', autoCreateQueue: true,
        autoCreateExchangeBindings: true, ...config })
      await amqp.connect()
      await recover()
      const old = amqp.channel
      expect(old.bindQueue.calledOnce).to.equal(true)
      await amqp.close()
      expect(old.unbindQueue.calledOnce).to.equal(true)
      expect(old.unbindQueue.firstCall.args).to.deep.equal([
        ...old.bindQueue.firstCall.args.slice(0, 3),
        config.exchangeType === 'headers' ? { 'x-match': 'all', kind: 'test' } : undefined,
      ])
    })
  }
  it('finishes shutdown after an unbind error without restarting the shared transport', async () => {
    const { amqp, node } = client('closing', { type: 'amqp-in', autoCreateQueue: true,
      autoCreateExchangeBindings: true, exchangeRoutingKey: 'first,second' })
    const peer = client('peer').amqp
    await Promise.all([amqp.connect(), peer.connect()])
    const raw = await recover(), old = amqp.channel
    old.unbindQueue.onFirstCall().callsFake(async () => {
      old.emit('error', new Error('unbind denied'))
      throw new Error('unbind denied')
    })
    await amqp.close()
    expect(old.unbindQueue.calledTwice).to.equal(true)
    expect(node.error.calledWithMatch(/Error unbinding queue.*unbind denied/)).to.equal(true)
    expect(old.close.calledOnce).to.equal(true)
    expect(raw.close.called).to.equal(false)
    expect(wrapper.close.called).to.equal(false)
    expect(old.listenerCount('error')).to.equal(0)
    expect(old.listenerCount('close')).to.equal(0)
  })
  it('finishes channel shutdown before closing the final shared transport', async () => {
    const { amqp } = client('one')
    await amqp.connect()
    await recover()
    let finish
    amqp.channel.close.callsFake(() => new Promise(resolve => { finish = resolve }))
    const closing = amqp.close()
    await new Promise(resolve => setImmediate(resolve))
    expect(wrapper.close.called).to.equal(false)
    finish()
    await closing
    expect(wrapper.close.callCount).to.equal(1)
  })
  it('waits for other owners already closing before closing their shared transport', async () => {
    const first = client('first').amqp, last = client('last').amqp
    await Promise.all([first.connect(), last.connect()])
    await recover()
    let finish
    first.channel.close.callsFake(() => new Promise(resolve => { finish = resolve }))
    const closing = Promise.all([first.close(), last.close()])
    await new Promise(resolve => setImmediate(resolve))
    expect(wrapper.close.called).to.equal(false)
    finish()
    await closing
    expect(wrapper.close.callCount).to.equal(1)
  })
  it('releases a failed handle acquisition so a later owner can connect', async () => {
    connect.onFirstCall().rejects(new Error('invalid connection options'))
    const failed = client('failed').amqp
    try { await failed.connect(); expect.fail() } catch (error) {
      expect(error.message).to.equal('invalid connection options')
    }
    await client('next').amqp.connect()
    await recover()
    expect(connect.callCount).to.equal(2)
    expect(broker.nodeStates.next).to.equal('connected')
  })
  for (const late of [false, true]) {
    it(`isolates a failing consumer ${late ? 'joining a healthy connection' : 'during startup'}`, async () => {
      const out = client('out', { waitForConfirms: true }).amqp
      await out.connect()
      const raw = model(), broken = channel()
      broken.consume.callsFake(async () => {
        broken.emit('error', new Error('queue unavailable'))
        broken.emit('close')
        throw new Error('queue unavailable')
      })
      if (late) await recover(raw)
      raw.createChannel.resolves(broken)
      const input = client('in', { type: 'amqp-in', queueName: 'missing' }).amqp
      await input.connect()
      if (!late) await recover(raw)
      await new Promise(resolve => setImmediate(resolve))
      expect(broker.nodeStates).to.deep.equal({ out: 'connected', in: 'errored' })
      expect(broker.lastError.in.message).to.equal('queue unavailable')
      expect(raw.close.called).to.equal(false)
      expect(wrapper.close.called).to.equal(false)
      await out.publish('healthy publisher')
      expect(out.channel.publish.callCount).to.equal(1)
      wrapper.emit('disconnect', new Error('transport lost'))
      await recover()
      expect(broker.nodeStates).to.deep.equal({ out: 'connected', in: 'connected' })
      expect(input.channel.consume.callCount).to.equal(1)
    })
  }
  it('rejects publishes during disconnection and surfaces uncertain confirms without replay', async () => {
    const { amqp } = client('out', { waitForConfirms: true })
    await amqp.connect()
    await recover()
    const old = amqp.channel
    old.waitForConfirms.rejects(new Error('channel closed before confirmation'))
    try { await amqp.publish('uncertain'); expect.fail() } catch (error) {
      expect(error.message).to.include('before confirmation')
    }
    wrapper.emit('disconnect', new Error('outage'))
    try { await amqp.publish('offline'); expect.fail() } catch (error) {
      expect(error.message).to.include('not ready')
    }
    await recover()
    expect(amqp.channel.publish.called).to.equal(false)
  })
  it('restores owners joining an already connected pool without reopening the transport', async () => {
    await client('first').amqp.connect()
    const raw = await recover()
    const late = client('late', { type: 'amqp-in-manual-ack', autoCreateQueue: true })
    await late.amqp.connect()
    expect(connect.callCount).to.equal(1)
    expect(raw.createChannel.callCount).to.equal(2)
    expect(late.amqp.channel.consume.callCount).to.equal(1)
    expect(broker.nodeStates.late).to.equal('connected')
  })
  it('includes a new owner while another owner is setting up', async () => {
    const first = client('first').amqp
    await first.connect()
    const raw = model()
    let finish
    raw.createChannel.callsFake(() => new Promise(resolve => { finish = resolve }))
    const setup = options.setup(raw)
    await new Promise(resolve => setImmediate(resolve))
    const late = client('late', { waitForConfirms: true }).amqp
    await late.connect()
    finish(channel())
    await setup
    wrapper.emit('connect', raw)
    await new Promise(resolve => setImmediate(resolve))
    expect(broker.nodeStates).to.deep.equal({ first: 'connected', late: 'connected' })
    expect(raw.createConfirmChannel.callCount).to.equal(1)
  })
  for (const type of ['amqp-out', 'amqp-in']) {
    it(`rejects shared setup when a restored ${type} channel closes while a peer is still initializing`, async () => {
      const first = client('first', { type, queueName: 'first', waitForConfirms: type === 'amqp-out' })
      const peer = client('peer').amqp
      await Promise.all([first.amqp.connect(), peer.connect()])
      const raw = model(), restored = channel()
      let finishPeer
      const pending = () => new Promise(resolve => { finishPeer = resolve })
      if (type === 'amqp-out') {
        raw.createConfirmChannel.resolves(restored)
        raw.createChannel.callsFake(pending)
      } else {
        raw.createChannel.onFirstCall().resolves(restored)
        raw.createChannel.onSecondCall().callsFake(pending)
      }
      raw.close.callsFake(async () => { raw.emit('close') })
      const setupResult = options.setup(raw).then(() => undefined, error => error)
      await new Promise(resolve => setImmediate(resolve))
      expect(first.amqp.settingUp).to.equal(false)
      expect(first.amqp.channel).to.equal(restored)
      restored.emit('close')
      await new Promise(resolve => setImmediate(resolve))
      finishPeer(channel())
      expect(await setupResult).to.be.instanceOf(Error)
      expect(raw.close.calledOnce).to.equal(true)
      // A late native connect event must not revive the invalidated setup result.
      wrapper.emit('connect', raw)
      await new Promise(resolve => setImmediate(resolve))
      expect(first.node.status.calledWithMatch({ text: 'Connected' })).to.equal(false)
      expect(broker.nodeStates.first).not.to.equal('connected')
      expect(first.amqp.channel).to.equal(undefined)
      wrapper.emit('disconnect', new Error('setup interrupted'))
      await recover()
      await new Promise(resolve => setImmediate(resolve))
      expect(broker.nodeStates).to.deep.equal({ first: 'connected', peer: 'connected' })
      expect(first.amqp.channel).not.to.equal(restored)
      if (type === 'amqp-out') {
        await first.amqp.publish('after valid recovery')
        expect(first.amqp.channel.waitForConfirms.calledOnce).to.equal(true)
      } else {
        expect(first.amqp.channel.consume.calledOnce).to.equal(true)
      }
      expect(connect.calledOnce).to.equal(true)
    })
  }
  it('closes a channel that arrives after its node was shut down', async () => {
    const { amqp, node } = client('closing')
    await amqp.connect()
    const raw = model(), pending = channel()
    let finish
    raw.createChannel.callsFake(() => new Promise(resolve => { finish = resolve }))
    const setup = options.setup(raw)
    await new Promise(resolve => setImmediate(resolve))
    await amqp.close()
    finish(pending)
    try { await setup } catch (error) { expect(error.message).to.include('cancelled') }
    wrapper.emit('connect', raw)
    await new Promise(resolve => setImmediate(resolve))
    expect(wrapper.close.callCount).to.equal(1)
    expect(pending.close.callCount).to.equal(1)
    expect(node.status.calledWithMatch({ text: 'Connected' })).to.equal(false)
  })
  it('does not fail shared setup when one owner closes during channel creation', async () => {
    const closing = client('closing').amqp, survivor = client('survivor', { waitForConfirms: true }).amqp
    await Promise.all([closing.connect(), survivor.connect()])
    const raw = model(), pending = channel()
    let finish
    raw.createChannel.callsFake(() => new Promise(resolve => { finish = resolve }))
    const setup = options.setup(raw)
    await new Promise(resolve => setImmediate(resolve))
    await closing.close()
    finish(pending)
    await setup
    wrapper.emit('connect', raw)
    await new Promise(resolve => setImmediate(resolve))
    expect(wrapper.close.called).to.equal(false)
    expect(raw.close.called).to.equal(false)
    expect(pending.close.callCount).to.equal(1)
    expect(broker.nodeStates.survivor).to.equal('connected')
  })
  it('lets native recovery handle channel closure and manual control without another connect call', async () => {
    const { amqp } = client('one')
    await amqp.connect()
    const raw = await recover()
    amqp.channel.emit('close')
    await amqp.reconnect()
    expect(raw.close.callCount).to.equal(1)
    expect(connect.callCount).to.equal(1)
    wrapper.emit('disconnect', new Error('closed'))
    await amqp.reconnect()
    expect(connect.callCount).to.equal(1)
    await recover()
    expect(broker.nodeStates.one).to.equal('connected')
  })
  it('releases the old vhost recovery before starting another one', async () => {
    wrapper.waitForConnect = sinon.stub().resolves()
    const { amqp } = client('one')
    await amqp.connect()
    await amqp.setVhost('another')
    expect(wrapper.close.callCount).to.equal(1)
    expect(connect.callCount).to.equal(2)
    expect(connect.secondCall.args[0]).to.equal('amqp://username:password@host:222/another')
    await amqp.close()
    expect(wrapper.close.callCount).to.equal(2)
  })
  it('bounds a switch to an unavailable vhost and clears its deadline', async () => {
    const clock = sinon.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
    try {
      wrapper.waitForConnect = sinon.stub().returns(new Promise(() => {}))
      const { amqp } = client('one')
      await amqp.connect()
      let result
      const switching = amqp.setVhost('unavailable').then(() => { result = 'ready' }, error => { result = error })
      await new Promise(resolve => setImmediate(resolve))
      await clock.tickAsync(9999)
      expect(result).to.equal(undefined)
      await clock.tickAsync(1)
      expect(result).to.be.instanceOf(Error)
      expect(result.message).to.include('Timed out')
      await switching
      expect(clock.countTimers()).to.equal(0)
      // The node still owns the destination, so native recovery can succeed later.
      await recover()
      await amqp.publish('after recovery')
      expect(amqp.channel.publish.callCount).to.equal(1)
    } finally {
      clock.restore()
    }
  })
  it('cancels a vhost readiness wait on close even when another owner retains the connection', async () => {
    const clock = sinon.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
    try {
      wrapper.waitForConnect = sinon.stub().returns(new Promise(() => {}))
      const { amqp } = client('one')
      await amqp.connect()
      const peer = client('peer').amqp
      peer.vhostOverride = 'unavailable'
      await peer.connect()
      let result
      const switching = amqp.setVhost('unavailable').then(() => { result = 'ready' }, error => { result = error })
      await new Promise(resolve => setImmediate(resolve))
      await amqp.close()
      await new Promise(resolve => setImmediate(resolve))
      expect(result).to.be.instanceOf(Error)
      expect(result.message).to.include('cancelled')
      await switching
      expect(wrapper.close.callCount).to.equal(1)
      expect(clock.countTimers()).to.equal(0)
    } finally {
      clock.restore()
    }
  })
  it('does not start another vhost connection when shutdown interrupts the switch', async () => {
    const { amqp } = client('one')
    await amqp.connect()
    let finish
    wrapper.close.callsFake(() => new Promise(resolve => { finish = resolve }))
    const switching = amqp.setVhost('another').then(() => undefined, error => error)
    await new Promise(resolve => setImmediate(resolve))
    await amqp.close()
    finish()
    expect((await switching).message).to.include('cancelled during shutdown')
    expect(connect.callCount).to.equal(1)
  })
  it('does not let cancelled setup close a replacement vhost channel', async () => {
    wrapper.waitForConnect = sinon.stub().resolves()
    const { amqp } = client('one')
    await amqp.connect()
    const old = model(), pending = channel()
    let finish
    old.createChannel.callsFake(() => new Promise(resolve => { finish = resolve }))
    const setup = options.setup(old)
    await new Promise(resolve => setImmediate(resolve))
    await amqp.setVhost('replacement')
    await recover()
    const replacement = amqp.channel
    finish(pending)
    await setup
    expect(pending.close.callCount).to.equal(1)
    expect(replacement.close.called).to.equal(false)
    await amqp.publish('replacement still ready')
    expect(replacement.publish.callCount).to.equal(1)
  })
})
