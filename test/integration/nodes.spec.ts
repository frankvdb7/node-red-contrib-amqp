import { expect } from 'chai'
import { Harness, consumer, helper, nextMessage, until } from './harness'

describe('compiled AMQP nodes against RabbitMQ', function () {
  this.timeout(30000)
  const broker = new Harness()

  before(async function () {
    this.timeout(180000)
    await broker.start()
  })
  after(async function () {
    this.timeout(60000)
    await broker.stop()
  })
  afterEach(async function () {
    try {
      if (this.currentTest.state === 'failed') await broker.diagnostics(this.currentTest.fullTitle())
    } finally {
      await broker.cleanup()
    }
  })

  for (const queueType of ['classic', 'quorum']) {
    it(`publishes confirmed messages through amqp-out to amqp-in with an auto-created ${queueType} queue`, async () => {
      const topology = broker.topology()
      await broker.load([
        consumer(topology, { queueType }), { id: 'sink', type: 'helper' },
        // Shared broker-config lifecycle is a separate pending regression (#184).
        { id: 'out', type: 'amqp-out', broker: 'publisher-broker', exchangeName: topology.exchange,
          exchangeType: 'direct', exchangeRoutingKey: topology.key, waitForConfirms: true },
        { id: 'completion', type: 'complete', scope: ['out'], wires: [['confirmed']] },
        { id: 'confirmed', type: 'helper' },
      ])
      const receive = nextMessage(helper.getNode('sink'))
      const payload = { test: queueType, value: 42 }
      const completed = nextMessage(helper.getNode('confirmed'))
      helper.getNode('out').receive({ payload })
      const message = await receive()
      expect((await completed()).payload).to.deep.equal(payload)
      expect(message.payload).to.deep.equal(payload)
      expect(message.fields.routingKey).to.equal(topology.key)
      const queue = await broker.queue(topology.queue)
      expect(queue.type).to.equal(queueType)
      await until('automatic acknowledgement and empty queue', async () => {
        const state = await broker.queue(topology.queue)
        return state.messages === 0 && state.messages_unacknowledged === 0 &&
          state.message_stats?.ack === 1
      })
      const delivered = await broker.queue(topology.queue)
      expect(delivered.message_stats.publish).to.equal(1)
      expect(delivered.message_stats.deliver_get).to.equal(1)
      // Confirm processing completion independently of delivery and publisher confirms.
      expect((await broker.channel.checkQueue(topology.queue)).messageCount).to.equal(0)
    })
  }

  it('consumes pre-provisioned topology without auto-create and removes its consumer on unload', async () => {
    const topology = broker.topology()
    await broker.channel.assertExchange(topology.exchange, 'direct', { durable: true })
    await broker.channel.assertQueue(topology.queue, { durable: true })
    await broker.channel.bindQueue(topology.queue, topology.exchange, topology.key)
    await broker.load([
      consumer(topology, { autoCreateQueue: false, autoCreateExchangeBindings: false }),
      { id: 'sink', type: 'helper' },
    ])
    const receive = nextMessage(helper.getNode('sink'))
    broker.channel.publish(topology.exchange, topology.key, Buffer.from('{"existing":true}'))
    await broker.channel.waitForConfirms()
    expect((await receive()).payload).to.deep.equal({ existing: true })
    expect((await broker.channel.checkQueue(topology.queue)).consumerCount).to.equal(1)
    const closingAt = Date.now()
    await helper.unload()
    expect(Date.now() - closingAt).to.be.lessThan(5000)
    await until('consumer removed after unload', async () =>
      (await broker.channel.checkQueue(topology.queue)).consumerCount === 0)
    expect((await helper.request().get('/amqp-broker/health')).status).to.equal(503)
  })

  it('unbinds an exclusive non-auto-delete queue on node close while retaining the shared transport and durable bindings', async () => {
    const exclusive = broker.topology(), durable = broker.topology()
    await broker.load([
      consumer(exclusive, { id: 'exclusive', queueExclusive: true, queueDurable: false, queueAutoDelete: false }),
      consumer(durable, { id: 'durable' }), { id: 'sink', type: 'helper' },
    ])
    const connectionNames = (await broker.connections()).map(connection => connection.name).sort()
    expect((await broker.bindings(exclusive.queue)).some(binding => binding.source === exclusive.exchange)).to.equal(true)
    await helper.getNode('exclusive').close()
    const remaining = await broker.queue(exclusive.queue)
    expect(remaining.exclusive).to.equal(true)
    expect(remaining.auto_delete).to.equal(false)
    expect(remaining.consumer_details).to.have.length(0)
    expect((await broker.bindings(exclusive.queue)).some(binding => binding.source === exclusive.exchange)).to.equal(false)
    expect((await broker.connections()).map(connection => connection.name).sort()).to.deep.equal(connectionNames)
    expect((await broker.bindings(durable.queue)).some(binding => binding.source === durable.exchange)).to.equal(true)
    const receive = nextMessage(helper.getNode('sink'))
    broker.channel.publish(durable.exchange, durable.key, Buffer.from('"surviving consumer"'))
    await broker.channel.waitForConfirms()
    expect((await receive()).payload).to.equal('surviving consumer')
    await helper.getNode('durable').close()
    expect((await broker.bindings(durable.queue)).some(binding => binding.source === durable.exchange)).to.equal(true)
  })

  it('manually nacks/requeues, observes redelivery, then acks without redelivery on shutdown', async () => {
    const topology = broker.topology()
    const flow = [consumer(topology, { type: 'amqp-in-manual-ack' }), { id: 'sink', type: 'helper' }]
    await broker.load(flow)
    const receive = nextMessage(helper.getNode('sink'))
    broker.channel.publish(topology.exchange, topology.key, Buffer.from('{"manual":true}'))
    await broker.channel.waitForConfirms()
    const first = await receive()
    expect(first.payload).to.deep.equal({ manual: true })
    expect(first.fields.redelivered).to.equal(false)
    await until('message remains unacknowledged', async () =>
      (await broker.queue(topology.queue)).messages_unacknowledged === 1)
    helper.getNode('in').receive({ ...first, manualAck: { ackMode: 'nack', requeue: true } })
    const redelivery = await receive()
    expect(redelivery.payload).to.deep.equal(first.payload)
    expect(redelivery.fields.redelivered).to.equal(true)
    helper.getNode('in').receive({ ...redelivery, manualAck: { ackMode: 'ack' } })
    await until('manual acknowledgement reaches broker', async () => {
      const queue = await broker.queue(topology.queue)
      return queue.messages === 0 && queue.messages_unacknowledged === 0
    })
    await helper.unload()
    expect((await broker.channel.checkQueue(topology.queue)).consumerCount).to.equal(0)
    expect(await broker.channel.get(topology.queue)).to.equal(false)
  })

  for (const failure of ['missing queue', 'incompatible queue']) {
    it(`keeps shared healthy nodes usable despite ${failure}`, async () => {
      const healthy = broker.topology(), broken = broker.topology()
      if (failure === 'incompatible queue') {
        await broker.channel.assertQueue(broken.queue, { durable: true })
      }
      await broker.load([
        consumer(healthy), { id: 'sink', type: 'helper' },
        consumer(broken, { id: 'broken', autoCreateQueue: failure === 'incompatible queue',
          queueDurable: false, autoCreateExchangeBindings: false, wires: [] }),
        { id: 'out', type: 'amqp-out', broker: 'broker', exchangeName: healthy.exchange,
          exchangeRoutingKey: healthy.key, waitForConfirms: true },
        { id: 'completion', type: 'complete', scope: ['out'], wires: [['confirmed']] },
        { id: 'confirmed', type: 'helper' },
      ], false)
      await until('healthy nodes ready and failing node isolated', () => {
        const states = helper.getNode('broker').nodeStates
        return states.in === 'connected' && states.out === 'connected' && states.broken === 'errored'
      })
      const receive = nextMessage(helper.getNode('sink'))
      const confirmed = nextMessage(helper.getNode('confirmed'))
      helper.getNode('out').receive({ payload: failure })
      expect((await receive()).payload).to.equal(failure)
      expect((await confirmed()).payload).to.equal(failure)
      expect((await helper.request().get('/amqp-broker/health')).status).to.equal(503)
      expect((await broker.channel.checkQueue(healthy.queue)).consumerCount).to.equal(1)
    })
  }

  it('cancels startup recovery on unload without opening orphan connections (#181)', async function () {
    this.timeout(60000)
    const topology = broker.topology()
    let paused = false
    try {
      await broker.pause(); paused = true
      await broker.load([consumer(topology), { id: 'sink', type: 'helper' },
        { id: 'out', type: 'amqp-out', broker: 'broker' }], false)
      await until('startup connection failure', () => helper.getNode('broker').lastError.out)
      await helper.unload()
      await broker.resume(); paused = false
      await new Promise(resolve => setTimeout(resolve, 4000))
      // Only the test observer may connect after the runtime was unloaded.
      await until('only the observer connection remains after unload', async () =>
        (await broker.connections()).length === 1)
    } finally {
      if (paused) await broker.resume()
    }
  })

  for (const unavailableAtStartup of [true, false]) {
    it(`recovers shared consumers and publishers after ${unavailableAtStartup ? 'startup failure' : 'a sustained outage'} (#181)`, async function () {
      this.timeout(120000)
      const automatic = broker.topology(), manual = broker.topology()
      const nodes = [
        consumer(automatic, { queueExclusive: true, queueDurable: false, queueAutoDelete: true }),
        consumer(manual, { id: 'manual', type: 'amqp-in-manual-ack', wires: [['manual-sink']],
          queueExclusive: true, queueDurable: false, queueAutoDelete: true }),
        { id: 'sink', type: 'helper' }, { id: 'manual-sink', type: 'helper' },
        ...[false, true].map(confirm => ({
          id: confirm ? 'confirmed-out' : 'out', type: 'amqp-out', broker: 'broker',
          exchangeName: automatic.exchange, exchangeRoutingKey: automatic.key,
          waitForConfirms: confirm,
        })),
        { id: 'completion', type: 'complete', scope: ['confirmed-out'], wires: [['confirmed']] },
        { id: 'confirmed', type: 'helper' },
        { id: 'publish-errors', type: 'catch', scope: ['out'], wires: [['failed']] },
        { id: 'failed', type: 'helper' },
      ]
      let paused = false
      try {
        if (unavailableAtStartup) { await broker.pause(); paused = true }
        await broker.load(nodes, !unavailableAtStartup)
        if (!unavailableAtStartup) {
          await broker.pause(); paused = true
        }
        await until('broker health becomes unavailable', async () =>
          (await helper.request().get('/amqp-broker/health')).status === 503)
        const failed = nextMessage(helper.getNode('failed'))
        helper.getNode('out').receive({ payload: 'offline' })
        expect((await failed()).error.message).to.include('not ready')
        // Exceed the first three native retry intervals, including jitter.
        await new Promise(resolve => setTimeout(resolve, 10000))
        expect((await helper.request().get('/amqp-broker/health')).status).to.equal(503)
        expect(helper.getNode('broker').lastError.out.message).to.match(/AMQP retry [3-9]/)
        await broker.resume(); paused = false
        await broker.ready()
        const receive = nextMessage(helper.getNode('sink'))
        const confirmed = nextMessage(helper.getNode('confirmed'))
        for (const id of ['out', 'confirmed-out']) {
          helper.getNode(id).receive({ payload: { recovered: id } })
          expect((await receive()).payload).to.deep.equal({ recovered: id })
        }
        expect((await confirmed()).payload).to.deep.equal({ recovered: 'confirmed-out' })
        const manualReceive = nextMessage(helper.getNode('manual-sink'))
        broker.channel.publish(manual.exchange, manual.key, Buffer.from('{"manual":true}'))
        await broker.channel.waitForConfirms()
        const message = await manualReceive()
        helper.getNode('manual').receive(message)
        await until('recovered manual consumer acknowledges', async () =>
          (await broker.queue(manual.queue)).message_stats?.ack === 1)
        // Closing one owner must leave the shared transport and consumers running.
        await helper.getNode('out').close()
        helper.getNode('confirmed-out').receive({ payload: 'surviving owner' })
        expect((await receive()).payload).to.equal('surviving owner')
        await confirmed()
        expect((await broker.queue(automatic.queue)).consumer_details).to.have.length(1)
        expect((await broker.queue(manual.queue)).consumer_details).to.have.length(1)
      } finally {
        if (paused) await broker.resume()
      }
    })
  }

  // Known defects are intentionally pending until their dedicated fixes land.
  it.skip('isolates concurrent msg.vhost routing (#183)')
  it.skip('keeps shared connections alive when one node closes (#184)')
  it.skip('rejects stale manual acknowledgements after channel replacement (#185)')
  it.skip('validates invalid quorum declarations (#186)')
  it.skip('preserves non-RPC correlationId and replyTo (#187)')
  it.skip('reports mandatory publisher returns (#188)')
  it.skip('handles publisher backpressure and closed channels (#189)')
})
