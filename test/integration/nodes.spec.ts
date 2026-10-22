export {}
const { expect } = require('chai')
const { Harness, consumer, helper, nextMessage, until } = require('./harness')

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
        { id: 'out', type: 'amqp-out', broker: 'broker', exchangeName: topology.exchange,
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
    await helper.unload()
    await until('consumer removed after unload', async () =>
      (await broker.channel.checkQueue(topology.queue)).consumerCount === 0)
    expect((await helper.request().get('/amqp-broker/health')).status).to.equal(503)
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

  // Known defects are intentionally pending until their dedicated fixes land.
  it.skip('recovers from startup and sustained broker outages (#181)')
  it.skip('isolates concurrent msg.vhost routing (#183)')
  it.skip('keeps shared connections alive when one node closes (#184)')
  it.skip('rejects stale manual acknowledgements after channel replacement (#185)')
  it.skip('validates invalid quorum declarations (#186)')
  it.skip('preserves non-RPC correlationId and replyTo (#187)')
  it.skip('reports mandatory publisher returns (#188)')
  it.skip('handles publisher backpressure and closed channels (#189)')
})
