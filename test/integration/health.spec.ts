import { expect } from 'chai'
import { Harness, consumer, helper, until } from './harness'

describe('AMQP broker health against RabbitMQ', function () {
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

  it('reports healthy when an AMQP consumer is connected', async () => {
    const topology = broker.topology()
    await broker.load([consumer(topology), { id: 'sink', type: 'helper' }])

    const response = await helper.request().get('/amqp-broker/health')
    expect(response.status).to.equal(200)
    expect(response.body).to.deep.equal({
      overallStatus: 'healthy',
      brokers: [{ id: 'broker', status: 'connected' }],
    })
  })

  it('reports unhealthy when no broker config node is active', async () => {
    await broker.load([], false)

    const response = await helper.request().get('/amqp-broker/health')
    expect(response.status).to.equal(503)
    expect(response.body).to.deep.equal({ overallStatus: 'unhealthy', brokers: [] })
  })

  it('reports unhealthy when one consumer fails while another remains connected', async () => {
    const healthy = broker.topology()
    const missing = broker.topology()
    await broker.load([
      consumer(healthy), { id: 'sink', type: 'helper' },
      consumer(missing, {
        id: 'broken', autoCreateQueue: false, autoCreateExchangeBindings: false, wires: [],
      }),
    ], false)
    await until('healthy and errored consumer states', () => {
      const states = helper.getNode('broker').nodeStates
      return states.in === 'connected' && states.broken === 'errored'
    })

    const response = await helper.request().get('/amqp-broker/health')
    expect(response.status).to.equal(503)
    expect(response.body.overallStatus).to.equal('unhealthy')
    expect(response.body.brokers).to.have.length(1)
    expect(response.body.brokers[0].status).to.equal('errored')
    expect(response.body.brokers[0].lastError).to.have.property('broken')
  })

  it('reports unhealthy during a broker outage and healthy after recovery', async function () {
    this.timeout(90000)
    const topology = broker.topology()
    await broker.load([consumer(topology), { id: 'sink', type: 'helper' }])

    let paused = false
    try {
      await broker.pause()
      paused = true
      const unavailable = await until('health endpoint reports broker outage', async () => {
        const response = await helper.request().get('/amqp-broker/health')
        return response.status === 503 ? response : false
      }, 45000)
      expect(unavailable.body.overallStatus).to.equal('unhealthy')
      expect(unavailable.body.brokers).to.have.length(1)
      expect(unavailable.body.brokers[0].status).not.to.equal('connected')

      await broker.resume()
      paused = false
      const recovered = await until('health endpoint reports recovered broker', async () => {
        const response = await helper.request().get('/amqp-broker/health')
        return response.status === 200 ? response : false
      }, 45000)
      expect(recovered.body.overallStatus).to.equal('healthy')
      expect(recovered.body.brokers).to.have.length(1)
      expect(recovered.body.brokers[0].status).to.equal('connected')
    } finally {
      if (paused) await broker.resume()
    }
  })
})
