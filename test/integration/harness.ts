export {}
const { RabbitMQContainer } = require('@testcontainers/rabbitmq')
const { connect } = require('amqplib')
const { randomUUID } = require('node:crypto')
const { setTimeout: delay } = require('node:timers/promises')
const helper = require('node-red-node-test-helper')

helper.init(require.resolve('node-red'))
const modules = ['amqp-broker', 'amqp-in', 'amqp-in-manual-ack', 'amqp-out']
  .map(name => require(`../../build/src/nodes/${name}`))

// A patch-pinned management image allows broker-side queue/consumer assertions.
const IMAGE = 'rabbitmq:4.3.6-management'

async function until(description, probe, timeout = 15000) {
  const deadline = Date.now() + timeout
  let last
  do {
    last = await probe()
    if (last) return last
    await delay(100)
  } while (Date.now() < deadline)
  throw new Error(`Timed out after ${timeout}ms: ${description}; last result: ${JSON.stringify(last)}`)
}

function nextMessage(node) {
  // Buffer deliveries immediately so a second delivery cannot race the assertion.
  const messages = []
  node.on('input', msg => messages.push(msg))
  return () => until('Node-RED output message', () => messages.shift())
}

class Harness {
  container
  connection
  channel
  queues = []
  exchanges = []
  ephemeralQueues = new Set()

  async start() {
    this.container = await new RabbitMQContainer(IMAGE)
      .withEnvironment({ RABBITMQ_DEFAULT_USER: 'integration', RABBITMQ_DEFAULT_PASS: 'integration' })
      .withStartupTimeout(120000).start()
    await this.observe()
  }

  async observe() {
    const url = new URL(this.container.getAmqpUrl())
    url.username = 'integration'
    url.password = 'integration'
    this.connection = await connect(url.toString())
    this.connection.on('error', error => console.error('Observer connection:', error))
    this.channel = await this.connection.createConfirmChannel()
    this.channel.on('error', error => console.error('Observer channel:', error))
  }

  async pause() {
    await this.connection?.close()
    this.connection = undefined
    const result = await this.container.exec(['rabbitmqctl', 'stop_app'])
    if (result.exitCode !== 0) throw new Error(result.output)
  }

  async resume() {
    const result = await this.container.exec(['rabbitmqctl', 'start_app'])
    if (result.exitCode !== 0) throw new Error(result.output)
    await this.observe()
  }

  async stop() {
    try {
      await this.connection?.close()
    } finally {
      await this.container?.stop()
    }
  }

  async queue(name) {
    const response = await fetch(`http://${this.container.getHost()}:${this.container.getMappedPort(15672)}/api/queues/%2F/${encodeURIComponent(name)}`, {
      headers: { Authorization: `Basic ${Buffer.from('integration:integration').toString('base64')}` },
      signal: AbortSignal.timeout(5000),
    })
    if (!response.ok) throw new Error(`Queue ${name}: HTTP ${response.status}`)
    return response.json()
  }

  async connections() {
    const response = await fetch(`http://${this.container.getHost()}:${this.container.getMappedPort(15672)}/api/connections`, {
      headers: { Authorization: `Basic ${Buffer.from('integration:integration').toString('base64')}` },
      signal: AbortSignal.timeout(5000),
    })
    if (!response.ok) throw new Error(`Connections: HTTP ${response.status}`)
    return response.json()
  }

  topology() {
    const queue = `integration-${randomUUID()}`
    const exchange = `${queue}-exchange`
    this.queues.push(queue)
    this.exchanges.push(exchange)
    return { queue, exchange, key: 'integration.message' }
  }

  async load(nodes, waitReady = true) {
    for (const node of nodes) {
      if (node.queueName && (node.queueExclusive || node.queueAutoDelete)) {
        this.ephemeralQueues.add(node.queueName)
      }
    }
    await new Promise((resolve, reject) => helper.startServer(err => err ? reject(err) : resolve(undefined)))
    const brokerIds = [...new Set(nodes.map(node => node.broker).filter(Boolean))]
    const brokers = brokerIds.map(id => ({
      id, type: 'amqp-broker', host: this.container.getHost(),
      port: this.container.getMappedPort(5672), vhost: '/',
    }))
    const credentials = Object.fromEntries(brokerIds.map(id => [id, {
      username: 'integration', password: 'integration',
    }]))
    await helper.load(modules, [{ id: 'flow', type: 'tab', label: 'Integration' },
      ...brokers, ...nodes.map(node => ({ z: 'flow', ...node }))], credentials)
    if (waitReady) await this.ready()
  }

  async ready() {
    await until('all runtime nodes connected', async () => {
      const response = await helper.request().get('/amqp-broker/health')
      return response.status === 200
    }, 45000)
  }

  async cleanup() {
    try {
      await helper.unload()
    } finally {
      await new Promise((resolve, reject) => helper.stopServer(err => err ? reject(err) : resolve(undefined)))
      for (const queue of this.queues.splice(0)) {
        if (this.ephemeralQueues.delete(queue)) {
          // RabbitMQ deletes exclusive queues asynchronously after transport close.
          await until(`exclusive queue ${queue} removed on unload`, async () => {
            try { await this.queue(queue); return false }
            catch (error) {
              if (/HTTP 404/.test(String(error))) return true
              throw error
            }
          })
        } else {
          await this.channel.deleteQueue(queue)
        }
      }
      for (const exchange of this.exchanges.splice(0)) await this.channel.deleteExchange(exchange)
    }
  }

  async diagnostics(title) {
    console.error(`RabbitMQ integration failure: ${title}`)
    for (const queue of this.queues) {
      try { console.error(JSON.stringify(await this.queue(queue))) }
      catch (error) { console.error(String(error)) }
    }
    if (this.container) {
      const result = await this.container.exec(['rabbitmq-diagnostics', 'status'])
      console.error(result.output)
    }
  }
}

function consumer({ queue, exchange, key }, overrides = {}) {
  return {
    id: 'in', type: 'amqp-in', broker: 'broker', wires: [['sink']],
    queueName: queue, queueType: 'classic', queueDurable: true,
    queueExclusive: false, queueAutoDelete: false, autoCreateQueue: true,
    exchangeName: exchange, exchangeType: 'direct', exchangeDurable: true,
    exchangeRoutingKey: key, autoCreateExchangeBindings: true,
    noAck: false, prefetch: 1, ...overrides,
  }
}

module.exports = { Harness, consumer, helper, nextMessage, until }
