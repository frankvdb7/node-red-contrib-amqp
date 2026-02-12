import { NodeRedApp, Node } from 'node-red'
import { v4 as uuidv4 } from 'uuid'
import cloneDeep from 'lodash.clonedeep'
import {
  ChannelModel,
  Channel,
  ConfirmChannel,
  Replies,
  connect,
  ConsumeMessage,
  MessageProperties,
} from 'amqplib'
import {
  AmqpConfig,
  BrokerConfig,
  AmqpBrokerNode,
  NodeType,
  AssembledMessage,
  GenericJsonObject,
  JsonValue,
  JsonObject,
  ExchangeType,
  AmqpInNodeDefaults,
  AmqpOutNodeDefaults,
} from './types'
import { NODE_STATUS } from './constants'

export default class Amqp {
  private config: AmqpConfig
  private broker: AmqpBrokerNode
  private connection: ChannelModel
  private channel: Channel
  private q: Replies.AssertQueue
  private vhostOverride?: string
  private static connectionPool: Map<string, { connection: ChannelModel; count: number }> = new Map()
  private connectionErrorHandler: (e: unknown) => void
  private connectionCloseHandler: () => void
  private channelErrorHandler: (e: unknown) => void
  private channelCloseHandler: () => void
  private channelReturnHandler: () => void
  private closed = false

  constructor(
    private readonly RED: NodeRedApp,
    private readonly node: Node,
    config: AmqpInNodeDefaults & AmqpOutNodeDefaults,
  ) {
    this.config = {
      name: config.name,
      broker: config.broker,
      prefetch: config.prefetch,
      reconnectOnError: config.reconnectOnError,
      noAck: config.noAck,
      waitForConfirms: config.waitForConfirms ?? false,
      exchange: {
        name: config.exchangeName,
        type: config.exchangeType,
        routingKey: config.exchangeRoutingKey,
        durable: config.exchangeDurable,
      },
      queue: {
        name: config.queueName,
        exclusive: config.queueExclusive,
        durable: config.queueDurable,
        autoDelete: config.queueAutoDelete,
        queueType: config.queueType,
        queueArguments: this.parseJsonObject(config.queueArguments),
      },
      amqpProperties: this.parseJsonObject(
        config.amqpProperties,
      ) as unknown as MessageProperties,
      headers: this.parseJsonObject(config.headers),
      outputs: config.outputs,
      rpcTimeout: config.rpcTimeoutMilliseconds,
    }
  }

  public async connect(): Promise<ChannelModel> {
    const { broker } = this.config

    // wtf happened to the types?
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    this.broker = this.RED.nodes.getNode(broker)

    if (!this.broker) {
      const err = new Error(`AMQP broker node not found: ${broker}`)
      this.node.error(err.message)
      throw err
    }

    const brokerConfig: BrokerConfig = {
      ...(this.broker as unknown as BrokerConfig),
      vhost:
        this.vhostOverride ?? (this.broker as unknown as BrokerConfig).vhost,
    }

    const brokerUrl = this.getBrokerUrl(brokerConfig)
    const { host, port, vhost } = brokerConfig

    const brokerInfo = `${host}:${port}${vhost ? `/${vhost}` : ''}`
    const key = `${broker}:${vhost}`

    let entry = Amqp.connectionPool.get(key)
    if (!entry) {
      this.node.log(`Connecting to AMQP broker ${brokerInfo}`)
      try {
        const connection = await connect(brokerUrl, { heartbeat: 2 })
        this.node.log(`Connected to AMQP broker ${brokerInfo}`)
        entry = { connection, count: 0 }
        Amqp.connectionPool.set(key, entry)
      } catch (err) {
        this.node.warn(`Failed to connect to AMQP broker ${brokerInfo}: ${err}`)
        throw err
      }
    }

    this.broker.connections[this.node.id] = true
    this.node.status(NODE_STATUS.Connected)

    entry.count += 1
    this.connection = entry.connection

    this.connectionErrorHandler = (e): void => {
      /* istanbul ignore next */
      this.node.status(NODE_STATUS.Disconnected)
      this.node.warn(`AMQP connection error ${e}`)
    }

    this.connectionCloseHandler = (): void => {
      /* istanbul ignore next */
      this.broker.connections[this.node.id] = false
      this.node.status(NODE_STATUS.Disconnected)
      this.node.log(`AMQP Connection closed`)
    }

    this.connection.on('error', this.connectionErrorHandler)
    this.connection.on('close', this.connectionCloseHandler)

    this.closed = false

    return this.connection
  }

  public async initialize(): Promise<Channel> {
    await this.createChannel()
    await this.assertExchange()
    return this.channel;
  }

  public async consume(): Promise<void> {
    try {
      const { noAck } = this.config
      await this.assertQueue()
      await this.bindQueue()
      await this.channel.consume(
        this.q.queue,
        amqpMessage => {
          const msg = this.assembleMessage(amqpMessage)
          this.node.log(
            `Received message with deliveryTag: ${msg?.fields?.deliveryTag}`,
          )
          this.node.send(msg)
          /* istanbul ignore else */
          if (!noAck && !this.isManualAck()) {
            this.ack(msg)
          }
        },
        { noAck },
      )
    } catch (e) {
      this.node.error(`Could not consume message: ${e}`)
    }
  }

  public setRoutingKey(newRoutingKey: string): void {
    this.config.exchange.routingKey = newRoutingKey
  }

  public async setVhost(newVhost: string): Promise<void> {
    const broker = this.broker as unknown as BrokerConfig
    const currentVhost = this.vhostOverride ?? broker?.vhost

    if (!broker || currentVhost === newVhost) {
      return
    }

    try {
      await this.close()
      this.vhostOverride = newVhost
      await this.connect()
      await this.initialize()
    } catch (e) {
      this.node.error(`Could not switch vhost: ${e}`)
      throw e
    }
  }

  public getConnection(): ChannelModel {
    return this.connection
  }

  public getChannel(): Channel {
    return this.channel
  }

  public ack(msg: AssembledMessage): void {
    const allUpTo = !!msg.manualAck?.allUpTo
    try {
      this.node.log(`Acking message with deliveryTag: ${msg?.fields?.deliveryTag}`)
      this.channel.ack(msg, allUpTo)
    } catch (e) {
      this.node.error(`Could not ack message: ${e}`)
    }
  }

  public ackAll(): void {
    try {
      this.node.log('Acking all outstanding messages')
      this.channel.ackAll()
    } catch (e) {
      this.node.error(`Could not ackAll messages: ${e}`)
    }
  }

  public nack(msg: AssembledMessage): void {
    const allUpTo = !!msg.manualAck?.allUpTo
    const requeue = msg.manualAck?.requeue ?? true
    try {
      this.node.log(
        `Nacking message with deliveryTag: ${msg?.fields?.deliveryTag}`,
      )
      this.channel.nack(msg, allUpTo, requeue)
    } catch (e) {
      this.node.error(`Could not nack message: ${e}`)
    }
  }

  public nackAll(msg: AssembledMessage): void {
    const requeue = msg.manualAck?.requeue ?? true
    try {
      this.node.log('Nacking all outstanding messages')
      this.channel.nackAll(requeue)
    } catch (e) {
      this.node.error(`Could not nackAll messages: ${e}`)
    }
  }

  public reject(msg: AssembledMessage): void {
    const requeue = msg.manualAck?.requeue ?? true
    try {
      this.node.log(
        `Rejecting message with deliveryTag: ${msg?.fields?.deliveryTag}`,
      )
      this.channel.reject(msg, requeue)
    } catch (e) {
      this.node.error(`Could not reject message: ${e}`)
    }
  }

  public async publish(
    msg: unknown,
    properties?: MessageProperties,
  ): Promise<void> {
    const routingKeys = this.parseRoutingKeys()
    await Promise.all(
      routingKeys.map(routingKey =>
        this.handlePublish(this.config, msg, properties, routingKey),
      ),
    )
  }

  private async handlePublish(
    config: AmqpConfig,
    msg: unknown,
    properties?: MessageProperties,
    routingKey?: string,
  ) {
    const {
      exchange: { name },
      outputs: rpcRequested,
    } = config

    try {
      let correlationId = ''
      let replyTo = ''

      if (rpcRequested) {
        // Send request for remote procedure call
        correlationId =
          properties?.correlationId ||
          this.config.amqpProperties?.correlationId ||
          uuidv4()
        replyTo =
          properties?.replyTo || this.config.amqpProperties?.replyTo || uuidv4()
        await this.handleRemoteProcedureCall(correlationId, replyTo)
      }

      const options = {
        correlationId,
        replyTo,
        ...this.config.amqpProperties,
        ...properties,
      }
      // when the name field is empty, publish just like the sendToQueue method;
      // see https://amqp-node.github.io/amqplib/channel_api.html#channel_publish
      this.channel.publish(
        name,
        routingKey,
        Buffer.from(msg as string),
        options,
      )

      if (config.waitForConfirms) {
        await (this.channel as ConfirmChannel).waitForConfirms()
      }
    } catch (e) {
      this.node.error(`Could not publish message: ${e}`)
    }
  }

  private getRpcConfig(replyTo: string): AmqpConfig {
    const rpcConfig = cloneDeep(this.config)
    rpcConfig.exchange.name = ''
    rpcConfig.queue.name = replyTo
    rpcConfig.queue.autoDelete = true
    rpcConfig.queue.exclusive = true
    rpcConfig.queue.durable = false
    rpcConfig.noAck = true

    return rpcConfig
  }

  private async handleRemoteProcedureCall(
    correlationId: string,
    replyTo: string,
  ): Promise<void> {
    const rpcConfig = this.getRpcConfig(replyTo)

    try {
      // If we try to delete a queue that's already deleted
      // bad things will happen.
      let rpcQueueHasBeenDeleted = false
      let additionalErrorMessaging = ''

      /************************************
       * assert queue and set up consumer
       ************************************/
      const queueName = await this.assertQueue(rpcConfig)

      await this.channel.consume(
        queueName,
        async amqpMessage => {
          if (amqpMessage) {
            const msg = this.assembleMessage(amqpMessage)
            if (msg.properties.correlationId === correlationId) {
              this.node.send(msg)
              /* istanbul ignore else */
              if (!rpcQueueHasBeenDeleted) {
                await this.channel.deleteQueue(queueName)
                rpcQueueHasBeenDeleted = true
              }
            } else {
              additionalErrorMessaging += ` Correlation ids do not match. Expecting: ${correlationId}, received: ${msg.properties.correlationId}`
            }
          }
        },
        { noAck: rpcConfig.noAck },
      )

      /****************************************
       * Check if RPC has timed out and handle
       ****************************************/
      setTimeout(async () => {
        try {
          if (!rpcQueueHasBeenDeleted) {
            this.node.send({
              payload: {
                message: `Timeout while waiting for RPC response.${additionalErrorMessaging}`,
                config: rpcConfig,
              },
            })
            await this.channel.deleteQueue(queueName)
          }
        } catch (e) {
          // TODO: Keep an eye on this
          // This might close the whole channel
          this.node.error(`Error trying to cancel RPC consumer: ${e}`)
        }
      }, rpcConfig.rpcTimeout || 3000)
    } catch (e) {
      this.node.error(`Could not consume RPC message: ${e}`)
    }
  }

  public async close(): Promise<void> {
    if (this.closed) {
      return
    }
    this.closed = true

    await this.unbindQueues()
    await this.closeChannel()
    await this.releaseConnection()
  }

  private async unbindQueues(): Promise<void> {
    const { name: exchangeName } = this.config.exchange
    const queueName = this.q?.queue

    if (exchangeName && queueName && this.shouldUnbindQueueOnClose()) {
      const routingKeys = this.parseRoutingKeys()
      for (const routingKey of routingKeys) {
        try {
          await this.channel.unbindQueue(queueName, exchangeName, routingKey)
        } catch (e) {
          /* istanbul ignore next */
          this.node.error(`Error unbinding queue for routing key ${routingKey}: ${e.message}`)
        }
      }
    }
  }

  private shouldUnbindQueueOnClose(): boolean {
    const { name, exclusive, autoDelete } = this.config.queue

    // Keep bindings for long-lived queues so reconnects don't temporarily
    // remove routes and drop unroutable messages in-flight.
    return !name || exclusive || autoDelete
  }

  private async closeChannel(): Promise<void> {
    if (this.channel) {
      this.channel.off('error', this.channelErrorHandler)
      this.channel.off('close', this.channelCloseHandler)
      this.channel.off('return', this.channelReturnHandler)
      try {
        await this.channel.close()
      } catch (e) {
        this.node.error(`Error closing AMQP channel: ${e}`)
      }
    }
  }

  private async releaseConnection(): Promise<void> {
    const brokerId = this.config.broker
    const broker = this.broker as unknown as BrokerConfig
    const vhost = this.vhostOverride ?? broker?.vhost
    const key = `${brokerId}:${vhost}`

    this.broker.connections[this.node.id] = false
    this.node.status(NODE_STATUS.Disconnected)

    if (this.connection) {
      this.connection.off('error', this.connectionErrorHandler)
      this.connection.off('close', this.connectionCloseHandler)
    }

    const entry = Amqp.connectionPool.get(key)
    if (entry) {
      entry.count -= 1
      if (entry.count <= 0) {
        Amqp.connectionPool.delete(key)
        try {
          await entry.connection.close()
        } catch (e) {
          /* istanbul ignore next */
          this.node.error(`Error closing AMQP connection: ${e}`)
        }
      }
    }
  }

  private async createChannel(): Promise<Channel> {
    const { prefetch, waitForConfirms } = this.config

    this.channel = await (waitForConfirms
      ? this.connection.createConfirmChannel()
      : this.connection.createChannel())
    this.channel.prefetch(Number(prefetch))

    this.channelErrorHandler = (e): void => {
      /* istanbul ignore next */
      this.node.status(NODE_STATUS.Disconnected)
      this.node.error(`AMQP Connection Error ${e}`, { payload: { error: e, source: 'Amqp' } })
    }

    this.channelCloseHandler = (): void => {
      /* istanbul ignore next */
      this.node.status(NODE_STATUS.Disconnected)
      this.node.log('AMQP Channel closed')
    }

    this.channelReturnHandler = (): void => {
      /* istanbul ignore next */
      this.node.warn('AMQP Message returned')
    }

    this.channel.on('error', this.channelErrorHandler)
    this.channel.on('close', this.channelCloseHandler)
    this.channel.on('return', this.channelReturnHandler)

    return this.channel;
  }

  private async assertExchange(): Promise<void> {
    const { name, type, durable } = this.config.exchange

    /* istanbul ignore else */
    if (name) {
      await this.channel.assertExchange(name, type, {
        durable,
      })
    }
  }

  private async assertQueue(configParams?: AmqpConfig): Promise<string> {
    const { queue } = configParams || this.config
    const { name, exclusive, durable, autoDelete, queueType, queueArguments } = queue

    this.q = await this.channel.assertQueue(name, {
      exclusive,
      durable,
      autoDelete,
      arguments: {
        "x-queue-type": queueType,
        ...(queueArguments || {}),
      },
    })

    return name
  }

  private async bindQueue(configParams?: AmqpConfig): Promise<void> {
    const { name, type, routingKey } =
      configParams?.exchange || this.config.exchange
    const { headers } = configParams?.amqpProperties || this.config

    try {
      if (this.canHaveRoutingKey(type) && name) {
        const promises = this.parseRoutingKeys(routingKey).map(key =>
          this.channel.bindQueue(this.q.queue, name, key),
        )
        await Promise.all(promises)
      }

      if (type === ExchangeType.Fanout) {
        await this.channel.bindQueue(this.q.queue, name, '')
      }

      if (type === ExchangeType.Headers) {
        await this.channel.bindQueue(this.q.queue, name, '', headers)
      }
    } catch (e) {
      this.node.error(`Could not bind queue: ${e}`)
    }
  }

  private canHaveRoutingKey(type: ExchangeType): boolean {
    return type === ExchangeType.Direct || type === ExchangeType.Topic
  }

  private getBrokerUrl(broker: BrokerConfig): string {
    let url = ''

    if (broker) {
      const { host, port, vhost, tls, credsFromSettings, credentials } = broker

      const { username, password } = credsFromSettings
        ? this.getCredsFromSettings()
        : credentials

      const protocol = tls ? /* istanbul ignore next */ 'amqps' : 'amqp'
      const vhostPath = vhost ? `/${encodeURIComponent(vhost)}` : '/'
      url = `${protocol}://${encodeURIComponent(username)}:${encodeURIComponent(
        password,
      )}@${host}:${port}${vhostPath}`
    }

    return url
  }

  private getCredsFromSettings(): {
    username: string
    password: string
  } {
    return {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      username: this.RED.settings.MW_CONTRIB_AMQP_USERNAME,
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      password: this.RED.settings.MW_CONTRIB_AMQP_PASSWORD,
    }
  }

  private parseRoutingKeys(routingKeyArg?: string): string[] {
    const routingKey =
      routingKeyArg || this.config.exchange.routingKey || this.q?.queue || ''
    const keys = routingKey?.split(',').map(key => key.trim())
    return keys
  }

  private assembleMessage(amqpMessage: ConsumeMessage): AssembledMessage {
    const payload = this.parseJson(amqpMessage.content.toString(), true)
    ;(amqpMessage as AssembledMessage).payload = payload
    return amqpMessage as AssembledMessage
  }

  private isManualAck(): boolean {
    return this.node.type === NodeType.AmqpInManualAck
  }

  private parseJson(jsonInput: unknown, logError = false): JsonValue {
    let output: unknown
    try {
      output = JSON.parse(jsonInput as string)
    } catch (e) {
      output = jsonInput
      /* istanbul ignore next */
      if (logError) {
        this.node.error(`Invalid JSON payload: ${e}`)
      }
    }
    return output as JsonValue
  }

  private parseJsonObject(jsonInput: unknown, logError = false): JsonObject {
    const output = this.parseJson(jsonInput, logError)
    return this.isJsonObject(output) ? output : {}
  }

  private isJsonObject(value: JsonValue): value is JsonObject {
    return typeof value === 'object' && value !== null && !Array.isArray(value)
  }
}
