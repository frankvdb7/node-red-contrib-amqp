import {
  type Channel,
  type ChannelModel,
  type ConfirmChannel,
  type ConsumeMessage,
  connect,
  type MessageProperties,
  type RecoveringChannelModel,
  type Replies,
} from 'amqplib'
import cloneDeep from 'lodash.clonedeep'
import type { Node, NodeMessage, NodeRedApp } from 'node-red'
import { NODE_STATUS } from './constants'
import {
  type AmqpBrokerNode,
  type AmqpConfig,
  type AmqpInNodeDefaults,
  type AmqpOutNodeDefaults,
  type AssembledMessage,
  type BrokerConfig,
  type BrokerNodeError,
  type BrokerNodeState,
  ExchangeType,
  type JsonObject,
  type JsonValue,
  NodeType,
} from './types'

export default class Amqp {
  private config: AmqpConfig
  private broker: AmqpBrokerNode
  private connection: RecoveringChannelModel
  private model: ChannelModel
  private channel: Channel
  private q: Replies.AssertQueue
  private vhostOverride?: string
  private connectionErrorHandler: (e: unknown) => void
  private connectionCloseHandler: () => void
  private channelErrorHandler: (e: unknown) => void
  private channelCloseHandler: () => void
  private channelReturnHandler: () => void
  private rpcTimeouts: Set<NodeJS.Timeout> = new Set()
  private closed = false
  private recoveryHandler?: () => Promise<void>
  private initialRecovery = true

  constructor(
    private readonly RED: NodeRedApp,
    private readonly node: Node,
    config: AmqpInNodeDefaults & AmqpOutNodeDefaults,
  ) {
    this.config = {
      name: config.name,
      broker: config.broker,
      prefetch: config.prefetch,
      noAck: config.noAck,
      waitForConfirms: config.waitForConfirms,
      exchange: {
        name: config.exchangeName,
        type: config.exchangeType,
        routingKey: config.exchangeRoutingKey,
        durable: config.exchangeDurable,
        autoCreate: config.autoCreateExchangeBindings ?? false,
      },
      queue: {
        name: config.queueName,
        exclusive: config.queueExclusive,
        durable: config.queueDurable,
        autoDelete: config.queueAutoDelete,
        autoCreate: config.autoCreateQueue ?? false,
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

  public async connect(): Promise<RecoveringChannelModel> {
    const { broker } = this.config

    // wtf happened to the types?
    // @ts-expect-error
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
    this.initialRecovery = true
    this.node.log(`Connecting to AMQP broker ${brokerInfo}`)
    try {
      this.connection = await connect(`${brokerUrl}?heartbeat=2`, {
        recovery: {
          setup: async model => this.handleRecovery(model),
        },
      })
      this.node.log(`Connected to AMQP broker ${brokerInfo}`)
    } catch (err) {
      this.setBrokerNodeState('errored', err)
      this.node.warn(`Failed to connect to AMQP broker ${brokerInfo}: ${err}`)
      throw err
    }

    this.connectionErrorHandler = (e): void => {
      /* istanbul ignore next */
      this.setBrokerNodeState('errored', e)
      this.node.status(NODE_STATUS.Disconnected)
      this.node.warn(`AMQP connection error ${e}`)
    }

    this.connectionCloseHandler = (): void => {
      /* istanbul ignore next */
      this.setBrokerNodeState(
        'disconnected',
        new Error('AMQP connection closed'),
      )
      this.node.status(NODE_STATUS.Disconnected)
      this.node.log(`AMQP Connection closed`)
    }

    this.connection.on('error', this.connectionErrorHandler)
    this.connection.on('disconnect', this.connectionCloseHandler)

    this.closed = false

    return this.connection
  }

  public onRecovery(handler: () => Promise<void>): void {
    this.recoveryHandler = handler
  }

  public markConnected(): void {
    this.setBrokerNodeState('connected')
    this.node.status(NODE_STATUS.Connected)
  }

  public removeBrokerNodeState(): void {
    if (!this.broker || !this.node?.id) {
      return
    }

    if (this.broker.nodeStates) {
      delete this.broker.nodeStates[this.node.id]
    }

    if (this.broker.lastError) {
      delete this.broker.lastError[this.node.id]
    }
  }

  public async initialize(): Promise<Channel> {
    await this.createChannel()
    if (this.shouldAutoCreateExchangeBindings()) {
      await this.assertExchange()
    }
    return this.channel
  }

  public async consume(): Promise<void> {
    try {
      const { noAck } = this.config
      if (this.shouldAutoCreateQueue()) {
        await this.assertQueue()
      } else {
        this.useExistingQueue()
      }

      if (this.shouldAutoCreateExchangeBindings()) {
        await this.bindQueue()
      }
      await this.channel.consume(
        this.q.queue,
        amqpMessage => {
          if (!amqpMessage) {
            this.node.warn('AMQP consumer was cancelled')
            this.node.status(NODE_STATUS.Disconnected)
            const eventEmitterNode = this.node as unknown as {
              emit?: (event: string) => void
            }
            eventEmitterNode.emit?.('amqp:consumer-cancelled')
            return
          }
          const msg = this.assembleMessage(amqpMessage)
          this.node.log(
            `Received message with deliveryTag: ${msg?.fields?.deliveryTag}`,
          )
          this.node.send(msg as unknown as NodeMessage)
          /* istanbul ignore else */
          if (!noAck && !this.isManualAck()) {
            this.ack(msg)
          }
        },
        { noAck },
      )
    } catch (e) {
      this.node.error(`Could not consume message: ${e}`)
      throw e
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
      this.markConnected()
    } catch (e) {
      this.node.error(`Could not switch vhost: ${e}`)
      throw e
    }
  }

  public getConnection(): ChannelModel {
    return this.connection as ChannelModel
  }

  public getChannel(): Channel {
    return this.channel
  }

  public ack(msg: AssembledMessage): void {
    const allUpTo = !!msg.manualAck?.allUpTo
    try {
      this.node.log(
        `Acking message with deliveryTag: ${msg?.fields?.deliveryTag}`,
      )
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

    let cancelRpcConsumer: (() => Promise<void>) | null = null

    try {
      let correlationId = ''
      let replyTo = ''

      if (rpcRequested) {
        // Send request for remote procedure call
        const uuidv4 = (await import('uuid')).v4
        correlationId =
          properties?.correlationId ||
          this.config.amqpProperties?.correlationId ||
          uuidv4()
        replyTo =
          properties?.replyTo || this.config.amqpProperties?.replyTo || uuidv4()
        cancelRpcConsumer = await this.handleRemoteProcedureCall(
          correlationId,
          replyTo,
        )
      }

      const options = {
        ...this.config.amqpProperties,
        ...properties,
        correlationId,
        replyTo,
      }
      // when the name field is empty, publish just like the sendToQueue method;
      // see https://amqp-node.github.io/amqplib/channel_api.html#channel_publish
      this.channel.publish(name, routingKey, this.toPublishBuffer(msg), options)

      if (config.waitForConfirms) {
        await (this.channel as ConfirmChannel).waitForConfirms()
      }
    } catch (e) {
      if (cancelRpcConsumer) {
        await cancelRpcConsumer()
      }
      this.node.error(`Could not publish message: ${e}`)
      throw e
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
  ): Promise<() => Promise<void>> {
    const rpcConfig = this.getRpcConfig(replyTo)
    let queueName = ''

    try {
      // If we try to delete a queue that's already deleted
      // bad things will happen.
      let rpcQueueHasBeenDeleted = false
      let rpcResponseFinalized = false
      let additionalErrorMessaging = ''
      let rpcTimeout: NodeJS.Timeout | null = null
      let rpcConsumerTag = ''
      let cleanupPromise: Promise<void> | null = null

      const clearRpcTimeout = (): void => {
        if (rpcTimeout) {
          clearTimeout(rpcTimeout)
          this.rpcTimeouts.delete(rpcTimeout)
          rpcTimeout = null
        }
      }

      const finalizeRpcResponse = (): boolean => {
        if (rpcResponseFinalized) {
          return false
        }
        rpcResponseFinalized = true
        clearRpcTimeout()
        return true
      }

      const cleanupRpcResources = async (): Promise<void> => {
        if (rpcQueueHasBeenDeleted || !queueName) {
          return
        }

        if (!cleanupPromise) {
          cleanupPromise = (async () => {
            try {
              await this.channel.deleteQueue(queueName)
              rpcQueueHasBeenDeleted = true
            } catch (deleteError) {
              this.node.error(
                `Error trying to cancel RPC consumer: ${deleteError}`,
              )

              const canCancelConsumer =
                typeof this.channel.cancel === 'function'
              if (canCancelConsumer && rpcConsumerTag) {
                try {
                  await this.channel.cancel(rpcConsumerTag)
                  rpcQueueHasBeenDeleted = true
                } catch (cancelError) {
                  this.node.error(
                    `Error trying to cancel RPC consumer: ${cancelError}`,
                  )
                }
              }
            } finally {
              cleanupPromise = null
            }
          })()
        }

        await cleanupPromise
      }

      const cancelRpcConsumer = async (): Promise<void> => {
        finalizeRpcResponse()
        await cleanupRpcResources()
      }

      /************************************
       * assert queue and set up consumer
       ************************************/
      queueName = await this.assertQueue(rpcConfig)

      const consumeResponse = await this.channel.consume(
        queueName,
        amqpMessage => {
          if (amqpMessage) {
            const msg = this.assembleMessage(amqpMessage)
            if (msg.properties.correlationId === correlationId) {
              if (finalizeRpcResponse()) {
                this.node.send(msg as unknown as NodeMessage)
                void cleanupRpcResources()
              }
            } else {
              additionalErrorMessaging += ` Correlation ids do not match. Expecting: ${correlationId}, received: ${msg.properties.correlationId}`
            }
          }
        },
        { noAck: rpcConfig.noAck },
      )
      rpcConsumerTag = consumeResponse?.consumerTag || ''

      /****************************************
       * Check if RPC has timed out and handle
       ****************************************/
      rpcTimeout = setTimeout(async () => {
        clearRpcTimeout()
        if (this.closed) {
          return
        }

        try {
          if (finalizeRpcResponse()) {
            this.node.send({
              payload: {
                message: `Timeout while waiting for RPC response.${additionalErrorMessaging}`,
                config: rpcConfig,
              },
            })
          }
          await cleanupRpcResources()
        } catch (e) {
          // TODO: Keep an eye on this
          // This might close the whole channel
          this.node.error(`Error trying to cancel RPC consumer: ${e}`)
        }
      }, rpcConfig.rpcTimeout || 3000)
      this.rpcTimeouts.add(rpcTimeout)
      return cancelRpcConsumer
    } catch (e) {
      // If setup failed after queue assertion, try to clean up the temporary RPC queue.
      if (queueName) {
        await this.channel.deleteQueue(queueName).catch(deleteError => {
          this.node.error(`Error trying to cancel RPC consumer: ${deleteError}`)
        })
      }
      this.node.error(`Could not consume RPC message: ${e}`)
      throw e
    }
  }

  public async close(): Promise<void> {
    if (this.closed) {
      return
    }
    this.closed = true
    this.clearRpcTimeouts()

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
          this.node.error(
            `Error unbinding queue for routing key ${routingKey}: ${e.message}`,
          )
        }
      }
    }
  }

  private shouldUnbindQueueOnClose(): boolean {
    const { name, exclusive, autoDelete } = this.config.queue

    // Keep bindings for long-lived queues so reconnects don't temporarily
    // remove routes and drop unroutable messages in-flight.
    return (
      this.shouldAutoCreateExchangeBindings() &&
      (!name || exclusive || autoDelete)
    )
  }

  private shouldAutoCreateExchangeBindings(configParams?: AmqpConfig): boolean {
    return (configParams || this.config).exchange.autoCreate
  }

  private shouldAutoCreateQueue(configParams?: AmqpConfig): boolean {
    return (configParams || this.config).queue.autoCreate
  }

  private async closeChannel(): Promise<void> {
    if (this.channel) {
      this.channel.off?.('error', this.channelErrorHandler)
      this.channel.off?.('close', this.channelCloseHandler)
      this.channel.off?.('return', this.channelReturnHandler)
      try {
        await this.channel.close()
      } catch (e) {
        this.node.error(`Error closing AMQP channel: ${e}`)
      }
    }
  }

  private async releaseConnection(): Promise<void> {
    this.setBrokerNodeState('disconnected')
    this.node.status(NODE_STATUS.Disconnected)

    if (this.connection) {
      this.connection.off?.('error', this.connectionErrorHandler)
      this.connection.off?.('disconnect', this.connectionCloseHandler)
    }

    try {
      await this.connection?.close()
    } catch (e) {
      /* istanbul ignore next */
      this.node.error(`Error closing AMQP connection: ${e}`)
    }
  }

  private async handleRecovery(model: ChannelModel): Promise<void> {
    this.model = model
    this.closed = false
    if (this.initialRecovery) {
      this.initialRecovery = false
      return
    }
    await this.recoveryHandler?.()
  }

  private async createChannel(): Promise<Channel> {
    const { prefetch, waitForConfirms } = this.config

    const model = this.model ?? (this.connection as unknown as ChannelModel)
    this.channel = await (waitForConfirms
      ? model.createConfirmChannel()
      : model.createChannel())
    this.channel.prefetch(Number(prefetch))

    this.channelErrorHandler = (e): void => {
      /* istanbul ignore next */
      this.setBrokerNodeState('errored', e)
      this.node.status(NODE_STATUS.Disconnected)
      this.node.error(`AMQP Connection Error ${e}`, {
        payload: { error: e, source: 'Amqp' },
      })
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

    return this.channel
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
    const { name, exclusive, durable, autoDelete, queueType, queueArguments } =
      queue

    this.q = await this.channel.assertQueue(name, {
      exclusive,
      durable,
      autoDelete,
      arguments: {
        'x-queue-type': queueType,
        ...(queueArguments || {}),
      },
    })

    return name
  }

  private useExistingQueue(configParams?: AmqpConfig): string {
    const { queue } = configParams || this.config
    const { name } = queue

    if (!name) {
      throw new Error(
        'Queue Name is required when "Auto-create" queue is disabled',
      )
    }

    this.q = { queue: name } as Replies.AssertQueue

    return name
  }

  private async bindQueue(configParams?: AmqpConfig): Promise<void> {
    const { name, type, routingKey } =
      configParams?.exchange || this.config.exchange
    const { headers } = configParams?.amqpProperties || this.config

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
      // @ts-expect-error
      username: this.RED.settings.MW_CONTRIB_AMQP_USERNAME,
      // @ts-expect-error
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

  private clearRpcTimeouts(): void {
    for (const timeout of this.rpcTimeouts) {
      clearTimeout(timeout)
    }
    this.rpcTimeouts.clear()
  }

  private setBrokerNodeState(state: BrokerNodeState, error?: unknown): void {
    if (!this.broker) {
      return
    }

    if (!this.broker.nodeStates) {
      this.broker.nodeStates = {}
    }
    this.broker.nodeStates[this.node.id] = state

    if (!this.broker.lastError) {
      this.broker.lastError = {}
    }

    if (error !== undefined) {
      this.broker.lastError[this.node.id] = this.toBrokerNodeError(error)
    } else if (state === 'connected') {
      delete this.broker.lastError[this.node.id]
    }
  }

  private toBrokerNodeError(error: unknown): BrokerNodeError {
    const message = error instanceof Error ? error.message : String(error)
    const code =
      error && typeof error === 'object'
        ? String((error as { code?: unknown }).code || '')
        : ''
    return {
      message,
      code: code || undefined,
      at: new Date().toISOString(),
    }
  }

  private toPublishBuffer(msg: unknown): Buffer {
    if (Buffer.isBuffer(msg)) {
      return msg
    }
    if (msg instanceof Uint8Array) {
      return Buffer.from(msg)
    }
    if (typeof msg === 'string') {
      return Buffer.from(msg)
    }
    if (msg === undefined) {
      return Buffer.from('')
    }
    try {
      const serialized = JSON.stringify(msg)
      if (serialized === undefined) {
        return Buffer.from('')
      }
      return Buffer.from(serialized)
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error)
      throw new Error(`Could not serialize payload: ${reason}`)
    }
  }
}
