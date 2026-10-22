import type {
  Channel,
  ChannelModel,
  ConfirmChannel,
  ConsumeMessage,
  Options,
  RecoveringChannelModel,
  Replies,
} from 'amqplib'
import cloneDeep from 'lodash.clonedeep'
import type { Node, NodeAPI } from 'node-red'
import { NODE_STATUS, VHOST_READY_TIMEOUT_MS } from './constants'
import SharedConnection, { type ConnectionOwner } from './shared-connection'
import {
  type AmqpBrokerNode,
  type AmqpConfig,
  type AmqpInNodeDefaults,
  type AmqpOutNodeDefaults,
  type AmqpSettings,
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
  private channel: Channel
  private q: Replies.AssertQueue
  private vhostOverride?: string
  private static connectionPool = new Map<string, SharedConnection>()
  private poolEntry?: SharedConnection
  private poolKey?: string
  private channelModel?: ChannelModel
  private ready = false
  private generation = 0
  private setupFailure?: Error
  private settingUp = false
  private readonly recoveryOwner: ConnectionOwner = {
    setup: model => this.restore(model),
    connected: () => {
      if (!this.closed) this.markConnected()
    },
    disconnected: error => this.disconnected(error),
    failed: error => {
      this.disconnected(error)
      this.setBrokerNodeState('errored', error)
      const message = error instanceof Error ? error.message : String(error)
      this.node.status(
        /ACCESS_REFUSED|403/.test(message)
          ? NODE_STATUS.Invalid
          : NODE_STATUS.Error,
      )
      this.node.warn(`AMQP recovery: ${message}`)
    },
  }
  private channelErrorHandler: (e: unknown) => void
  private channelCloseHandler: () => void
  private channelReturnHandler: () => void
  private rpcTimeouts: Set<NodeJS.Timeout> = new Set()
  private vhostWaits = new Set<(error: Error) => void>()
  private closed = false
  private shuttingDown = false

  constructor(
    private readonly RED: NodeAPI<AmqpSettings>,
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
      amqpProperties: this.parseJsonObject(config.amqpProperties),
      headers: this.parseJsonObject(config.headers),
      outputs: config.outputs,
      rpcTimeout: config.rpcTimeoutMilliseconds,
    }
  }

  public async connect(): Promise<RecoveringChannelModel> {
    const { broker } = this.config

    const brokerConfig = this.getBrokerConfig()

    const brokerUrl = this.getBrokerUrl(brokerConfig)
    const { host, port, vhost } = brokerConfig

    const brokerInfo = `${host}:${port}${vhost ? `/${vhost}` : ''}`
    const key = `${broker}:${vhost}`

    if (this.closed) return this.connection
    if (this.poolEntry) return this.poolEntry.connect()
    this.setBrokerNodeState('disconnected')
    let entry = Amqp.connectionPool.get(key)
    if (!entry) {
      this.node.log(`Connecting to AMQP broker ${brokerInfo}`)
      entry = new SharedConnection(brokerUrl)
      Amqp.connectionPool.set(key, entry)
    }
    this.poolEntry = entry
    this.poolKey = key
    const generation = this.generation
    const isCurrentAttempt = () =>
      !this.closed && generation === this.generation
    try {
      const connection = await entry.add(this.recoveryOwner)
      if (isCurrentAttempt()) this.connection = connection
      return connection
    } catch (error) {
      if (this.poolEntry === entry) {
        await this.releaseConnection().catch(() => undefined)
      }
      if (isCurrentAttempt()) {
        this.setBrokerNodeState('errored', error)
      }
      throw error
    }
  }

  private getBrokerConfig(): BrokerConfig {
    const broker = this.RED.nodes.getNode(this.config.broker)

    if (!this.isBrokerNode(broker)) {
      const err = new Error(`AMQP broker node not found: ${this.config.broker}`)
      this.node.error(err.message)
      throw err
    }

    this.broker = broker
    return { ...broker, vhost: this.vhostOverride ?? broker.vhost }
  }

  private isBrokerNode(node: Node): node is AmqpBrokerNode {
    return node?.type === 'amqp-broker'
  }

  private async restore(model: ChannelModel): Promise<void> {
    if (this.closed) return
    const generation = this.generation
    const cancelled = () => this.closed || generation !== this.generation
    const ownsSetup = () =>
      generation === this.generation && this.channelModel === model
    this.ready = false
    this.setupFailure = undefined
    this.clearRpcTimeouts()
    await this.closeChannel()
    if (cancelled()) return
    this.channelModel = model
    this.settingUp = true
    try {
      await this.initialize()
      if (cancelled()) return
      if (this.node.type !== NodeType.AmqpOut) await this.consume()
      if (this.setupFailure) throw this.setupFailure
    } catch (error) {
      if (ownsSetup()) {
        await this.closeChannel()
      }
      throw error
    } finally {
      if (ownsSetup()) {
        this.settingUp = false
      }
    }
  }

  private disconnected(error: unknown): void {
    if (this.closed) return
    this.ready = false
    if (this.rpcTimeouts.size) {
      this.node.error(
        'AMQP connection lost before RPC replies; pending requests cancelled',
      )
    }
    this.clearRpcTimeouts()
    this.setBrokerNodeState('disconnected', error)
    this.node.status(NODE_STATUS.Disconnected)
  }

  public async reconnect(): Promise<void> {
    if (!this.closed) await this.poolEntry?.restart()
  }

  public markConnected(): void {
    this.ready = true
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
      const channel = this.channel
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
          if (this.closed || this.channel !== channel) return
          if (!amqpMessage) {
            this.node.warn('AMQP consumer was cancelled')
            this.setupFailure = new Error('AMQP consumer was cancelled')
            this.disconnected(this.setupFailure)
            if (!this.settingUp) {
              void this.reconnect().catch(error =>
                this.node.error(String(error)),
              )
            }
            return
          }
          const msg = this.assembleMessage(amqpMessage)
          this.node.log(
            `Received message with deliveryTag: ${msg?.fields?.deliveryTag}`,
          )
          this.node.send(msg)
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

  public async setVhost(newVhost: string): Promise<void> {
    const broker = this.broker
    const currentVhost = this.vhostOverride ?? broker?.vhost

    if (!broker || currentVhost === newVhost) {
      return
    }

    try {
      await this.close(false)
      if (this.shuttingDown)
        throw new Error('AMQP vhost switch cancelled during shutdown')
      this.vhostOverride = newVhost
      this.closed = false
      await this.waitForVhostReady()
    } catch (e) {
      this.node.error(`Could not switch vhost: ${e}`)
      throw e
    }
  }

  private async waitForVhostReady(): Promise<void> {
    let timeout: NodeJS.Timeout
    let cancel: (error: Error) => void
    const deadline = new Promise<never>((_, reject) => {
      cancel = reject
      this.vhostWaits.add(cancel)
      timeout = setTimeout(() => {
        reject(
          new Error(
            `Timed out waiting for AMQP vhost readiness after ${VHOST_READY_TIMEOUT_MS}ms`,
          ),
        )
      }, VHOST_READY_TIMEOUT_MS)
    })
    try {
      await Promise.race([
        this.connect().then(connection => connection.waitForConnect()),
        deadline,
      ])
    } finally {
      clearTimeout(timeout)
      this.vhostWaits.delete(cancel)
    }
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
    properties?: Options.Publish,
    routingKey?: string,
  ): Promise<void> {
    if (this.closed || !this.ready) {
      throw new Error('AMQP channel is not ready; message was not published')
    }
    const routingKeys = this.parseRoutingKeys(routingKey)
    await Promise.all(
      routingKeys.map(routingKey =>
        this.handlePublish(this.config, msg, properties, routingKey),
      ),
    )
  }

  private async handlePublish(
    config: AmqpConfig,
    msg: unknown,
    properties?: Options.Publish,
    routingKey?: string,
  ) {
    const {
      exchange: { name },
      outputs: rpcRequested,
    } = config

    let cancelRpcConsumer: (() => Promise<void>) | null = null
    const channel = this.channel

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
      channel.publish(name, routingKey, this.toPublishBuffer(msg), options)

      if (config.waitForConfirms) {
        await (channel as ConfirmChannel).waitForConfirms()
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
    const channel = this.channel
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
              await channel.deleteQueue(queueName)
              rpcQueueHasBeenDeleted = true
            } catch (deleteError) {
              this.node.error(
                `Error trying to cancel RPC consumer: ${deleteError}`,
              )

              const canCancelConsumer = typeof channel.cancel === 'function'
              if (canCancelConsumer && rpcConsumerTag) {
                try {
                  await channel.cancel(rpcConsumerTag)
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

      const consumeResponse = await channel.consume(
        queueName,
        amqpMessage => {
          if (amqpMessage && !this.closed && this.channel === channel) {
            const msg = this.assembleMessage(amqpMessage)
            if (msg.properties.correlationId === correlationId) {
              if (finalizeRpcResponse()) {
                this.node.send(msg)
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
        await channel.deleteQueue(queueName).catch(deleteError => {
          this.node.error(`Error trying to cancel RPC consumer: ${deleteError}`)
        })
      }
      this.node.error(`Could not consume RPC message: ${e}`)
      throw e
    }
  }

  public async close(permanent = true): Promise<void> {
    if (permanent) this.shuttingDown = true
    for (const cancel of this.vhostWaits) {
      cancel(new Error('AMQP vhost readiness wait cancelled'))
    }
    if (this.closed) {
      return
    }
    this.closed = true
    this.ready = false
    this.generation += 1
    this.clearRpcTimeouts()

    // Detach first so a node closing during setup cannot be restored again.
    await this.releaseConnection()
  }

  private shouldAutoCreateExchangeBindings(configParams?: AmqpConfig): boolean {
    return (configParams || this.config).exchange.autoCreate
  }

  private shouldAutoCreateQueue(configParams?: AmqpConfig): boolean {
    return (configParams || this.config).queue.autoCreate
  }

  private shouldUnbindQueueOnClose(): boolean {
    const { name, exclusive, autoDelete } = this.config.queue
    return (
      this.shouldAutoCreateExchangeBindings() &&
      (!name || exclusive || autoDelete)
    )
  }

  private async unbindQueues(channel: Channel): Promise<void> {
    const { name, type } = this.config.exchange
    const queue = this.q?.queue
    if (!name || !queue || !this.shouldUnbindQueueOnClose()) return
    const keys = this.canHaveRoutingKey(type) ? this.parseRoutingKeys() : ['']
    const args = type === ExchangeType.Headers ? this.config.headers : undefined
    for (const key of keys) {
      try {
        await channel.unbindQueue(queue, name, key, args)
      } catch (error) {
        this.node.error(
          `Error unbinding queue for routing key ${key}: ${error}`,
        )
      }
    }
  }

  private async closeChannel(cleanupBindings = false): Promise<void> {
    if (this.channel) {
      const channel = this.channel
      const { channelErrorHandler, channelCloseHandler, channelReturnHandler } =
        this
      this.channel = undefined
      try {
        if (cleanupBindings) await this.unbindQueues(channel)
        await channel.close()
      } catch (e) {
        this.node.error(`Error closing AMQP channel: ${e}`)
      } finally {
        channel.off?.('error', channelErrorHandler)
        channel.off?.('close', channelCloseHandler)
        channel.off?.('return', channelReturnHandler)
      }
    }
  }

  private async releaseConnection(): Promise<void> {
    const entry = this.poolEntry
    const key = this.poolKey
    this.poolEntry = undefined
    this.poolKey = undefined
    this.setBrokerNodeState('disconnected')
    this.node.status(NODE_STATUS.Disconnected)
    if (!entry) {
      await this.closeChannel(true)
      return
    }
    const release = entry.remove(this.recoveryOwner, () =>
      this.closeChannel(true),
    )
    if (!entry.size && Amqp.connectionPool.get(key) === entry) {
      Amqp.connectionPool.delete(key)
    }
    await release
  }

  private async createChannel(): Promise<Channel> {
    const { prefetch, waitForConfirms } = this.config

    const model = this.channelModel || this.connection
    const generation = this.generation
    const channel = await (waitForConfirms
      ? model.createConfirmChannel()
      : model.createChannel())
    if (this.closed || generation !== this.generation) {
      await channel.close()
      throw new Error('AMQP channel setup cancelled')
    }
    this.channel = channel

    this.channelErrorHandler = (e): void => {
      if (this.closed || this.channel !== channel) return
      this.setupFailure = e instanceof Error ? e : new Error(String(e))
      this.disconnected(e)
      this.setBrokerNodeState('errored', e)
      this.node.error(`AMQP Connection Error ${e}`, {
        payload: { error: e, source: 'Amqp' },
      })
    }

    this.channelCloseHandler = (): void => {
      if (this.closed || this.channel !== channel) return
      this.channel = undefined
      this.setupFailure = new Error('AMQP channel closed')
      this.disconnected(this.setupFailure)
      if (!this.settingUp) {
        void this.reconnect().catch(error => this.node.error(String(error)))
      }
    }

    this.channelReturnHandler = (): void => {
      this.node.warn('AMQP Message returned')
    }

    this.channel.on('error', this.channelErrorHandler)
    this.channel.on('close', this.channelCloseHandler)
    this.channel.on('return', this.channelReturnHandler)
    await this.channel.prefetch(Number(prefetch))

    return this.channel
  }

  private async assertExchange(): Promise<void> {
    const { name, type, durable } = this.config.exchange
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

      const protocol = tls ? 'amqps' : 'amqp'
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
      username: this.RED.settings.MW_CONTRIB_AMQP_USERNAME,
      password: this.RED.settings.MW_CONTRIB_AMQP_PASSWORD,
    }
  }

  private parseRoutingKeys(routingKeyArg?: string): string[] {
    const routingKey =
      (routingKeyArg ?? this.config.exchange.routingKey) || this.q?.queue || ''
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
