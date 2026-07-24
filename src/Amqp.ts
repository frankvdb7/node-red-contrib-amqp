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
  Options,
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
  BrokerNodeState,
  BrokerNodeError,
} from './types'
import { NODE_STATUS } from './constants'

const DELIVERY_TOKEN: unique symbol = Symbol('amqp-delivery-token')
const RETURN_TOKEN_HEADER = 'x-node-red-contrib-amqp-return-token'
const MAX_UNMATCHED_RPC_RESPONSES = 100
const BINDING_CLEANUP_TIMEOUT_MS = 5_000
const BINDING_CLEANUP_MAX_ATTEMPTS = 3

type TrackedAssembledMessage = AssembledMessage & {
  [DELIVERY_TOKEN]?: string
}

interface TrackedDelivery {
  message: AssembledMessage
  channel: Channel
  deliveryTag: number
}

interface PendingRpcRequest {
  owner: Amqp
  handleResponse: (message: AssembledMessage) => void
  interrupt: (error: Error, cleanup: boolean) => void
  cancel: () => void
}

interface RpcRequestLifecycle {
  cancel: () => Promise<void>
  startTimeout: () => void
}

interface RpcConsumerState {
  key: string
  channel: Channel
  queueName: string
  consumerTag: string
  pending: Map<string, PendingRpcRequest>
  unmatchedResponseCount: number
  unmatchedResponseOverflow: boolean
  ready: Promise<void>
  cleanupPromise: Promise<void> | null
  cleanedUp: boolean
  registry: Map<string, RpcConsumerState>
  users: Set<Amqp>
}

interface AmqpCloseOptions {
  interruption?: Error
  removeBindings?: boolean
}

interface AmqpConnectOptions {
  timeout?: number
}

interface PendingConnection {
  promise: Promise<ChannelModel>
  activeWaiters: number
  accepted: boolean
}

interface ChannelDrainGate {
  channel: Channel
  promise: Promise<void>
  reject: (error: Error) => void
}

interface ChannelWriteQueue {
  channel: Channel
  tail: Promise<void>
}

export default class Amqp {
  private config: AmqpConfig
  private broker: AmqpBrokerNode
  private connection: ChannelModel
  private channel: Channel
  private q: Replies.AssertQueue
  private vhostOverride?: string
  private connectionPoolKey?: string
  private static connectionPool: Map<string, { connection: ChannelModel; count: number }> = new Map()
  private static pendingConnections: Map<string, PendingConnection> = new Map()
  private static endpointGenerations: Map<
    string,
    { brokerUrl: string; generation: number }
  > = new Map()
  private static rpcConsumerRegistries: WeakMap<
    object,
    Map<string, RpcConsumerState>
  > = new WeakMap()
  private connectionErrorHandler: (e: unknown) => void
  private connectionCloseHandler: () => void
  private channelErrorHandler: (e: unknown) => void
  private channelCloseHandler: () => void
  private channelReturnHandler: (message: ConsumeMessage) => void
  private rpcTimeouts: Set<NodeJS.Timeout> = new Set()
  private rpcInterruptHandlers: Set<(error: Error) => void> = new Set()
  private rpcConsumers: Map<string, RpcConsumerState> = new Map()
  private returnedPublishes: Map<string, Error> = new Map()
  private trackedDeliveries: Map<string, TrackedDelivery> = new Map()
  private closed = false
  private closePromise: Promise<void> | null = null
  private bindingCleanupPromise: Promise<void> | null = null
  private bindingsRemoved = false
  private channelDrainGate: ChannelDrainGate | null = null
  private channelWriteQueue: ChannelWriteQueue | null = null
  private channelWriteFailure: { channel: Channel; error: Error } | null = null
  private lifecycleVersion = 0

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
      ) as unknown as Options.Publish,
      headers: this.parseJsonObject(config.headers),
      outputs: config.outputs,
      rpcTimeout: config.rpcTimeoutMilliseconds,
    }
  }

  public async connect(
    options: AmqpConnectOptions = {},
  ): Promise<ChannelModel> {
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
    const logicalKey = `${broker}:${vhost}`
    const key = this.getEndpointGenerationKey(logicalKey, brokerUrl)

    let entry = Amqp.connectionPool.get(key)
    if (entry && !this.isConnectionOpen(entry.connection)) {
      Amqp.connectionPool.delete(key)
      entry = undefined
    }

    if (!entry) {
      let pendingConnection = Amqp.pendingConnections.get(key)
      if (!pendingConnection) {
        this.node.log(`Connecting to AMQP broker ${brokerInfo}`)
        pendingConnection = {
          promise: this.createPooledConnection(brokerUrl, brokerInfo),
          activeWaiters: 0,
          accepted: false,
        }
        Amqp.pendingConnections.set(key, pendingConnection)
        void pendingConnection.promise.then(
          async connection => {
            if (
              !pendingConnection.accepted &&
              pendingConnection.activeWaiters === 0
            ) {
              if (Amqp.pendingConnections.get(key) === pendingConnection) {
                Amqp.pendingConnections.delete(key)
              }
              try {
                await connection.close()
              } catch (error) {
                this.node.error(`Error closing unowned AMQP connection: ${error}`)
              }
              return
            }
            if (Amqp.pendingConnections.get(key) === pendingConnection) {
              Amqp.pendingConnections.delete(key)
            }
          },
          () => {
            if (Amqp.pendingConnections.get(key) === pendingConnection) {
              Amqp.pendingConnections.delete(key)
            }
          },
        )
      }

      pendingConnection.activeWaiters += 1
      try {
        const connection = await this.awaitConnection(
          pendingConnection.promise,
          brokerInfo,
          options.timeout,
        )
        entry = Amqp.connectionPool.get(key)
        if (!entry) {
          pendingConnection.accepted = true
          entry = { connection, count: 0 }
          Amqp.connectionPool.set(key, entry)
          connection.on('close', () => {
            const current = Amqp.connectionPool.get(key)
            if (current?.connection === connection) {
              Amqp.connectionPool.delete(key)
            }
          })
        } else if (entry.connection !== connection) {
          throw new Error(`AMQP connection closed while connecting to ${brokerInfo}`)
        }
      } catch (err) {
        this.setBrokerNodeState('errored', err)
        this.node.warn(`Failed to connect to AMQP broker ${brokerInfo}: ${err}`)
        throw err
      } finally {
        pendingConnection.activeWaiters -= 1
        if (
          pendingConnection.activeWaiters === 0 &&
          !pendingConnection.accepted &&
          Amqp.pendingConnections.get(key) === pendingConnection
        ) {
          Amqp.pendingConnections.delete(key)
        }
      }
    }

    entry.count += 1
    this.connection = entry.connection
    this.connectionPoolKey = key

    this.connectionErrorHandler = (e): void => {
      /* istanbul ignore next */
      this.setBrokerNodeState('errored', e)
      this.node.status(NODE_STATUS.Disconnected)
      this.node.warn(`AMQP connection error ${e}`)
    }

    this.connectionCloseHandler = (): void => {
      /* istanbul ignore next */
      this.setBrokerNodeState('disconnected', new Error('AMQP connection closed'))
      this.node.status(NODE_STATUS.Disconnected)
      this.node.log(`AMQP Connection closed`)
    }

    this.connection.on('error', this.connectionErrorHandler)
    this.connection.on('close', this.connectionCloseHandler)

    this.closed = false
    this.bindingsRemoved = false

    return this.connection
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
    return this.channel;
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
            const eventEmitterNode = this.node as unknown as { emit?: (event: string) => void }
            eventEmitterNode.emit?.('amqp:consumer-cancelled')
            return
          }
          const msg = this.assembleMessage(amqpMessage)
          this.node.log(
            `Received message with deliveryTag: ${msg?.fields?.deliveryTag}`,
          )
          this.node.send(msg as any)
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
      await this.close(new Error('AMQP virtual host changed during RPC'))
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
    return this.connection
  }

  public getChannel(): Channel {
    return this.channel
  }

  public ack(msg: AssembledMessage): boolean {
    const allUpTo = !!msg.manualAck?.allUpTo
    const delivery = this.resolveDelivery(msg, 'ack')
    if (!delivery) {
      return false
    }
    try {
      this.node.log(
        `Acking message with deliveryTag: ${delivery.message.fields?.deliveryTag}`,
      )
      this.channel.ack(delivery.message, allUpTo)
      this.removeTrackedDeliveries(delivery, allUpTo)
      return true
    } catch (e) {
      this.node.error(`Could not ack message: ${e}`)
      return false
    }
  }

  public ackAll(msg?: AssembledMessage): boolean {
    if (this.isManualAck() && (!msg || !this.resolveDelivery(msg, 'ackAll'))) {
      return false
    }
    try {
      this.node.log('Acking all outstanding messages')
      this.channel.ackAll()
      this.trackedDeliveries.clear()
      return true
    } catch (e) {
      this.node.error(`Could not ackAll messages: ${e}`)
      return false
    }
  }

  public nack(msg: AssembledMessage): boolean {
    const allUpTo = !!msg.manualAck?.allUpTo
    const requeue = msg.manualAck?.requeue ?? true
    const delivery = this.resolveDelivery(msg, 'nack')
    if (!delivery) {
      return false
    }
    try {
      this.node.log(
        `Nacking message with deliveryTag: ${delivery.message.fields?.deliveryTag}`,
      )
      this.channel.nack(delivery.message, allUpTo, requeue)
      this.removeTrackedDeliveries(delivery, allUpTo)
      return true
    } catch (e) {
      this.node.error(`Could not nack message: ${e}`)
      return false
    }
  }

  public nackAll(msg: AssembledMessage): boolean {
    const requeue = msg.manualAck?.requeue ?? true
    if (!this.resolveDelivery(msg, 'nackAll')) {
      return false
    }
    try {
      this.node.log('Nacking all outstanding messages')
      this.channel.nackAll(requeue)
      this.trackedDeliveries.clear()
      return true
    } catch (e) {
      this.node.error(`Could not nackAll messages: ${e}`)
      return false
    }
  }

  public reject(msg: AssembledMessage): boolean {
    const requeue = msg.manualAck?.requeue ?? true
    const delivery = this.resolveDelivery(msg, 'reject')
    if (!delivery) {
      return false
    }
    try {
      this.node.log(
        `Rejecting message with deliveryTag: ${delivery.message.fields?.deliveryTag}`,
      )
      this.channel.reject(delivery.message, requeue)
      this.removeTrackedDeliveries(delivery, false)
      return true
    } catch (e) {
      this.node.error(`Could not reject message: ${e}`)
      return false
    }
  }

  public async publish(
    msg: unknown,
    properties?: Options.Publish,
    routingKey?: string,
  ): Promise<void> {
    const routingKeys = this.parseRoutingKeys(routingKey)
    const results = await Promise.allSettled(
      routingKeys.map(routingKey =>
        this.handlePublish(this.config, msg, properties, routingKey),
      ),
    )
    const failedRoutingKeys = routingKeys.filter(
      (_, index) => results[index].status === 'rejected',
    )
    if (failedRoutingKeys.length === 0) {
      return
    }

    if (routingKeys.length === 1) {
      throw (results[0] as PromiseRejectedResult).reason
    }

    const successfulRoutingKeys = routingKeys.filter(
      (_, index) => results[index].status === 'fulfilled',
    )
    const error = new Error(
      `AMQP publish failed for routing keys: ${failedRoutingKeys.join(', ')}`,
    ) as Error & {
      successfulRoutingKeys: string[]
      failedRoutingKeys: string[]
      causes: unknown[]
    }
    error.name = 'AmqpPartialPublishError'
    error.successfulRoutingKeys = successfulRoutingKeys
    error.failedRoutingKeys = failedRoutingKeys
    error.causes = results
      .filter(
        (result): result is PromiseRejectedResult =>
          result.status === 'rejected',
      )
      .map(result => result.reason)
    throw error
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

    let rpcRequest: RpcRequestLifecycle | null = null
    let rpcAdmissionController: AbortController | null = null
    let rpcAdmissionTimeout: NodeJS.Timeout | null = null
    let returnToken: string | undefined
    const clearRpcAdmissionTimeout = (): void => {
      if (rpcAdmissionTimeout) {
        clearTimeout(rpcAdmissionTimeout)
        rpcAdmissionTimeout = null
      }
    }

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
        rpcRequest = await this.handleRemoteProcedureCall(
          correlationId,
          replyTo,
        )
        rpcAdmissionController = new AbortController()
        rpcAdmissionTimeout = setTimeout(() => {
          rpcAdmissionController?.abort(
            new Error('Timeout while waiting to publish RPC request'),
          )
        }, config.rpcTimeout || 3000)
      }

      const options = {
        correlationId,
        replyTo,
        ...this.config.amqpProperties,
        ...properties,
      }
      const mandatory = options.mandatory
      if (mandatory && config.waitForConfirms) {
        returnToken = uuidv4()
        options.headers = {
          ...options.headers,
          [RETURN_TOKEN_HEADER]: returnToken,
        }
      }
      // when the name field is empty, publish just like the sendToQueue method;
      // see https://amqp-node.github.io/amqplib/channel_api.html#channel_publish
      const content = this.toPublishBuffer(msg)
      const { confirmation, drain } = await this.writeWithBackpressure(
        publishChannel => {
          let accepted: boolean
          let publishConfirmation: Promise<void> | null = null
          if (config.waitForConfirms) {
            publishConfirmation = new Promise<void>((resolve, reject) => {
              accepted = (publishChannel as ConfirmChannel).publish(
                name,
                routingKey,
                content,
                options,
                error => {
                  if (error) {
                    reject(error)
                    return
                  }
                  resolve()
                },
              )
            })
          } else {
            accepted = publishChannel.publish(
              name,
              routingKey,
              content,
              options,
            )
          }
          return {
            confirmation: publishConfirmation,
            drain:
              accepted === false
                ? this.getChannelDrainGate(publishChannel)
                : null,
          }
        },
        rpcAdmissionController?.signal,
      )
      clearRpcAdmissionTimeout()
      rpcRequest?.startTimeout()
      if (confirmation && drain) {
        await Promise.all([confirmation, drain])
      } else if (confirmation) {
        await confirmation
      } else if (drain) {
        await drain
      }
      if (returnToken) {
        const returnedError = this.returnedPublishes.get(returnToken)
        this.returnedPublishes.delete(returnToken)
        if (returnedError) {
          throw returnedError
        }
      }
    } catch (e) {
      clearRpcAdmissionTimeout()
      if (returnToken) {
        this.returnedPublishes.delete(returnToken)
      }
      if (rpcRequest) {
        await rpcRequest.cancel()
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
  ): Promise<RpcRequestLifecycle> {
    const rpcConfig = this.getRpcConfig(replyTo)

    try {
      const rpcConsumer = await this.getOrCreateRpcConsumer(replyTo, rpcConfig)
      if (rpcConsumer.pending.has(correlationId)) {
        throw new Error(
          `RPC correlation ID is already in use: ${correlationId}`,
        )
      }

      let rpcResponseFinalized = false
      let rpcTimeout: NodeJS.Timeout | null = null
      let interruptRpc: ((error: Error) => void) | null = null

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
        if (interruptRpc) {
          this.rpcInterruptHandlers.delete(interruptRpc)
        }
        if (rpcConsumer.pending.get(correlationId) === pendingRequest) {
          rpcConsumer.pending.delete(correlationId)
        }
        clearRpcTimeout()
        return true
      }

      const cleanupRpcResources = async (): Promise<void> => {
        if (rpcConsumer.pending.size === 0) {
          await this.cleanupRpcConsumer(rpcConsumer)
        }
      }

      const cancelRpcConsumer = async (): Promise<void> => {
        finalizeRpcResponse()
        await cleanupRpcResources()
      }

      const pendingRequest: PendingRpcRequest = {
        owner: this,
        handleResponse: msg => {
          if (finalizeRpcResponse()) {
            this.node.send(msg as any)
            void cleanupRpcResources()
          }
        },
        interrupt: (error, cleanup) => {
          if (finalizeRpcResponse()) {
            this.node.send({
              payload: {
                message: `RPC interrupted: ${error.message}`,
                config: rpcConfig,
              },
            })
            if (cleanup) {
              void cleanupRpcResources()
            }
          }
        },
        cancel: () => {
          finalizeRpcResponse()
        },
      }
      rpcConsumer.pending.set(correlationId, pendingRequest)
      interruptRpc = error => pendingRequest.interrupt(error, false)
      this.rpcInterruptHandlers.add(interruptRpc)

      const startTimeout = (): void => {
        if (rpcResponseFinalized || rpcTimeout) {
          return
        }

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
              const unmatchedResponseCount =
                rpcConsumer.unmatchedResponseCount
              const unmatchedResponseDiagnostic =
                unmatchedResponseCount > 0
                  ? ` Observed ${unmatchedResponseCount}${rpcConsumer.unmatchedResponseOverflow ? '+' : ''} unmatched RPC response${unmatchedResponseCount === 1 && !rpcConsumer.unmatchedResponseOverflow ? '' : 's'} on the shared reply consumer.`
                  : ''
              this.node.send({
                payload: {
                  message: `Timeout while waiting for RPC response.${unmatchedResponseDiagnostic}`,
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
      }
      return { cancel: cancelRpcConsumer, startTimeout }
    } catch (e) {
      this.node.error(`Could not consume RPC message: ${e}`)
      throw e
    }
  }

  private async getOrCreateRpcConsumer(
    replyTo: string,
    rpcConfig: AmqpConfig,
  ): Promise<RpcConsumerState> {
    const registry = this.getRpcConsumerRegistry()
    const existing = registry.get(replyTo)
    if (existing?.cleanedUp) {
      if (existing.cleanupPromise) {
        await existing.cleanupPromise
      }
      if (registry.get(replyTo) === existing) {
        registry.delete(replyTo)
      }
      return this.getOrCreateRpcConsumer(replyTo, rpcConfig)
    }
    if (existing) {
      existing.users.add(this)
      this.rpcConsumers.set(replyTo, existing)
      await existing.ready
      return existing
    }

    const state: RpcConsumerState = {
      key: replyTo,
      channel: this.channel,
      queueName: '',
      consumerTag: '',
      pending: new Map(),
      unmatchedResponseCount: 0,
      unmatchedResponseOverflow: false,
      ready: Promise.resolve(),
      cleanupPromise: null,
      cleanedUp: false,
      registry,
      users: new Set([this]),
    }

    registry.set(replyTo, state)
    this.rpcConsumers.set(replyTo, state)
    state.ready = (async () => {
      try {
        state.queueName = await this.assertQueue(rpcConfig)
        const consumeResponse = await state.channel.consume(
          state.queueName,
          amqpMessage => {
            if (!amqpMessage) {
              const error = new Error('AMQP RPC consumer was cancelled')
              for (const request of [...state.pending.values()]) {
                request.interrupt(error, true)
              }
              return
            }

            const msg = this.assembleMessage(amqpMessage)
            const receivedCorrelationId = msg.properties.correlationId
            const request =
              typeof receivedCorrelationId === 'string'
                ? state.pending.get(receivedCorrelationId)
                : undefined
            if (request) {
              request.handleResponse(msg)
              return
            }

            if (
              state.unmatchedResponseCount <
              MAX_UNMATCHED_RPC_RESPONSES
            ) {
              state.unmatchedResponseCount += 1
            } else {
              state.unmatchedResponseOverflow = true
            }
          },
          { noAck: rpcConfig.noAck },
        )
        state.consumerTag = consumeResponse?.consumerTag || ''
      } catch (error) {
        await this.cleanupRpcConsumer(state)
        throw error
      }
    })()

    await state.ready
    return state
  }

  private getRpcConsumerRegistry(): Map<string, RpcConsumerState> {
    const scope = (this.connection || this.channel) as unknown as object
    let registry = Amqp.rpcConsumerRegistries.get(scope)
    if (!registry) {
      registry = new Map()
      Amqp.rpcConsumerRegistries.set(scope, registry)
    }
    return registry
  }

  private async cleanupRpcConsumer(state: RpcConsumerState): Promise<void> {
    if (state.cleanupPromise) {
      await state.cleanupPromise
      return
    }
    if (state.cleanedUp) {
      return
    }

    state.cleanedUp = true
    for (const user of state.users) {
      if (user.rpcConsumers.get(state.key) === state) {
        user.rpcConsumers.delete(state.key)
      }
    }
    state.users.clear()

    state.cleanupPromise = (async () => {
      if (!state.queueName) {
        return
      }

      try {
        await state.channel.deleteQueue(state.queueName)
      } catch (deleteError) {
        this.node.error(`Error trying to cancel RPC consumer: ${deleteError}`)

        const canCancelConsumer = typeof state.channel.cancel === 'function'
        if (canCancelConsumer && state.consumerTag) {
          try {
            await state.channel.cancel(state.consumerTag)
          } catch (cancelError) {
            this.node.error(`Error trying to cancel RPC consumer: ${cancelError}`)
          }
        }
      }
    })()

    try {
      await state.cleanupPromise
    } finally {
      if (state.registry.get(state.key) === state) {
        state.registry.delete(state.key)
      }
    }
  }

  private async detachRpcConsumers(interruption?: Error): Promise<void> {
    const states = [...new Set(this.rpcConsumers.values())]
    for (const state of states) {
      for (const request of [...state.pending.values()]) {
        if (request.owner === this) {
          request.cancel()
        }
      }

      state.users.delete(this)
      if (this.rpcConsumers.get(state.key) === state) {
        this.rpcConsumers.delete(state.key)
      }

      if (state.channel === this.channel) {
        const error =
          interruption ?? new Error('Shared AMQP RPC consumer was closed')
        for (const request of [...state.pending.values()]) {
          request.interrupt(error, false)
        }
        state.pending.clear()
        state.cleanedUp = true
        if (state.registry.get(state.key) === state) {
          state.registry.delete(state.key)
        }
        for (const user of state.users) {
          if (user.rpcConsumers.get(state.key) === state) {
            user.rpcConsumers.delete(state.key)
          }
        }
        state.users.clear()
      } else if (state.pending.size === 0) {
        await this.cleanupRpcConsumer(state)
      }
    }
  }

  private async waitForWritableChannel(
    signal?: AbortSignal,
  ): Promise<Channel> {
    while (true) {
      this.throwIfWriteAborted(signal)
      const channel = this.channel
      const failure = this.channelWriteFailure
      if (failure?.channel === channel) {
        throw failure.error
      }
      const gate = this.channelDrainGate
      if (!gate || gate.channel !== channel) {
        return channel
      }
      await this.waitForWriteAdmission(gate.promise, signal)
    }
  }

  private async writeWithBackpressure<T>(
    write: (channel: Channel) => T,
    signal?: AbortSignal,
  ): Promise<T> {
    this.throwIfWriteAborted(signal)
    const initialChannel = this.channel
    const failure = this.channelWriteFailure
    if (failure?.channel === initialChannel) {
      throw failure.error
    }
    const gateActive = this.channelDrainGate?.channel === initialChannel
    const queueActive = this.channelWriteQueue?.channel === initialChannel
    if (!gateActive && !queueActive) {
      return write(initialChannel)
    }

    const release = await this.acquireChannelWrite(initialChannel, signal)
    try {
      const channel = await this.waitForWritableChannel(signal)
      if (this.closed) {
        throw new Error('AMQP channel closed while waiting for write buffer')
      }
      return write(channel)
    } finally {
      release()
    }
  }

  private async acquireChannelWrite(
    channel: Channel,
    signal?: AbortSignal,
  ): Promise<() => void> {
    this.throwIfWriteAborted(signal)
    const previous =
      this.channelWriteQueue?.channel === channel
        ? this.channelWriteQueue.tail
        : Promise.resolve()
    let release: () => void = () => undefined
    const ticket = new Promise<void>(resolve => {
      release = resolve
    })
    const queue = {
      channel,
      tail: previous.then(() => ticket),
    }
    this.channelWriteQueue = queue
    try {
      await this.waitForWriteAdmission(previous, signal)
    } catch (error) {
      release()
      void queue.tail.then(() => {
        if (this.channelWriteQueue === queue) {
          this.channelWriteQueue = null
        }
      })
      throw error
    }

    return () => {
      release()
      if (this.channelWriteQueue === queue) {
        this.channelWriteQueue = null
      }
    }
  }

  private throwIfWriteAborted(signal?: AbortSignal): void {
    if (!signal?.aborted) {
      return
    }
    throw signal.reason instanceof Error
      ? signal.reason
      : new Error('AMQP write admission cancelled')
  }

  private async waitForWriteAdmission<T>(
    promise: Promise<T>,
    signal?: AbortSignal,
  ): Promise<T> {
    if (!signal) {
      return promise
    }
    this.throwIfWriteAborted(signal)

    return new Promise<T>((resolve, reject) => {
      const onAbort = (): void => {
        reject(
          signal.reason instanceof Error
            ? signal.reason
            : new Error('AMQP write admission cancelled'),
        )
      }
      signal.addEventListener('abort', onAbort, { once: true })
      promise.then(
        value => {
          signal.removeEventListener('abort', onAbort)
          resolve(value)
        },
        error => {
          signal.removeEventListener('abort', onAbort)
          reject(error)
        },
      )
    })
  }

  private getChannelDrainGate(channel: Channel): Promise<void> {
    const existingGate = this.channelDrainGate
    if (existingGate?.channel === channel) {
      return existingGate.promise
    }
    existingGate?.reject(
      new Error('AMQP channel replaced while waiting for write buffer'),
    )

    let resolveGate: () => void = () => undefined
    let rejectGate: (error: Error) => void = () => undefined
    const promise = new Promise<void>((resolve, reject) => {
      resolveGate = resolve
      rejectGate = reject
    })
    let settled = false
    const gate: ChannelDrainGate = {
      channel,
      promise,
      reject: error => {
        this.channelWriteFailure = { channel, error }
        settle(() => rejectGate(error))
      },
    }
    const cleanup = (): void => {
      channel.off('drain', onDrain)
      channel.off('close', onClose)
      channel.off('error', onError)
      if (this.channelDrainGate === gate) {
        this.channelDrainGate = null
      }
    }
    const settle = (complete: () => void): void => {
      if (settled) {
        return
      }
      settled = true
      cleanup()
      complete()
    }
    const onDrain = (): void => settle(resolveGate)
    const onClose = (): void =>
      gate.reject(
        new Error('AMQP channel closed while waiting for write buffer'),
      )
    const onError = (error: unknown): void =>
      gate.reject(
        error instanceof Error
          ? error
          : new Error(`AMQP channel error: ${String(error)}`),
      )

    this.channelDrainGate = gate
    channel.once('drain', onDrain)
    channel.once('close', onClose)
    channel.once('error', onError)
    return promise
  }

  public async close(options?: Error | AmqpCloseOptions): Promise<void> {
    const interruption =
      options instanceof Error ? options : options?.interruption
    const removeBindings =
      !(options instanceof Error) && options?.removeBindings === true
    const cleanupDeadline = removeBindings
      ? Date.now() + BINDING_CLEANUP_TIMEOUT_MS
      : undefined
    if (this.bindingCleanupPromise) {
      await this.bindingCleanupPromise
      return
    }
    if (this.closed) {
      let priorCloseError: unknown
      if (removeBindings) {
        try {
          await this.awaitCleanupOperation(
            Promise.resolve(this.closePromise),
            cleanupDeadline as number,
            'finish existing close',
          )
        } catch (error) {
          priorCloseError = error
        }
      } else {
        await this.closePromise
      }
      if (removeBindings) {
        await this.removeBindingsAfterClose(
          BINDING_CLEANUP_MAX_ATTEMPTS,
          cleanupDeadline,
          priorCloseError,
        )
      }
      return
    }
    this.closed = true
    this.lifecycleVersion += 1
    const closeOperation = (async (): Promise<void> => {
      let cleanupError: unknown
      if (interruption) {
        for (const interruptRpc of [...this.rpcInterruptHandlers]) {
          interruptRpc(interruption)
        }
      }

      if (removeBindings) {
        try {
          await this.awaitCleanupOperation(
            this.detachRpcConsumers(interruption),
            cleanupDeadline as number,
            'detach RPC consumers',
          )
        } catch (error) {
          cleanupError = error
        }
      } else {
        await this.detachRpcConsumers(interruption)
      }
      this.clearRpcTimeouts()

      let bindingsRemoved = false
      if (removeBindings) {
        try {
          bindingsRemoved = await this.awaitCleanupOperation(
            this.unbindQueues(true),
            cleanupDeadline as number,
            'unbind queues',
          )
          this.bindingsRemoved ||= bindingsRemoved
        } catch (error) {
          cleanupError = error
        }
        try {
          await this.awaitCleanupOperation(
            this.closeChannel(),
            cleanupDeadline as number,
            'close channel',
          )
        } catch (error) {
          cleanupError ??= error
        }
        try {
          await this.awaitCleanupOperation(
            this.releaseConnection(),
            cleanupDeadline as number,
            'release connection',
          )
        } catch (error) {
          cleanupError ??= error
        }
      } else {
        try {
          bindingsRemoved = await this.unbindQueues(false)
        } finally {
          try {
            await this.closeChannel()
          } finally {
            await this.releaseConnection()
          }
        }
      }
      if (removeBindings && !bindingsRemoved) {
        await this.removeBindingsAfterClose(
          BINDING_CLEANUP_MAX_ATTEMPTS - 1,
          cleanupDeadline,
          cleanupError,
        )
      } else if (cleanupError) {
        throw cleanupError
      }
    })()
    this.closePromise = closeOperation
    try {
      await closeOperation
    } finally {
      if (this.closePromise === closeOperation) {
        this.closePromise = null
      }
    }
  }

  private async removeBindingsAfterClose(
    maxAttempts = BINDING_CLEANUP_MAX_ATTEMPTS,
    deadline = Date.now() + BINDING_CLEANUP_TIMEOUT_MS,
    initialError?: unknown,
  ): Promise<void> {
    const { name: exchangeName } = this.config.exchange
    if (
      this.bindingsRemoved ||
      !exchangeName ||
      !this.q?.queue ||
      !this.shouldUnbindQueueOnClose(true)
    ) {
      return
    }
    if (this.bindingCleanupPromise) {
      await this.bindingCleanupPromise
      return
    }

    const cleanupOperation = (async (): Promise<void> => {
      let lastError: unknown = initialError
      for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
        const remainingTime = deadline - Date.now()
        if (remainingTime <= 0) {
          break
        }
        let connected = false
        try {
          await this.connect({ timeout: remainingTime })
          connected = true
          await this.awaitCleanupOperation(
            this.createChannel(),
            deadline,
            'create cleanup channel',
          )
          if (
            await this.awaitCleanupOperation(
              this.unbindQueues(true),
              deadline,
              'unbind queues',
            )
          ) {
            this.bindingsRemoved = true
            return
          }
          lastError = new Error('One or more AMQP queue bindings could not be removed')
        } catch (error) {
          lastError = error
        } finally {
          if (connected) {
            this.closed = true
            this.lifecycleVersion += 1
            try {
              await this.awaitCleanupOperation(
                this.closeChannel(),
                deadline,
                'close cleanup channel',
              )
            } finally {
              await this.awaitCleanupOperation(
                this.releaseConnection(),
                deadline,
                'release cleanup connection',
              )
            }
          }
        }
        if (Date.now() >= deadline) {
          break
        }
      }
      const cleanupError = new Error(
        `Could not remove AMQP bindings after ${BINDING_CLEANUP_MAX_ATTEMPTS} attempts`,
      )
      ;(cleanupError as Error & { cause?: unknown }).cause = lastError
      throw cleanupError
    })()
    this.bindingCleanupPromise = cleanupOperation
    try {
      await cleanupOperation
    } finally {
      if (this.bindingCleanupPromise === cleanupOperation) {
        this.bindingCleanupPromise = null
      }
    }
  }

  private async awaitCleanupOperation<T>(
    operation: Promise<T>,
    deadline: number,
    operationName: string,
  ): Promise<T> {
    const remainingTime = Math.max(0, deadline - Date.now())
    let timeoutHandle: NodeJS.Timeout | undefined
    try {
      return await Promise.race([
        operation,
        new Promise<never>((_resolve, reject) => {
          timeoutHandle = setTimeout(
            () =>
              reject(
                new Error(
                  `AMQP binding cleanup timed out while attempting to ${operationName}`,
                ),
              ),
            remainingTime,
          )
        }),
      ])
    } finally {
      if (timeoutHandle) {
        clearTimeout(timeoutHandle)
      }
    }
  }

  private async unbindQueues(removeBindings = false): Promise<boolean> {
    const { name: exchangeName } = this.config.exchange
    const queueName = this.q?.queue
    let succeeded = true

    if (
      exchangeName &&
      queueName &&
      this.shouldUnbindQueueOnClose(removeBindings)
    ) {
      const routingKeys = this.parseRoutingKeys()
      for (const routingKey of routingKeys) {
        try {
          await this.channel.unbindQueue(queueName, exchangeName, routingKey)
        } catch (e) {
          if (this.isNotFoundError(e)) {
            return true
          }
          succeeded = false
          /* istanbul ignore next */
          this.node.error(`Error unbinding queue for routing key ${routingKey}: ${e.message}`)
        }
      }
    }
    return succeeded
  }

  private isNotFoundError(error: unknown): boolean {
    if (!error || typeof error !== 'object') {
      return false
    }
    const amqpError = error as {
      code?: number
      replyCode?: number
      message?: string
    }
    return (
      amqpError.code === 404 ||
      amqpError.replyCode === 404 ||
      /(?:404|NOT[_-]FOUND)/i.test(amqpError.message ?? '')
    )
  }

  private shouldUnbindQueueOnClose(removeBindings: boolean): boolean {
    const { name, exclusive, autoDelete } = this.config.queue

    // Keep bindings for long-lived queues so reconnects don't temporarily
    // remove routes and drop unroutable messages in-flight.
    return (
      this.shouldAutoCreateExchangeBindings() &&
      (removeBindings || !name || exclusive || autoDelete)
    )
  }

  private shouldAutoCreateExchangeBindings(configParams?: AmqpConfig): boolean {
    return (configParams || this.config).exchange.autoCreate
  }

  private shouldAutoCreateQueue(configParams?: AmqpConfig): boolean {
    return (configParams || this.config).queue.autoCreate
  }

  private async closeChannel(): Promise<void> {
    this.trackedDeliveries.clear()
    this.returnedPublishes.clear()
    this.rpcInterruptHandlers.clear()
    if (this.channel) {
      if (this.channelDrainGate?.channel === this.channel) {
        this.channelDrainGate.reject(
          new Error('AMQP channel closed while waiting for write buffer'),
        )
      }
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
    const brokerId = this.config.broker
    const broker = this.broker as unknown as BrokerConfig
    const vhost = this.vhostOverride ?? broker?.vhost
    const key = this.connectionPoolKey ?? `${brokerId}:${vhost}`

    this.setBrokerNodeState('disconnected')
    this.node.status(NODE_STATUS.Disconnected)

    if (this.connection) {
      this.connection.off?.('error', this.connectionErrorHandler)
      this.connection.off?.('close', this.connectionCloseHandler)
    }

    const entry = Amqp.connectionPool.get(key)
    if (entry?.connection === this.connection) {
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
    this.connectionPoolKey = undefined
  }

  private async createPooledConnection(
    brokerUrl: string,
    brokerInfo: string,
  ): Promise<ChannelModel> {
    const connection = await connect(brokerUrl, { heartbeat: 2 })
    this.node.log(`Connected to AMQP broker ${brokerInfo}`)
    return connection
  }

  private getEndpointGenerationKey(
    logicalKey: string,
    brokerUrl: string,
  ): string {
    const current = Amqp.endpointGenerations.get(logicalKey)
    if (!current) {
      Amqp.endpointGenerations.set(logicalKey, { brokerUrl, generation: 0 })
      return logicalKey
    }
    if (current.brokerUrl !== brokerUrl) {
      current.brokerUrl = brokerUrl
      current.generation += 1
    }
    return current.generation === 0
      ? logicalKey
      : `${logicalKey}#${current.generation}`
  }

  private async awaitConnection(
    pendingConnection: Promise<ChannelModel>,
    brokerInfo: string,
    timeout?: number,
  ): Promise<ChannelModel> {
    if (timeout === undefined) {
      return pendingConnection
    }

    let timeoutHandle: NodeJS.Timeout | undefined
    try {
      return await Promise.race([
        pendingConnection,
        new Promise<never>((_resolve, reject) => {
          timeoutHandle = setTimeout(
            () =>
              reject(
                new Error(
                  `AMQP connection to ${brokerInfo} timed out after ${timeout}ms`,
                ),
              ),
            timeout,
          )
        }),
      ])
    } finally {
      if (timeoutHandle) {
        clearTimeout(timeoutHandle)
      }
    }
  }

  private async createChannel(): Promise<Channel> {
    const { prefetch, waitForConfirms } = this.config
    const lifecycleVersion = this.lifecycleVersion

    this.trackedDeliveries.clear()
    const channel = await (waitForConfirms
      ? this.connection.createConfirmChannel()
      : this.connection.createChannel())
    if (this.closed || lifecycleVersion !== this.lifecycleVersion) {
      try {
        await channel.close()
      } catch (error) {
        this.node.error(`Error closing stale AMQP channel: ${error}`)
      }
      throw new Error('AMQP channel creation completed after resources were closed')
    }

    this.channel = channel
    this.channelWriteFailure = null
    channel.prefetch(Number(prefetch))

    this.channelErrorHandler = (e): void => {
      /* istanbul ignore next */
      this.setBrokerNodeState('errored', e)
      this.node.status(NODE_STATUS.Disconnected)
      this.node.error(`AMQP Connection Error ${e}`, { payload: { error: e, source: 'Amqp' } })
    }

    this.channelCloseHandler = (): void => {
      /* istanbul ignore next */
      this.node.status(NODE_STATUS.Disconnected)
      this.node.log('AMQP Channel closed')
    }

    this.channelReturnHandler = (message): void => {
      /* istanbul ignore next */
      const returnToken = message.properties.headers?.[RETURN_TOKEN_HEADER]
      const { replyCode, replyText, exchange, routingKey } = message.fields as
        typeof message.fields & {
          replyCode: number
          replyText: string
        }
      const returnedError = new Error(
        `AMQP Message returned (${replyCode} ${replyText}) from ${exchange} with routing key ${routingKey}`,
      )
      if (typeof returnToken === 'string') {
        this.returnedPublishes.set(returnToken, returnedError)
      }
      this.node.warn(returnedError.message)
    }

    channel.on('error', this.channelErrorHandler)
    channel.on('close', this.channelCloseHandler)
    channel.on('return', this.channelReturnHandler)

    return channel;
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

  private useExistingQueue(configParams?: AmqpConfig): string {
    const { queue } = configParams || this.config
    const { name } = queue

    if (!name) {
      throw new Error('Queue Name is required when "Auto-create" queue is disabled')
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
    const msg = amqpMessage as TrackedAssembledMessage
    msg.payload = payload
    if (this.isManualAck()) {
      const token = uuidv4()
      msg[DELIVERY_TOKEN] = token
      this.trackedDeliveries.set(token, {
        message: msg,
        channel: this.channel,
        deliveryTag: msg.fields.deliveryTag,
      })
    }
    return msg
  }

  private isManualAck(): boolean {
    return this.node.type === NodeType.AmqpInManualAck
  }

  private resolveDelivery(
    msg: AssembledMessage,
    operation: string,
  ): (TrackedDelivery & { token?: string }) | null {
    if (!this.isManualAck()) {
      return {
        message: msg,
        channel: this.channel,
        deliveryTag: msg.fields?.deliveryTag ?? 0,
      }
    }

    const token = (msg as TrackedAssembledMessage)[DELIVERY_TOKEN]
    const delivery = token ? this.trackedDeliveries.get(token) : undefined
    if (!delivery || delivery.channel !== this.channel) {
      this.node.error(
        `Could not ${operation} message: delivery does not belong to the active AMQP channel`,
      )
      return null
    }

    return { ...delivery, token }
  }

  private removeTrackedDeliveries(
    delivery: TrackedDelivery & { token?: string },
    allUpTo: boolean,
  ): void {
    if (!this.isManualAck()) {
      return
    }

    if (!allUpTo) {
      if (delivery.token) {
        this.trackedDeliveries.delete(delivery.token)
      }
      return
    }

    for (const [token, trackedDelivery] of this.trackedDeliveries) {
      if (
        trackedDelivery.channel === delivery.channel &&
        trackedDelivery.deliveryTag <= delivery.deliveryTag
      ) {
        this.trackedDeliveries.delete(token)
      }
    }
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

  private isConnectionOpen(connection: ChannelModel): boolean {
    const stream = (connection as { connection?: { stream?: { destroyed?: boolean } } })
      .connection?.stream
    return stream?.destroyed !== true
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
    const code = error && typeof error === 'object' ? String((error as { code?: unknown }).code || '') : ''
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
