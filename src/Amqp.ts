import { NodeRedApp, Node } from 'node-red'
import { v4 as uuidv4 } from 'uuid'
import cloneDeep = require('lodash.clonedeep')
import {
  ChannelModel,
  Channel,
  Replies,
  connect,
  ConsumeMessage,
  MessageProperties,
} from 'amqplib'
import {
  AmqpConfig,
  BrokerConfig,
  NodeType,
  AssembledMessage,
  GenericJsonObject,
  ExchangeType,
  AmqpInNodeDefaults,
  AmqpOutNodeDefaults,
} from './types'
import { NODE_STATUS } from './constants'

export default class Amqp {
  private config: AmqpConfig
  private broker: Node
  private connection: ChannelModel
  private channel: Channel
  private q: Replies.AssertQueue
  private vhostOverride?: string

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
        queueArguments: this.parseJson(config.queueArguments)
      },
      amqpProperties: this.parseJson(
        config.amqpProperties,
      ) as MessageProperties,
      headers: this.parseJson(config.headers),
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
    this.node.log(`Connecting to AMQP broker ${brokerInfo}`)

    try {
      this.connection = await connect(brokerUrl, { heartbeat: 2 })
      this.node.log(`Connected to AMQP broker ${brokerInfo}`)
    } catch (err) {
      this.node.warn(`Failed to connect to AMQP broker ${brokerInfo}: ${err}`)
      throw err
    }

    /* istanbul ignore next */
    this.connection.on('error', (e): void => {
      // Set node to disconnected status
      this.node.status(NODE_STATUS.Disconnected)
      this.node.warn(`AMQP connection error ${e}`)
    })

    /* istanbul ignore next */
    this.connection.on('close', () => {
      this.node.status(NODE_STATUS.Disconnected)
      this.node.log(`AMQP Connection closed`);
    })

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
      this.channel.ack(msg, allUpTo)
    } catch (e) {
      this.node.error(`Could not ack message: ${e}`)
    }
  }

  public ackAll(): void {
    try {
      this.channel.ackAll()
    } catch (e) {
      this.node.error(`Could not ackAll messages: ${e}`)
    }
  }

  public nack(msg: AssembledMessage): void {
    const allUpTo = !!msg.manualAck?.allUpTo
    const requeue = msg.manualAck?.requeue ?? true
    try {
      this.channel.nack(msg, allUpTo, requeue)
    } catch (e) {
      this.node.error(`Could not nack message: ${e}`)
    }
  }

  public nackAll(msg: AssembledMessage): void {
    const requeue = msg.manualAck?.requeue ?? true
    try {
      this.channel.nackAll(requeue)
    } catch (e) {
      this.node.error(`Could not nackAll messages: ${e}`)
    }
  }

  public reject(msg: AssembledMessage): void {
    const requeue = msg.manualAck?.requeue ?? true
    try {
      this.channel.reject(msg, requeue)
    } catch (e) {
      this.node.error(`Could not reject message: ${e}`)
    }
  }

  public async publish(
    msg: unknown,
    properties?: MessageProperties,
  ): Promise<void> {
    this.parseRoutingKeys().forEach(async routingKey => {
      this.handlePublish(this.config, msg, properties, routingKey)
    })
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
    const { name: exchangeName } = this.config.exchange
    const queueName = this.q?.queue

    try {
      /* istanbul ignore else */
      if (exchangeName && queueName) {
        const routingKeys = this.parseRoutingKeys()
        try {
          for (let x = 0; x < routingKeys.length; x++) {
            await this.channel.unbindQueue(
              queueName,
              exchangeName,
              routingKeys[x],
            )
          }
        } catch (e) {
          /* istanbul ignore next */
          console.error('Error unbinding queue: ', e.message)
        }
      }
    } catch (e) {
      /* istanbul ignore next */
      this.node.error(`Error unbinding queue: ${e}`)
    }

    if (this.channel) {
      try {
        await this.channel.close()
      } catch (e) {
        this.node.error(`Error closing AMQP channel: ${e}`)
      }
    }

    if (this.connection) {
      try {
        await this.connection.close()
      } catch (e) {
        this.node.error(`Error closing AMQP connection: ${e}`)
      }
    }
  }

  private async createChannel(): Promise<Channel> {
    const { prefetch } = this.config

    this.channel = await this.connection.createChannel()
    this.channel.prefetch(Number(prefetch))

    /* istanbul ignore next */
    this.channel.on('error', (e): void => {
      // Set node to disconnected status
      this.node.status(NODE_STATUS.Disconnected)
      this.node.error(`AMQP Connection Error ${e}`, { payload: { error: e, source: 'Amqp' } })
    })

    /* istanbul ignore next */
    this.channel.on('close', (): void => {
      this.node.status(NODE_STATUS.Disconnected)
      this.node.log('AMQP Channel closed')
    })

    /* istanbul ignore next */
    this.channel.on('return', (): void => {
      this.node.warn('AMQP Message returned')
    })

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
      url = `${protocol}://${encodeURIComponent(username)}:${encodeURIComponent(
        password,
      )}@${host}:${port}/${vhost}`
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
    const payload = this.parseJson(amqpMessage.content.toString())

    return {
      ...amqpMessage,
      payload,
    }
  }

  private isManualAck(): boolean {
    return this.node.type === NodeType.AmqpInManualAck
  }

  private parseJson(jsonInput: unknown): GenericJsonObject {
    let output: unknown
    try {
      output = JSON.parse(jsonInput as string)
    } catch {
      output = jsonInput
    }
    return output
  }
}
