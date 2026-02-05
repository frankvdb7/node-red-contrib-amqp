import { NodeRedApp, EditorNodeProperties, Node, NodeMessageInFlow } from 'node-red'
import { NODE_STATUS } from '../constants'
import { AmqpInNodeDefaults, AmqpOutNodeDefaults, ErrorLocationEnum, ErrorType, NodeType } from '../types'
import Amqp from '../Amqp'
import { MessageProperties, Channel, ChannelModel } from 'amqplib'

module.exports = function (RED: NodeRedApp): void {
  const isErrorLike = (
    value: unknown,
  ): value is { code?: string; message?: string; isOperational?: boolean } =>
    typeof value === 'object' && value !== null

  function AmqpOut(
    config: EditorNodeProperties & {
      exchangeRoutingKey: string
      exchangeRoutingKeyType: string
      amqpProperties: string
    },
  ): void {
    let reconnectTimeout: NodeJS.Timeout
    let reconnect: (() => Promise<void>) | null = null
    let connection: ChannelModel | null = null
    let channel: Channel | null = null
    let onConnClose: (e: unknown) => Promise<void>
    let onConnError: (e: unknown) => Promise<void>
    let onChannelClose: () => Promise<void>
    let onChannelError: (e: unknown) => Promise<void>
    const me = this

    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    RED.nodes.createNode(this, config)
    this.status(NODE_STATUS.Disconnected)
    
    const configAmqp: AmqpInNodeDefaults & AmqpOutNodeDefaults = config;

    const amqp = new Amqp(RED, this, configAmqp)

    const reconnectOnError = configAmqp.reconnectOnError

    const removeEventListeners = (): void => {
      connection?.off?.('close', onConnClose)
      connection?.off?.('error', onConnError)
      channel?.off?.('close', onChannelClose)
      channel?.off?.('error', onChannelError)
    }

    const setupEventListeners = (nodeIns: Node): void => {
      onConnClose = async () => {
        await reconnect()
      }

      onConnError = async e => {
        reconnectOnError && (await reconnect())
        nodeIns.error(`Connection error ${e}`, {
          payload: { error: e, location: ErrorLocationEnum.ConnectionErrorEvent },
        })
      }

      onChannelClose = async () => {
        await reconnect()
      }

      onChannelError = async e => {
        reconnectOnError && (await reconnect())
        nodeIns.error(`Channel error ${e}`, {
          payload: { error: e, location: ErrorLocationEnum.ChannelErrorEvent },
        })
      }

      connection.on('close', onConnClose)
      connection.on('error', onConnError)
      channel.on('close', onChannelClose)
      channel.on('error', onChannelError)
    }

    const handleError = async (e: unknown, nodeIns: Node): Promise<void> => {
      const err = isErrorLike(e) ? e : {}
      if (
        err.code === ErrorType.InvalidLogin ||
        /ACCESS_REFUSED/i.test(err.message || '')
      ) {
        nodeIns.status(NODE_STATUS.Invalid)
        nodeIns.error(`AmqpOut() Could not connect to broker ${e}`, {
          payload: { error: e, location: ErrorLocationEnum.ConnectError },
        })
      } else if (
        err.code === ErrorType.ConnectionRefused ||
        err.code === ErrorType.DnsResolve ||
        err.code === ErrorType.HostNotFound ||
        err.isOperational
      ) {
        reconnectOnError && (await reconnect())
      } else {
        nodeIns.status(NODE_STATUS.Error)
        nodeIns.error(`AmqpOut() ${e}`, {
          payload: { error: e, location: ErrorLocationEnum.ConnectError },
        })
      }
    }

    // handle input event
    const inputListener = async (
      msg: NodeMessageInFlow & {
        routingKey?: string
        vhost?: string
        properties?: MessageProperties
      },
      _: unknown,
      done?: (err?: Error) => void,
    ) => {
      const { payload, routingKey, vhost, properties: msgProperties } = msg
      const {
        exchangeRoutingKey,
        exchangeRoutingKeyType,
        amqpProperties,
      } = config

      // message properties override config properties
      let properties: MessageProperties
      try {
        properties = {
          ...JSON.parse(amqpProperties),
          ...msgProperties,
        }
      } catch (e) {
        properties = msgProperties
      }

      switch (exchangeRoutingKeyType) {
        case 'msg':
        case 'flow':
        case 'global':
          amqp.setRoutingKey(
            RED.util.evaluateNodeProperty(
              exchangeRoutingKey,
              exchangeRoutingKeyType,
              this,
              msg,
            ),
          )
          break
        case 'jsonata': {
          const expr = RED.util.prepareJSONataExpression(exchangeRoutingKey, this)
          try {
            const result = await new Promise<unknown>((resolve, reject) => {
              RED.util.evaluateJSONataExpression(expr, msg, (err, value) => {
                if (err) {
                  reject(err)
                } else {
                  resolve(value)
                }
              })
            })

            if (typeof result !== 'string') {
              this.warn(
                `Routing key JSONata expression returned ${typeof result}; coercing to string`,
              )
            }

            amqp.setRoutingKey(String(result))
          } catch (err) {
            this.error(`Failed to evaluate JSONata expression: ${err}`)
            return
          }
          break
        }
        case 'str':
        default:
          if (routingKey) {
            // if incoming payload contains a routingKey value
            // override our string value with it.

            // Superfluous (and possibly confusing) at this point
            // but keeping it to retain backwards compatibility
            amqp.setRoutingKey(routingKey)
          }
          break
      }

      if (vhost) {
        try {
          removeEventListeners()
          await amqp.setVhost(vhost)
          connection = amqp.getConnection()
          channel = amqp.getChannel()
          setupEventListeners(me)
          me.status(NODE_STATUS.Connected)
        } catch (e) {
          await handleError(e, me)
        }
      }

      if (!!properties?.headers?.doNotStringifyPayload) {
        await amqp.publish(payload, properties)
      } else {
        await amqp.publish(JSON.stringify(payload), properties)
      }

      done && done()
    }

    this.on('input', inputListener)
    // When the node is re-deployed
    this.on('close', async (done: () => void): Promise<void> => {
      clearTimeout(reconnectTimeout)
      removeEventListeners()
      await amqp.close()
      done && done()
    })

    async function initializeNode(nodeIns: Node) {
      reconnect = async () => {
        removeEventListeners()
        await amqp.close()
        channel = null
        connection = null

        clearTimeout(reconnectTimeout)
        reconnectTimeout = setTimeout(() => {
          try {
            initializeNode(nodeIns)
          } catch (e) {
            reconnect()
          }
        }, 2000)
      }

      try {
        connection = await amqp.connect()
        if (connection) {
          channel = await amqp.initialize()
          setupEventListeners(nodeIns)
          nodeIns.status(NODE_STATUS.Connected)
        }
      } catch (e) {
        await handleError(e, nodeIns)
      }
    }

    // call
    initializeNode(this);
  }
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  RED.nodes.registerType(NodeType.AmqpOut, AmqpOut)
}
