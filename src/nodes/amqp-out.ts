import { NodeRedApp, EditorNodeProperties } from 'node-red'
import { NODE_STATUS } from '../constants'
import { AmqpInNodeDefaults, AmqpOutNodeDefaults, ErrorLocationEnum, ErrorType, NodeType } from '../types'
import Amqp from '../Amqp'
import { MessageProperties } from 'amqplib'

module.exports = function (RED: NodeRedApp): void {
  function AmqpOut(
    config: EditorNodeProperties & {
      exchangeRoutingKey: string
      exchangeRoutingKeyType: string
      amqpProperties: string
    },
  ): void {
    let reconnectTimeout: NodeJS.Timeout
    let reconnect = null
    let connection = null
    let channel = null
    const me = this

    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    RED.nodes.createNode(this, config)
    this.status(NODE_STATUS.Disconnected)
    
    const configAmqp: AmqpInNodeDefaults & AmqpOutNodeDefaults = config;

    const amqp = new Amqp(RED, this, configAmqp)

    const reconnectOnError = configAmqp.reconnectOnError

    const removeEventListeners = (): void => {
      connection?.removeAllListeners?.()
      channel?.removeAllListeners?.()
    }

    const setupEventListeners = (nodeIns): void => {
      connection.on('close', async e => {
        e && (await reconnect())
      })

      connection.on('error', async e => {
        reconnectOnError && (await reconnect())
        nodeIns.error(`Connection error ${e}`, {
          payload: { error: e, location: ErrorLocationEnum.ConnectionErrorEvent },
        })
      })

      channel.on('close', async () => {
        await reconnect()
      })

      channel.on('error', async e => {
        reconnectOnError && (await reconnect())
        nodeIns.error(`Channel error ${e}`, {
          payload: { error: e, location: ErrorLocationEnum.ChannelErrorEvent },
        })
      })
    }

    const handleError = async (e, nodeIns): Promise<void> => {
      if (
        e.code === ErrorType.InvalidLogin ||
        /ACCESS_REFUSED/i.test(e.message || '')
      ) {
        nodeIns.status(NODE_STATUS.Invalid)
        nodeIns.error(`AmqpOut() Could not connect to broker ${e}`, {
          payload: { error: e, location: ErrorLocationEnum.ConnectError },
        })
      } else if (
        e.code === ErrorType.ConnectionRefused ||
        e.code === ErrorType.DnsResolve ||
        e.code === ErrorType.HostNotFound ||
        e.isOperational
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
    const inputListener = async (msg, _, done) => {
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
            const result = await new Promise<any>((resolve, reject) => {
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
        amqp.publish(payload, properties)
      } else {
        amqp.publish(JSON.stringify(payload), properties)
      }

      done && done()
    }

    this.on('input', inputListener)
    // When the node is re-deployed
    this.on('close', async (done: () => void): Promise<void> => {
      clearTimeout(reconnectTimeout)
      await amqp.close()
      done && done()
    })

    async function initializeNode(nodeIns) {
      reconnect = async () => {
        removeEventListeners()
        try {
          if (channel) {
            channel.close().catch(err => {
              /* istanbul ignore next */
              me.error('Error closing channel:', err)
            })
          }
        } catch (error) {
          /* istanbul ignore next */
          me.error('Error occurred:', error)
        }
        channel = null

        try {
          if (connection) {
            connection.close().catch(err => {
              /* istanbul ignore next */
              me.error('Error closing connection:', err)
            })
          }
        } catch (error) {
          /* istanbul ignore next */
          me.error('Error occurred:', error)
        }
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
