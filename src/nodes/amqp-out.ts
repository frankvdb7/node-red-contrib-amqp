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
    let reconnect = null;
    let connection = null;
    let channel = null;
	let me = this;

    RED.events.once('flows:stopped', () => {
      clearTimeout(reconnectTimeout)
    })

    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    RED.nodes.createNode(this, config)
    this.status(NODE_STATUS.Disconnected)
    
    const configAmqp: AmqpInNodeDefaults & AmqpOutNodeDefaults = config;

    const amqp = new Amqp(RED, this, configAmqp)

    const reconnectOnError = configAmqp.reconnectOnError;


    // handle input event;
    const inputListener = async (msg, _, done) => {
      const { payload, routingKey, properties: msgProperties } = msg
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
      await amqp.close()
      done && done()
    })

    async function initializeNode(nodeIns) {
      reconnect = async () => {
        // check the channel and clear all the event listener
		try {
        if (channel && channel.removeAllListeners) {
          channel.removeAllListeners()
			  channel.close()
				.catch(err => {
					// produce very lengthy messages...
					//me.error(`Error closing channel: ${JSON.stringify(err)}`);
				});

			  //channel = null;
			}
		} catch (error) {
		  // catch and suppress error
		  me.error('Error occurred:', error);
		}
          channel = null;
		
		try {
        // check the connection and clear all the event listener
        if (connection && connection.removeAllListeners) {
          connection.removeAllListeners()
			  connection.close()
				.catch(err => {
					me.error('Error closing connection:', err);
				});
			  //connection = null;
			}
		} catch (error) {
		  // catch and suppress error
		  me.error('Error occurred:', error);
		}
          connection = null;

        // always clear timer before set it;
        clearTimeout(reconnectTimeout);
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

        // istanbul ignore else
        if (connection) {
          channel = await amqp.initialize()

          // When the server goes down
          connection.on('close', async e => {
            e && (await reconnect())
          })
          
          // When the connection goes down
          connection.on('error', async e => {
            reconnectOnError && (await reconnect())
            nodeIns.error(`Connection error ${e}`, { payload: { error: e, location: ErrorLocationEnum.ConnectionErrorEvent } })
          })

          // When the channel goes down
          channel.on('close', async () => {
            await reconnect()
          })

          // When the channel error occur
          channel.on('error', async e => {
            reconnectOnError && (await reconnect())
            nodeIns.error(`Channel error ${e}`, { payload: { error: e, location: ErrorLocationEnum.ChannelErrorEvent } })
          })
          
          nodeIns.status(NODE_STATUS.Connected)
        }
      } catch (e) {
        if (
          e.code === ErrorType.ConnectionRefused ||
          e.code === ErrorType.DnsResolve ||
          e.code === ErrorType.HostNotFound ||
          e.isOperational
        ) {
          reconnectOnError && (await reconnect())
        } else if (
          e.code === ErrorType.InvalidLogin ||
          /ACCESS_REFUSED/i.test(e.message || '')
        ) {
          nodeIns.status(NODE_STATUS.Invalid)
          nodeIns.error(`AmqpOut() Could not connect to broker ${e}`, {
            payload: { error: e, location: ErrorLocationEnum.ConnectError },
          })
        } else {
          nodeIns.status(NODE_STATUS.Error)
          nodeIns.error(`AmqpOut() ${e}`, {
            payload: { error: e, location: ErrorLocationEnum.ConnectError },
          })
        }
      }
    }

    // call
    initializeNode(this);
  }
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  RED.nodes.registerType(NodeType.AmqpOut, AmqpOut)
}
