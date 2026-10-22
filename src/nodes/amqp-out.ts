import type { MessageProperties } from 'amqplib'
import type {
  EditorNodeProperties,
  NodeMessageInFlow,
  NodeRedApp,
} from 'node-red'
import Amqp from '../Amqp'
import { startNode, toError } from '../node-lifecycle'
import {
  type AmqpInNodeDefaults,
  type AmqpOutNodeDefaults,
  NodeType,
} from '../types'

module.exports = (RED: NodeRedApp): void => {
  function AmqpOut(
    config: EditorNodeProperties & {
      exchangeRoutingKey: string
      exchangeRoutingKeyType: string
      amqpProperties: string
    },
  ): void {
    // @ts-expect-error Node-RED supplies the runtime node instance.
    RED.nodes.createNode(this, config)
    const amqp = new Amqp(
      RED,
      this,
      config as AmqpInNodeDefaults & AmqpOutNodeDefaults,
    )
    startNode(this, amqp)

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
      const { exchangeRoutingKey, exchangeRoutingKeyType, amqpProperties } =
        config

      // message properties override config properties
      let properties: MessageProperties
      try {
        properties = {
          ...JSON.parse(amqpProperties),
          ...msgProperties,
        }
      } catch {
        properties = msgProperties
      }

      let effectiveRoutingKey: string | undefined
      switch (exchangeRoutingKeyType) {
        case 'msg':
        case 'flow':
        case 'global':
          try {
            effectiveRoutingKey = RED.util.evaluateNodeProperty(
              exchangeRoutingKey,
              exchangeRoutingKeyType,
              this,
              msg,
            )
          } catch (err) {
            this.error(`Failed to evaluate routing key: ${err}`)
            done && done(toError(err))
            return
          }
          break
        case 'jsonata': {
          try {
            const expr = RED.util.prepareJSONataExpression(
              exchangeRoutingKey,
              this,
            )
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

            effectiveRoutingKey = String(result)
          } catch (err) {
            this.error(`Failed to evaluate JSONata expression: ${err}`)
            done && done(toError(err))
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
            effectiveRoutingKey = routingKey
          }
          break
      }

      if (vhost) {
        try {
          await amqp.setVhost(vhost)
        } catch (e) {
          this.error(`Could not switch vhost: ${e}`)
          done && done(toError(e))
          return
        }
      }

      try {
        await amqp.publish(payload, properties, effectiveRoutingKey)
      } catch (e) {
        done && done(toError(e))
        return
      }

      done && done()
    }

    this.on('input', inputListener)
  }
  // @ts-expect-error Node-RED supplies the runtime node instance.
  RED.nodes.registerType(NodeType.AmqpOut, AmqpOut)
}
