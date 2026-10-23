import type { Options } from 'amqplib'
import type { Node, NodeAPI, NodeMessageInFlow } from 'node-red'
import { createAmqpNode, toError } from '../node-lifecycle'
import { type AmqpNodeConfig, NodeType } from '../types'

export = (RED: NodeAPI): void => {
  function AmqpOut(
    this: Node,
    config: AmqpNodeConfig & {
      exchangeRoutingKey: string
      exchangeRoutingKeyType: string
      amqpProperties: string
    },
  ): void {
    const amqp = createAmqpNode(RED, this, config)

    const resolveRoutingKey = async (
      msg: NodeMessageInFlow & { routingKey?: string },
    ): Promise<string | undefined> => {
      const { exchangeRoutingKey, exchangeRoutingKeyType } = config
      try {
        switch (exchangeRoutingKeyType) {
          case 'msg':
          case 'flow':
          case 'global':
            return RED.util.evaluateNodeProperty(
              exchangeRoutingKey,
              exchangeRoutingKeyType,
              this,
              msg,
            )
          case 'jsonata': {
            const expr = RED.util.prepareJSONataExpression(
              exchangeRoutingKey,
              this,
            )
            const result = await new Promise<unknown>((resolve, reject) => {
              RED.util.evaluateJSONataExpression(expr, msg, (err, value) => {
                if (err) reject(err)
                else resolve(value)
              })
            })
            if (typeof result !== 'string') {
              this.warn(
                `Routing key JSONata expression returned ${typeof result}; coercing to string`,
              )
            }
            return String(result)
          }
          case 'str':
          default:
            // Preserve the per-message override; Amqp.publish applies the configured default.
            return msg.routingKey || undefined
        }
      } catch (err) {
        const expression =
          exchangeRoutingKeyType === 'jsonata'
            ? 'JSONata expression'
            : 'routing key'
        this.error(`Failed to evaluate ${expression}: ${err}`)
        throw err
      }
    }

    const inputListener = async (
      msg: NodeMessageInFlow & {
        routingKey?: string
        vhost?: string
        properties?: Options.Publish
      },
      _: unknown,
      done?: (err?: Error) => void,
    ) => {
      const { payload, vhost, properties: msgProperties } = msg
      const { amqpProperties } = config

      // message properties override config properties
      let properties: Options.Publish
      try {
        properties = {
          ...JSON.parse(amqpProperties),
          ...msgProperties,
        }
      } catch {
        properties = msgProperties
      }

      let effectiveRoutingKey: string | undefined
      try {
        effectiveRoutingKey = await resolveRoutingKey(msg)
      } catch (err) {
        done && done(toError(err))
        return
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
  RED.nodes.registerType(NodeType.AmqpOut, AmqpOut)
}
