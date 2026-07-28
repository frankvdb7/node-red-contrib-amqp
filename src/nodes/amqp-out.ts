import type { MessageProperties } from 'amqplib'
import type {
  EditorNodeProperties,
  NodeMessageInFlow,
  NodeRedApp,
} from 'node-red'
import Amqp from '../Amqp'
import { NODE_STATUS } from '../constants'
import {
  type AmqpInNodeDefaults,
  type AmqpOutNodeDefaults,
  ErrorLocationEnum,
  ErrorType,
  NodeType,
} from '../types'

module.exports = (RED: NodeRedApp): void => {
  const isInvalidLoginError = (err: unknown): boolean =>
    !!err &&
    typeof err === 'object' &&
    ((err as { code?: string }).code === ErrorType.InvalidLogin ||
      /ACCESS_REFUSED/i.test((err as { message?: string }).message || ''))
  const toError = (value: unknown): Error =>
    value instanceof Error ? value : new Error(String(value))

  function AmqpOut(
    config: EditorNodeProperties & {
      exchangeRoutingKey: string
      exchangeRoutingKeyType: string
      amqpProperties: string
    },
  ): void {
    let isShuttingDown = false

    // @ts-expect-error Node-RED creates the node instance.
    RED.nodes.createNode(this, config)
    this.status(NODE_STATUS.Disconnected)

    const amqp = new Amqp(
      RED,
      this,
      config as AmqpInNodeDefaults & AmqpOutNodeDefaults,
    )

    const initialize = async () => {
      if (isShuttingDown) {
        return
      }

      await amqp.initialize()
      amqp.markConnected()
    }
    amqp.onRecovery(initialize)

    this.on(
      'input',
      async (
        msg: NodeMessageInFlow & {
          routingKey?: string
          vhost?: string
          properties?: MessageProperties
        },
        _: unknown,
        done?: (err?: Error) => void,
      ) => {
        const { payload, routingKey, vhost, properties: msgProperties } = msg
        let properties: MessageProperties
        try {
          properties = {
            ...JSON.parse(config.amqpProperties),
            ...msgProperties,
          }
        } catch {
          properties = msgProperties
        }

        try {
          switch (config.exchangeRoutingKeyType) {
            case 'msg':
            case 'flow':
            case 'global':
              amqp.setRoutingKey(
                RED.util.evaluateNodeProperty(
                  config.exchangeRoutingKey,
                  config.exchangeRoutingKeyType,
                  this,
                  msg,
                ),
              )
              break
            case 'jsonata': {
              const expression = RED.util.prepareJSONataExpression(
                config.exchangeRoutingKey,
                this,
              )
              const result = await new Promise<unknown>((resolve, reject) =>
                RED.util.evaluateJSONataExpression(
                  expression,
                  msg,
                  (error, value) => (error ? reject(error) : resolve(value)),
                ),
              )
              amqp.setRoutingKey(String(result))
              break
            }
            default:
              if (routingKey) {
                amqp.setRoutingKey(routingKey)
              }
          }

          if (vhost) {
            await amqp.setVhost(vhost)
          }
          await amqp.publish(payload, properties)
          done?.()
        } catch (error) {
          done?.(toError(error))
        }
      },
    )

    this.on('close', async (removedOrDone, doneMaybe) => {
      const removed = typeof removedOrDone === 'boolean' && removedOrDone
      const done =
        typeof removedOrDone === 'function' ? removedOrDone : doneMaybe
      isShuttingDown = true
      try {
        await amqp.close()
        done?.()
      } catch (error) {
        done?.(toError(error))
      } finally {
        if (removed) {
          amqp.removeBrokerNodeState()
        }
      }
    })

    void (async () => {
      await amqp.connect()
      await initialize()
    })().catch(async error => {
      await amqp.close().catch(() => undefined)
      if (isShuttingDown) {
        return
      }
      this.status(
        isInvalidLoginError(error) ? NODE_STATUS.Invalid : NODE_STATUS.Error,
      )
      this.error(`AmqpOut() Could not connect to broker ${error}`, {
        payload: { error, location: ErrorLocationEnum.ConnectError },
      })
    })
  }

  // @ts-expect-error Node-RED's registration types do not accept this constructor.
  RED.nodes.registerType(NodeType.AmqpOut, AmqpOut)
}
