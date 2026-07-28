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
  type AssembledMessage,
  ErrorLocationEnum,
  ErrorType,
  type ManualAckFields,
  ManualAckType,
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

  function AmqpInManualAck(config: EditorNodeProperties): void {
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
      await amqp.consume()
      amqp.markConnected()
    }
    amqp.onRecovery(initialize)

    this.on(
      'input',
      (
        msg: NodeMessageInFlow &
          Partial<AssembledMessage> & { manualAck?: ManualAckFields },
        _: unknown,
        done?: (err?: Error) => void,
      ) => {
        const assembledMessage = msg as AssembledMessage
        switch (msg.manualAck?.ackMode) {
          case ManualAckType.AckAll:
            amqp.ackAll()
            break
          case ManualAckType.Nack:
            amqp.nack(assembledMessage)
            break
          case ManualAckType.NackAll:
            amqp.nackAll(assembledMessage)
            break
          case ManualAckType.Reject:
            amqp.reject(assembledMessage)
            break
          case ManualAckType.Ack:
          default:
            amqp.ack(assembledMessage)
        }
        done?.()
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
      this.error(`AmqpInManualAck() Could not connect to broker ${error}`, {
        payload: { error, location: ErrorLocationEnum.ConnectError },
      })
    })
  }

  // @ts-expect-error Node-RED's registration types do not accept this constructor.
  RED.nodes.registerType(NodeType.AmqpInManualAck, AmqpInManualAck)
}
