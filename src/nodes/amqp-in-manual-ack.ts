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
  type AssembledMessage,
  type ManualAckFields,
  ManualAckType,
  NodeType,
} from '../types'

module.exports = (RED: NodeRedApp): void => {
  function AmqpInManualAck(config: EditorNodeProperties): void {
    // @ts-expect-error Node-RED supplies the runtime node instance.
    RED.nodes.createNode(this, config)
    const amqp = new Amqp(
      RED,
      this,
      config as AmqpInNodeDefaults & AmqpOutNodeDefaults,
    )
    startNode(this, amqp)
    const inputListener = async (
      msg: NodeMessageInFlow &
        Partial<AssembledMessage> & {
          manualAck?: ManualAckFields
          payload?: { reconnectCall?: boolean }
        },
      _: unknown,
      done?: (err?: Error) => void,
    ) => {
      // handle manual reconnect control message
      if (msg.payload?.reconnectCall) {
        try {
          await amqp.reconnect()
          done && done()
        } catch (e) {
          done && done(toError(e))
        }
        return
      }

      const assembledMsg = msg as AssembledMessage
      // handle manualAck
      if (msg.manualAck) {
        const ackMode = msg.manualAck.ackMode

        switch (ackMode) {
          case ManualAckType.AckAll:
            amqp.ackAll()
            break
          case ManualAckType.Nack:
            amqp.nack(assembledMsg)
            break
          case ManualAckType.NackAll:
            amqp.nackAll(assembledMsg)
            break
          case ManualAckType.Reject:
            amqp.reject(assembledMsg)
            break
          case ManualAckType.Ack:
          default:
            amqp.ack(assembledMsg)
            break
        }
      } else {
        amqp.ack(assembledMsg)
      }

      done && done()
    }
    this.on('input', inputListener)
  }
  // @ts-expect-error Node-RED supplies the runtime node instance.
  RED.nodes.registerType(NodeType.AmqpInManualAck, AmqpInManualAck)
}
