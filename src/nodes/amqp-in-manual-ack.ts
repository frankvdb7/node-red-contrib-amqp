import type { Node, NodeAPI, NodeMessageInFlow } from 'node-red'
import { createAmqpNode, toError } from '../node-lifecycle'
import {
  type AmqpNodeConfig,
  type AssembledMessage,
  type ManualAckFields,
  ManualAckType,
  NodeType,
} from '../types'

export = (RED: NodeAPI): void => {
  function AmqpInManualAck(this: Node, config: AmqpNodeConfig): void {
    const amqp = createAmqpNode(RED, this, config)
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
      try {
        if (msg.manualAck) {
          switch (msg.manualAck.ackMode) {
            case ManualAckType.AckAll:
              amqp.ackAll(assembledMsg)
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
        done?.()
      } catch (error) {
        // A failed ack is a failed Node-RED message, not a successful completion.
        if (done) done(toError(error))
        else this.error(toError(error), msg)
      }
    }
    this.on('input', inputListener)
  }
  RED.nodes.registerType(NodeType.AmqpInManualAck, AmqpInManualAck)
}
