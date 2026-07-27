import { NodeRedApp, EditorNodeProperties, NodeMessageInFlow } from 'node-red'
import { NODE_STATUS } from '../constants'
import {
  NodeType,
  ManualAckType,
  ManualAckFields,
  AmqpOutNodeDefaults,
  AmqpInNodeDefaults,
  AssembledMessage,
} from '../types'
import Amqp from '../Amqp'
import createAmqpInputLifecycle from './amqp-input-lifecycle'

module.exports = function (RED: NodeRedApp): void {
  const toError = (value: unknown): Error =>
    value instanceof Error ? value : new Error(String(value))

  function AmqpInManualAck(config: EditorNodeProperties): void {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    RED.nodes.createNode(this, config)
    this.status(NODE_STATUS.Disconnected)

    const node = this
    const configAmqp: AmqpInNodeDefaults & AmqpOutNodeDefaults = config
    const amqp = new Amqp(RED, node, configAmqp)
    createAmqpInputLifecycle({
      RED,
      node,
      config: configAmqp,
      amqp,
      nodeName: 'AmqpInManualAck()',
      registerInput: lifecycle => {
        node.on(
          'input',
          async (
            msg: NodeMessageInFlow &
              Partial<AssembledMessage> & {
                manualAck?: ManualAckFields
                payload?: { reconnectCall?: boolean }
              },
            _: unknown,
            done?: (error?: Error) => void,
          ) => {
            const isAmqpDelivery = typeof msg.fields?.deliveryTag === 'number'
            if (!isAmqpDelivery && msg.payload?.reconnectCall) {
              await lifecycle
                .reconnect()
                .then(() => done?.())
                .catch(error => done?.(toError(error)))
              return
            }

            const assembledMessage = msg as AssembledMessage
            let settled: boolean
            switch (msg.manualAck?.ackMode) {
              case ManualAckType.AckAll:
                settled = amqp.ackAll(assembledMessage)
                break
              case ManualAckType.Nack:
                settled = amqp.nack(assembledMessage)
                break
              case ManualAckType.NackAll:
                settled = amqp.nackAll(assembledMessage)
                break
              case ManualAckType.Reject:
                settled = amqp.reject(assembledMessage)
                break
              case ManualAckType.Ack:
              default:
                settled = amqp.ack(assembledMessage)
                break
            }
            done?.(settled ? undefined : new Error('Could not settle AMQP message'))
          },
        )
      },
    })
  }

  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  RED.nodes.registerType(NodeType.AmqpInManualAck, AmqpInManualAck)
}
