import { NodeRedApp, EditorNodeProperties, NodeMessageInFlow } from 'node-red'
import { NODE_STATUS } from '../constants'
import { AmqpInNodeDefaults, AmqpOutNodeDefaults, NodeType } from '../types'
import Amqp from '../Amqp'
import createAmqpInputLifecycle from './amqp-input-lifecycle'

module.exports = function (RED: NodeRedApp): void {
  const toError = (value: unknown): Error =>
    value instanceof Error ? value : new Error(String(value))

  function AmqpIn(config: EditorNodeProperties): void {
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
      nodeName: 'AmqpIn()',
      registerInput: lifecycle => {
        node.on(
          'input',
          async (
            msg: NodeMessageInFlow & { payload?: { reconnectCall?: boolean } },
            _: unknown,
            done?: (error?: Error) => void,
          ) => {
            if (msg.payload?.reconnectCall) {
              await lifecycle
                .reconnect()
                .then(() => done?.())
                .catch(error => done?.(toError(error)))
            } else {
              done?.()
            }
          },
        )
      },
    })
  }

  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  RED.nodes.registerType(NodeType.AmqpIn, AmqpIn)
}
