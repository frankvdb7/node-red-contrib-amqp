import type {
  EditorNodeProperties,
  NodeMessageInFlow,
  NodeRedApp,
} from 'node-red'
import { createAmqpNode, toError } from '../node-lifecycle'
import { NodeType } from '../types'

module.exports = (RED: NodeRedApp): void => {
  function AmqpIn(config: EditorNodeProperties): void {
    const amqp = createAmqpNode(RED, this, config)
    const inputListener = async (
      msg: NodeMessageInFlow & { payload?: { reconnectCall?: boolean } },
      _: unknown,
      done?: (err?: Error) => void,
    ) => {
      if (msg.payload?.reconnectCall) {
        try {
          await amqp.reconnect()
          done && done()
        } catch (e) {
          done && done(toError(e))
        }
      } else {
        done && done()
      }
    }

    this.on('input', inputListener)
  }
  // @ts-expect-error Node-RED supplies the runtime node instance.
  RED.nodes.registerType(NodeType.AmqpIn, AmqpIn)
}
