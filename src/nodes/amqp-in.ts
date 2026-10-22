import type { Node, NodeAPI, NodeMessageInFlow } from 'node-red'
import { createAmqpNode, toError } from '../node-lifecycle'
import { type AmqpNodeConfig, NodeType } from '../types'

module.exports = (RED: NodeAPI): void => {
  function AmqpIn(this: Node, config: AmqpNodeConfig): void {
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
  RED.nodes.registerType(NodeType.AmqpIn, AmqpIn)
}
