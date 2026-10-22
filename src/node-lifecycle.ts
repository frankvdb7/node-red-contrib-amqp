import type { Node } from 'node-red'
import type Amqp from './Amqp'
import { NODE_STATUS } from './constants'

export const toError = (value: unknown): Error =>
  value instanceof Error ? value : new Error(String(value))

export function startNode(node: Node, amqp: Amqp): void {
  let closing = false
  node.status(NODE_STATUS.Disconnected)
  node.on('close', async (removed: boolean, done: (error?: Error) => void) => {
    closing = true
    try {
      await amqp.close()
      done()
    } catch (error) {
      done(toError(error))
    } finally {
      if (removed) amqp.removeBrokerNodeState()
    }
  })
  void amqp.connect().catch(error => {
    if (!closing) {
      node.status(NODE_STATUS.Error)
      node.error(`Could not initialize AMQP: ${error}`)
    }
  })
}
