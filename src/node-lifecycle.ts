import type { EditorNodeProperties, Node, NodeRedApp } from 'node-red'
import Amqp from './Amqp'
import { NODE_STATUS } from './constants'
import type { AmqpInNodeDefaults, AmqpOutNodeDefaults } from './types'

export const toError = (value: unknown): Error =>
  value instanceof Error ? value : new Error(String(value))

function startNode(node: Node, amqp: Amqp): void {
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

export function createAmqpNode(
  RED: NodeRedApp,
  node: Node,
  config: EditorNodeProperties,
): Amqp {
  // @ts-expect-error Node-RED supplies the runtime node instance.
  RED.nodes.createNode(node, config)
  const amqp = new Amqp(
    RED,
    node,
    config as AmqpInNodeDefaults & AmqpOutNodeDefaults,
  )
  startNode(node, amqp)
  return amqp
}
