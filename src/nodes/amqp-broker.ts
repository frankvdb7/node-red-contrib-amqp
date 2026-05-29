import { NodeRedApp } from 'node-red'
import { AmqpBrokerNode, BrokerNodeState, NodeType } from '../types'

type ConfiguredAmqpNode = {
  id: string
  type: string
  broker?: string
}

module.exports = function (RED: NodeRedApp): void {
  const brokerNodes: AmqpBrokerNode[] = []
  const amqpNodeTypes = new Set<string>([
    NodeType.AmqpIn,
    NodeType.AmqpInManualAck,
    NodeType.AmqpOut,
  ])

  function AmqpBroker(this: AmqpBrokerNode, n): void {
    // wtf happened to the types?
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    RED.nodes.createNode(this, n)
    this.name = n.name
    this.host = n.host
    this.port = n.port
    this.tls = n.tls
    this.vhost = n.vhost
    this.credsFromSettings = n.credsFromSettings
    this.nodeStates = n.nodeStates || {}
    this.lastError = n.lastError || {}
    brokerNodes.push(this)

    this.on('close', () => {
      const index = brokerNodes.indexOf(this)
      /* istanbul ignore else */
      if (index > -1) {
        brokerNodes.splice(index, 1)
      }
    })
  }
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  RED.nodes.registerType('amqp-broker', AmqpBroker, {
    credentials: {
      username: { type: 'text' },
      password: { type: 'password' },
    },
  })

  RED.httpAdmin.get('/amqp-broker/health', (_req, res) => {
    const brokerStatuses = brokerNodes.map(brokerNode => {
      const states = getEffectiveNodeStates(brokerNode)
      const stateValues = Object.values(states)
      const uniqueStates = new Set(stateValues)
      const status: BrokerNodeState = stateValues.length > 0 && stateValues.every(state => state === 'connected')
        ? 'connected'
        : uniqueStates.has('errored')
        ? 'errored'
        : 'disconnected'

      const brokerStatus: {
        id: string
        name: string
        status: BrokerNodeState
        lastError?: AmqpBrokerNode['lastError']
      } = {
        id: brokerNode.id,
        name: brokerNode.name,
        status,
      }

      const hasLastError = Object.keys(brokerNode.lastError || {}).length > 0
      if (hasLastError) {
        brokerStatus.lastError = brokerNode.lastError
      }

      return brokerStatus
    })

    const hasBrokers = brokerStatuses.length > 0
    const allConnected = hasBrokers && brokerStatuses.every(b => b.status === 'connected')

    const statusCode = allConnected ? 200 : 503
    const response = {
      overallStatus: allConnected ? 'healthy' : 'unhealthy',
      brokers: brokerStatuses,
    }

    res.status(statusCode).json(response)
  })

  function getEffectiveNodeStates(
    brokerNode: AmqpBrokerNode,
  ): Record<string, BrokerNodeState> {
    const states: Record<string, BrokerNodeState> = {
      ...(brokerNode.nodeStates || {}),
    }
    const nodes = RED.nodes as unknown as {
      eachNode?: (callback: (node: ConfiguredAmqpNode) => void) => void
    }

    nodes.eachNode?.(node => {
      if (node.broker === brokerNode.id && amqpNodeTypes.has(node.type)) {
        states[node.id] = states[node.id] || 'disconnected'
      }
    })

    return states
  }
}
