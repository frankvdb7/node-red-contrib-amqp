import type { NodeAPI } from 'node-red'
import {
  type AmqpBrokerNode,
  type AmqpBrokerNodeDef,
  type BrokerNodeState,
  NodeType,
} from '../types'

module.exports = (RED: NodeAPI): void => {
  const brokerNodes: AmqpBrokerNode[] = []
  const amqpNodeTypes = new Set<string>([
    NodeType.AmqpIn,
    NodeType.AmqpInManualAck,
    NodeType.AmqpOut,
  ])

  function AmqpBroker(this: AmqpBrokerNode, n: AmqpBrokerNodeDef): void {
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
      if (index > -1) {
        brokerNodes.splice(index, 1)
      }
    })
  }
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
      const status: BrokerNodeState =
        stateValues.length > 0 &&
        stateValues.every(state => state === 'connected')
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

      const activeLastError = getActiveLastError(brokerNode, states)
      const hasLastError = Object.keys(activeLastError).length > 0
      if (hasLastError) {
        brokerStatus.lastError = activeLastError
      }

      return brokerStatus
    })

    const hasBrokers = brokerStatuses.length > 0
    const allConnected =
      hasBrokers && brokerStatuses.every(b => b.status === 'connected')

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
    const states: Record<string, BrokerNodeState> = {}
    RED.nodes.eachNode(node => {
      if (
        'broker' in node &&
        node.broker === brokerNode.id &&
        amqpNodeTypes.has(node.type)
      ) {
        states[node.id] = brokerNode.nodeStates?.[node.id] || 'disconnected'
      }
    })

    return states
  }

  function getActiveLastError(
    brokerNode: AmqpBrokerNode,
    states: Record<string, BrokerNodeState>,
  ): AmqpBrokerNode['lastError'] {
    const source = brokerNode.lastError || {}
    const filtered: AmqpBrokerNode['lastError'] = {}

    Object.keys(states).forEach(nodeId => {
      if (source[nodeId]) {
        filtered[nodeId] = source[nodeId]
      }
    })

    return filtered
  }
}
