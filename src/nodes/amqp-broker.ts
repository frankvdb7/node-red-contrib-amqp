import type { NodeAPI } from 'node-red'
import {
  type AmqpBrokerNode,
  type AmqpBrokerNodeDef,
  type BrokerNodeState,
  NodeType,
} from '../types'

type EffectiveNodeData = {
  brokerNode: AmqpBrokerNode
  states: Record<string, BrokerNodeState>
  publishBlocked: AmqpBrokerNode['publishBlocked']
}

export = (RED: NodeAPI): void => {
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
    this.publishBlocked = {}
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
    const effectiveNodeData = getEffectiveNodeData()
    const brokerStatuses = effectiveNodeData.map(data => {
      const { brokerNode, states, publishBlocked } = data
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
        publishBlocked?: AmqpBrokerNode['publishBlocked']
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

      if (Object.keys(publishBlocked).length > 0) {
        brokerStatus.publishBlocked = publishBlocked
      }

      return brokerStatus
    })

    const hasBrokers = brokerStatuses.length > 0
    const allConnected =
      hasBrokers && brokerStatuses.every(b => b.status === 'connected')
    const hasBlockedPublishers = brokerStatuses.some(
      broker => Object.keys(broker.publishBlocked || {}).length > 0,
    )

    const healthy = allConnected && !hasBlockedPublishers
    const statusCode = healthy ? 200 : 503
    const response = {
      overallStatus: healthy
        ? 'healthy'
        : allConnected && hasBlockedPublishers
          ? 'degraded'
          : 'unhealthy',
      brokers: brokerStatuses,
    }

    res.status(statusCode).json(response)
  })

  function getEffectiveNodeData(): EffectiveNodeData[] {
    const effectiveNodeData: EffectiveNodeData[] = brokerNodes.map(
      brokerNode => ({
        brokerNode,
        states: {},
        publishBlocked: {},
      }),
    )
    const effectiveNodeDataByBrokerId = new Map(
      effectiveNodeData.map(data => [data.brokerNode.id, data] as const),
    )

    RED.nodes.eachNode(node => {
      if (!('broker' in node) || typeof node.broker !== 'string') return

      const data = effectiveNodeDataByBrokerId.get(node.broker)
      if (!data) return

      if (amqpNodeTypes.has(node.type)) {
        data.states[node.id] =
          data.brokerNode.nodeStates?.[node.id] || 'disconnected'
      }

      if (node.type === NodeType.AmqpOut) {
        const reason = data.brokerNode.publishBlocked?.[node.id]
        if (reason !== undefined) data.publishBlocked[node.id] = reason
      }
    })

    return effectiveNodeData
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
