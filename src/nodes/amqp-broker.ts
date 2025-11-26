import { NodeRedApp } from 'node-red'
import { AmqpBrokerNode } from '../types'

module.exports = function (RED: NodeRedApp): void {
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
    this.connections = n.connections || {}
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
    const brokerStatuses: { id: string; name: string; status: string }[] = []
    let allConnected = true

    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    RED.nodes.eachNode(n => {
      if (n.type === 'amqp-broker') {
        const brokerNode = n as AmqpBrokerNode
        const isConnected = Object.values(brokerNode.connections).some(
          status => status === true,
        )
        const status = isConnected ? 'connected' : 'disconnected'

        if (!isConnected) {
          allConnected = false
        }

        brokerStatuses.push({
          id: brokerNode.id,
          name: brokerNode.name,
          status,
        })
      }
    })

    const statusCode = allConnected ? 200 : 503
    const response = {
      overallStatus: allConnected ? 'healthy' : 'unhealthy',
      brokers: brokerStatuses,
    }

    res.status(statusCode).json(response)
  })
}
