import { NodeRedApp } from 'node-red'
import { AmqpBrokerNode } from '../types'

module.exports = function (RED: NodeRedApp): void {
  const brokerNodes: AmqpBrokerNode[] = []

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
      const isConnected = Object.values(brokerNode.connections || {}).some(
        status => status === true,
      )
      return {
        id: brokerNode.id,
        name: brokerNode.name,
        status: isConnected ? 'connected' : 'disconnected',
      }
    })

    const allConnected = brokerStatuses.every(b => b.status === 'connected')

    const statusCode = allConnected ? 200 : 503
    const response = {
      overallStatus: allConnected ? 'healthy' : 'unhealthy',
      brokers: brokerStatuses,
    }

    res.status(statusCode).json(response)
  })
}
