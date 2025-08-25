import { NodeRedApp, EditorNodeProperties } from 'node-red'
import { NODE_STATUS } from '../constants'
import { AmqpInNodeDefaults, AmqpOutNodeDefaults, ErrorType, NodeType, ErrorLocationEnum } from '../types'
import Amqp from '../Amqp'

module.exports = function (RED: NodeRedApp): void {
  function AmqpIn(config: EditorNodeProperties): void {
    let reconnectTimeout: NodeJS.Timeout
    let reconnect = null;
    let connection = null;
    let channel = null;
    let onConnClose: (e: unknown) => Promise<void>
    let onConnError: (e: unknown) => Promise<void>
    let onChannelClose: () => Promise<void>
    let onChannelError: (e: unknown) => Promise<void>


    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    RED.nodes.createNode(this, config)
    this.status(NODE_STATUS.Disconnected)

    const configAmqp: AmqpInNodeDefaults & AmqpOutNodeDefaults = config;

    const amqp = new Amqp(RED, this, configAmqp)

    const reconnectOnError = configAmqp.reconnectOnError;

    const inputListener = async (msg, _, done) => {
      if (msg.payload && msg.payload.reconnectCall && typeof reconnect === 'function') {
        await reconnect()
        done && done()
      } else {
        done && done()
      }
    }

    // receive input reconnectCall
    this.on('input', inputListener)
    // When the node is re-deployed
    this.on('close', async (done: () => void): Promise<void> => {
      clearTimeout(reconnectTimeout)
      removeEventListeners()
      await amqp.close()
      done && done()
    })
    

    const removeEventListeners = (): void => {
      connection?.off?.('close', onConnClose)
      connection?.off?.('error', onConnError)
      channel?.off?.('close', onChannelClose)
      channel?.off?.('error', onChannelError)
    }

    async function initializeNode(nodeIns) {
      reconnect = async () => {
        removeEventListeners()
        await amqp.close()
        channel = null
        connection = null

        // always clear timer before set it;
        clearTimeout(reconnectTimeout)
        reconnectTimeout = setTimeout(() => {
          try {
            initializeNode(nodeIns)
          } catch (e) {
            reconnect()
          }
        }, 2000)
      }

      try {
        connection = await amqp.connect()

        // istanbul ignore else
        if (connection) {
          channel = await amqp.initialize()
          await amqp.consume()

          onConnClose = async e => {
            e && (await reconnect())
          }

          onConnError = async e => {
            reconnectOnError && (await reconnect())
            nodeIns.error(`Connection error ${e}`, {
              payload: { error: e, location: ErrorLocationEnum.ConnectionErrorEvent },
            })
          }

          onChannelClose = async () => {
            // original code didn't reconnect; we keep same behavior
          }

          onChannelError = async e => {
            reconnectOnError && (await reconnect())
            nodeIns.error(`Channel error ${e}`, {
              payload: { error: e, location: ErrorLocationEnum.ChannelErrorEvent },
            })
          }

          connection.on('close', onConnClose)
          connection.on('error', onConnError)
          channel.on('close', onChannelClose)
          channel.on('error', onChannelError)

          nodeIns.status(NODE_STATUS.Connected)
        }
      } catch (e) {
        if (
          e.code === ErrorType.InvalidLogin ||
          /ACCESS_REFUSED/i.test(e.message || '')
        ) {
          nodeIns.status(NODE_STATUS.Invalid)
          nodeIns.error(`AmqpIn() Could not connect to broker ${e}`, { payload: { error: e, location: ErrorLocationEnum.ConnectError } })
        } else if (
          e.code === ErrorType.ConnectionRefused ||
          e.code === ErrorType.DnsResolve ||
          e.code === ErrorType.HostNotFound ||
          e.isOperational
        ) {
          reconnectOnError && (await reconnect())
        } else {
          nodeIns.status(NODE_STATUS.Error)
          nodeIns.error(`AmqpIn() ${JSON.stringify(e)}`, { payload: { error: e, location: ErrorLocationEnum.ConnectError } })
        }
      }
    }

    // call
    initializeNode(this);
  }
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  RED.nodes.registerType(NodeType.AmqpIn, AmqpIn)
}
