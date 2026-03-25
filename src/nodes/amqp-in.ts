import { NodeRedApp, EditorNodeProperties, Node, NodeMessageInFlow } from 'node-red'
import { Channel, ChannelModel } from 'amqplib'
import { NODE_STATUS } from '../constants'
import { AmqpInNodeDefaults, AmqpOutNodeDefaults, ErrorType, NodeType, ErrorLocationEnum } from '../types'
import Amqp from '../Amqp'

module.exports = function (RED: NodeRedApp): void {
  const isErrorLike = (
    value: unknown,
  ): value is { code?: string; message?: string; isOperational?: boolean } =>
    typeof value === 'object' && value !== null
  const toError = (value: unknown): Error =>
    value instanceof Error ? value : new Error(String(value))

  function AmqpIn(config: EditorNodeProperties): void {
    let reconnectTimeout: NodeJS.Timeout
    let reconnect: (() => Promise<void>) | null = null
    let reconnectScheduled = false
    let isShuttingDown = false
    let connection: ChannelModel | null = null
    let channel: Channel | null = null
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

    const inputListener = async (
      msg: NodeMessageInFlow & { payload?: { reconnectCall?: boolean } },
      _: unknown,
      done?: (err?: Error) => void,
    ) => {
      if (msg.payload && msg.payload.reconnectCall && typeof reconnect === 'function') {
        try {
          await reconnect()
          done && done()
        } catch (e) {
          done && done(toError(e))
        }
      } else {
        done && done()
      }
    }

    // receive input reconnectCall
    this.on('input', inputListener)
    // When the node is re-deployed
    this.on('close', async (done: (err?: Error) => void): Promise<void> => {
      isShuttingDown = true
      clearTimeout(reconnectTimeout)
      removeEventListeners()
      try {
        await amqp.close()
        done && done()
      } catch (e) {
        done && done(toError(e))
      }
    })
    

    const removeEventListeners = (): void => {
      if (typeof onConnClose === 'function') {
        connection?.off?.('close', onConnClose)
      }
      if (typeof onConnError === 'function') {
        connection?.off?.('error', onConnError)
      }
      if (typeof onChannelClose === 'function') {
        channel?.off?.('close', onChannelClose)
      }
      if (typeof onChannelError === 'function') {
        channel?.off?.('error', onChannelError)
      }
    }

    async function initializeNode(nodeIns: Node) {
      reconnect = async () => {
        if (isShuttingDown || reconnectScheduled) {
          if (isShuttingDown) {
            nodeIns.log('Reconnect skipped: node is shutting down')
          }
          return
        }
        reconnectScheduled = true
        clearTimeout(reconnectTimeout)
        try {
          nodeIns.log('Reconnect requested: closing AMQP resources')
          removeEventListeners()
          await amqp.close()
          channel = null
          connection = null

          nodeIns.log('Reconnect scheduled in 2000ms')
          reconnectTimeout = setTimeout(() => {
            reconnectScheduled = false
            if (isShuttingDown) {
              nodeIns.log('Reconnect timer fired but node is shutting down')
              return
            }
            nodeIns.log('Reconnect timer fired: re-initializing AMQP node')
            void initializeNode(nodeIns).catch(() => {
              if (typeof reconnect === 'function') {
                nodeIns.warn('Reconnect attempt failed during initialization; retrying')
                void reconnect()
              }
            })
          }, 2000)
        } catch (error) {
          reconnectScheduled = false
          throw error
        }
      }

      try {
        connection = await amqp.connect()

        // istanbul ignore else
        if (connection) {
          channel = await amqp.initialize()
          await amqp.consume()

          onConnClose = async () => {
            nodeIns.warn('AMQP connection closed event received')
            try {
              await reconnect()
            } catch (reconnectError) {
              nodeIns.error(`Reconnect failed after connection close: ${reconnectError}`, {
                payload: { error: reconnectError, location: ErrorLocationEnum.ConnectionErrorEvent },
              })
            }
          }

          onConnError = async e => {
            if (reconnectOnError) {
              try {
                await reconnect()
              } catch (reconnectError) {
                nodeIns.error(`Reconnect failed after connection error: ${reconnectError}`, {
                  payload: { error: reconnectError, location: ErrorLocationEnum.ConnectionErrorEvent },
                })
              }
            }
            nodeIns.error(`Connection error ${e}`, {
              payload: { error: e, location: ErrorLocationEnum.ConnectionErrorEvent },
            })
          }

          onChannelClose = async () => {
            nodeIns.warn('AMQP channel closed event received')
            try {
              await reconnect()
            } catch (reconnectError) {
              nodeIns.error(`Reconnect failed after channel close: ${reconnectError}`, {
                payload: { error: reconnectError, location: ErrorLocationEnum.ChannelErrorEvent },
              })
            }
          }

          onChannelError = async e => {
            if (reconnectOnError) {
              try {
                await reconnect()
              } catch (reconnectError) {
                nodeIns.error(`Reconnect failed after channel error: ${reconnectError}`, {
                  payload: { error: reconnectError, location: ErrorLocationEnum.ChannelErrorEvent },
                })
              }
            }
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
      } catch (e: unknown) {
        await amqp.close().catch(() => undefined)
        const err = isErrorLike(e) ? e : {}
        if (
          err.code === ErrorType.InvalidLogin ||
          /ACCESS_REFUSED/i.test(err.message || '')
        ) {
          nodeIns.status(NODE_STATUS.Invalid)
          nodeIns.error(`AmqpIn() Could not connect to broker ${e}`, { payload: { error: e, location: ErrorLocationEnum.ConnectError } })
        } else if (
          err.code === ErrorType.ConnectionRefused ||
          err.code === ErrorType.DnsResolve ||
          err.code === ErrorType.HostNotFound ||
          err.isOperational
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
