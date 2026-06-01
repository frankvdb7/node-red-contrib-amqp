import { NodeRedApp, EditorNodeProperties, Node, NodeMessageInFlow } from 'node-red'
import { Channel, ChannelModel } from 'amqplib'
import { NODE_STATUS } from '../constants'
import {
  ErrorType,
  NodeType,
  ManualAckType,
  ManualAckFields,
  AmqpOutNodeDefaults,
  AmqpInNodeDefaults,
  ErrorLocationEnum,
  AssembledMessage,
} from '../types'
import Amqp from '../Amqp'
import ReconnectBackoff from '../reconnect-backoff'

module.exports = function (RED: NodeRedApp): void {
  const isErrorLike = (
    value: unknown,
  ): value is { code?: string; message?: string; isOperational?: boolean } =>
    typeof value === 'object' && value !== null
  const isInvalidLoginError = (
    err: { code?: string; message?: string },
  ): boolean =>
    err.code === ErrorType.InvalidLogin || /ACCESS_REFUSED/i.test(err.message || '')
  const toError = (value: unknown): Error =>
    value instanceof Error ? value : new Error(String(value))

  function AmqpInManualAck(config: EditorNodeProperties): void {
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
    let onConsumerCancelled: () => Promise<void>
    const me = this
    const reconnectBackoff = new ReconnectBackoff()
    const nodeEmitter = me as unknown as {
      on?: (event: string, listener: (...args: unknown[]) => void) => void
      off?: (event: string, listener: (...args: unknown[]) => void) => void
    }


    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    RED.nodes.createNode(this, config)
    this.status(NODE_STATUS.Disconnected)

    const configAmqp: AmqpInNodeDefaults & AmqpOutNodeDefaults = config;

    const amqp = new Amqp(RED, this, configAmqp)

    const reconnectOnError = configAmqp.reconnectOnError;

    const inputListener = async (
      msg: NodeMessageInFlow &
        Partial<AssembledMessage> & {
          manualAck?: ManualAckFields
          payload?: { reconnectCall?: boolean }
        },
      _: unknown,
      done?: (err?: Error) => void,
    ) => {
      // handle manual reconnect control message
      if (msg.payload && msg.payload.reconnectCall && typeof reconnect === 'function') {
        try {
          await reconnect()
          done && done()
        } catch (e) {
          done && done(toError(e))
        }
        return
      }

      const assembledMsg = msg as AssembledMessage
      // handle manualAck
      if (msg.manualAck) {
        const ackMode = msg.manualAck.ackMode

        switch (ackMode) {
          case ManualAckType.AckAll:
            amqp.ackAll()
            break
          case ManualAckType.Nack:
            amqp.nack(assembledMsg)
            break
          case ManualAckType.NackAll:
            amqp.nackAll(assembledMsg)
            break
          case ManualAckType.Reject:
            amqp.reject(assembledMsg)
            break
          case ManualAckType.Ack:
          default:
            amqp.ack(assembledMsg)
            break
        }
      } else {
        amqp.ack(assembledMsg)
      }

      done && done()
    }
    // receive input reconnectCall
    this.on('input', inputListener)
    // When the server goes down
    this.on('close', async (removedOrDone: boolean | ((err?: Error) => void), doneMaybe?: (err?: Error) => void): Promise<void> => {
      const removed = typeof removedOrDone === 'boolean' ? removedOrDone : false
      const done = typeof removedOrDone === 'function' ? removedOrDone : doneMaybe
      isShuttingDown = true
      clearTimeout(reconnectTimeout)
      removeEventListeners()
      try {
        await amqp.close()
        if (removed) {
          amqp.removeBrokerNodeState()
        }
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
      if (typeof onConsumerCancelled === 'function') {
        nodeEmitter.off?.('amqp:consumer-cancelled', onConsumerCancelled)
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
          if (isShuttingDown) {
            reconnectScheduled = false
            nodeIns.log('Reconnect aborted: node started shutting down while closing AMQP resources')
            return
          }
          channel = null
          connection = null

          const reconnectDelayMs = reconnectBackoff.nextDelayMs()
          nodeIns.log(`Reconnect scheduled in ${reconnectDelayMs}ms`)
          reconnectTimeout = setTimeout(() => {
            reconnectScheduled = false
            if (isShuttingDown) {
              nodeIns.log('Reconnect timer fired but node is shutting down')
              return
            }
            nodeIns.log('Reconnect timer fired: re-initializing AMQP node')
            void initializeNode(nodeIns)
          }, reconnectDelayMs)
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
          if (isShuttingDown) {
            await amqp.close().catch(() => undefined)
            return
          }

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
            nodeIns.error(`Connection error ${e}`, { payload: { error: e, location: ErrorLocationEnum.ConnectionErrorEvent } })
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
            nodeIns.error(`Channel error ${e}`, { payload: { error: e, location: ErrorLocationEnum.ChannelErrorEvent } })
          }

          onConsumerCancelled = async () => {
            nodeIns.warn('AMQP consumer cancelled event received')
            try {
              await reconnect()
            } catch (reconnectError) {
              nodeIns.error(`Reconnect failed after consumer cancellation: ${reconnectError}`, {
                payload: { error: reconnectError, location: ErrorLocationEnum.ChannelErrorEvent },
              })
            }
          }

          connection.on('close', onConnClose)
          connection.on('error', onConnError)
          channel.on('close', onChannelClose)
          channel.on('error', onChannelError)
          nodeEmitter.on?.('amqp:consumer-cancelled', onConsumerCancelled)

          amqp.markConnected()
          reconnectBackoff.reset()
        }
      } catch (e: unknown) {
        await amqp.close().catch(() => undefined)
        if (isShuttingDown) {
          return
        }
        const err = isErrorLike(e) ? e : {}
        if (isInvalidLoginError(err)) {
          nodeIns.status(NODE_STATUS.Invalid)
          nodeIns.error(`AmqpInManualAck() Could not connect to broker ${e}`, { payload: { error: e, location: ErrorLocationEnum.ConnectError } })
          if (reconnectOnError) {
            let reconnectFailed = false
            await reconnect().catch(reconnectError => {
              reconnectFailed = true
              nodeIns.status(NODE_STATUS.Error)
              nodeIns.error(`Reconnect failed during initialization: ${reconnectError}`, {
                payload: { error: reconnectError, location: ErrorLocationEnum.ConnectError },
              })
            })
            if (!reconnectFailed) {
              nodeIns.status(NODE_STATUS.Invalid)
            }
          }
        } else {
          nodeIns.error(`AmqpInManualAck() ${e}`, {
            payload: { error: e, location: ErrorLocationEnum.ConnectError },
          })
          if (reconnectOnError) {
            await reconnect().catch(reconnectError => {
              nodeIns.status(NODE_STATUS.Error)
              nodeIns.error(`Reconnect failed during initialization: ${reconnectError}`, {
                payload: { error: reconnectError, location: ErrorLocationEnum.ConnectError },
              })
            })
          } else {
            nodeIns.status(NODE_STATUS.Error)
          }
        }
      }
    }

    // call
    initializeNode(this);
  }
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  RED.nodes.registerType(NodeType.AmqpInManualAck, AmqpInManualAck)
}
