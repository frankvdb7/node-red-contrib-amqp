import { NodeRedApp, EditorNodeProperties, Node, NodeMessageInFlow } from 'node-red'
import { Channel, ChannelModel } from 'amqplib'
import { NODE_STATUS } from '../constants'
import { AmqpInNodeDefaults, AmqpOutNodeDefaults, ErrorType, NodeType, ErrorLocationEnum } from '../types'
import Amqp from '../Amqp'
import ReconnectBackoff from '../reconnect-backoff'
import trackTerminalClose from './flow-close-tracker'

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

  function AmqpIn(config: EditorNodeProperties): void {
    let reconnectTimeout: NodeJS.Timeout
    let reconnect: ((continueUntilConnected?: boolean) => Promise<void>) | null = null
    let reconnectScheduled = false
    let recoveringFromDisconnect = false
    let isShuttingDown = false
    let initializationVersion = 0
    let initializationPromise: Promise<void> | null = null
    let initializationAbortController: AbortController | null = null
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
    const terminalClose = trackTerminalClose(RED, this)

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
    this.on('close', async (removedOrDone: boolean | ((err?: Error) => void), doneMaybe?: (err?: Error) => void): Promise<void> => {
      const removed = typeof removedOrDone === 'boolean' ? removedOrDone : false
      const done = typeof removedOrDone === 'function' ? removedOrDone : doneMaybe
      const removeBindings = terminalClose.shouldRemoveBindings(removed)
      terminalClose.dispose()
      isShuttingDown = true
      initializationVersion += 1
      initializationAbortController?.abort(
        new Error('AMQP initialization cancelled during shutdown'),
      )
      clearTimeout(reconnectTimeout)
      removeEventListeners()
      let closeError: unknown
      try {
        await amqp.close(removeBindings ? { removeBindings: true } : undefined)
      } catch (e) {
        closeError = e
      } finally {
        if (removed) {
          amqp.removeBrokerNodeState()
        }
      }

      if (closeError) {
        done && done(toError(closeError))
        return
      }

      done && done()
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
      const attemptVersion = initializationVersion
      const abortController = new AbortController()
      initializationAbortController = abortController
      const initializationIsStale = (): boolean =>
        isShuttingDown || attemptVersion !== initializationVersion

      reconnect = async (continueUntilConnected = false) => {
        recoveringFromDisconnect ||= continueUntilConnected
        if (isShuttingDown || reconnectScheduled) {
          if (isShuttingDown) {
            nodeIns.log('Reconnect skipped: node is shutting down')
          }
          return
        }
        initializationVersion += 1
        initializationAbortController?.abort(
          new Error('AMQP initialization cancelled for reconnect'),
        )
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
            if (isShuttingDown) {
              reconnectScheduled = false
              nodeIns.log('Reconnect timer fired but node is shutting down')
              return
            }
            nodeIns.log('Reconnect timer fired: re-initializing AMQP node')
            void queueInitialization(nodeIns, true)
          }, reconnectDelayMs)
        } catch (error) {
          reconnectScheduled = false
          throw error
        }
      }

      try {
        connection = await amqp.connect({ signal: abortController.signal })
        if (initializationIsStale()) {
          await amqp.close().catch(() => undefined)
          return
        }

        // istanbul ignore else
        if (connection) {
          channel = await amqp.initialize({
            signal: abortController.signal,
          })
          if (initializationIsStale()) {
            await amqp.close().catch(() => undefined)
            return
          }
          await amqp.consume()
          if (initializationIsStale()) {
            await amqp.close().catch(() => undefined)
            return
          }

          onConnClose = async () => {
            nodeIns.warn('AMQP connection closed event received')
            try {
              await reconnect(true)
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
              await reconnect(true)
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

          onConsumerCancelled = async () => {
            nodeIns.warn('AMQP consumer cancelled event received')
            try {
              await reconnect(true)
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
          recoveringFromDisconnect = false
          reconnectBackoff.reset()
        }
      } catch (e: unknown) {
        await amqp.close().catch(() => undefined)
        if (
          isShuttingDown ||
          attemptVersion !== initializationVersion
        ) {
          return
        }
        const err = isErrorLike(e) ? e : {}
        if (isInvalidLoginError(err)) {
          nodeIns.status(NODE_STATUS.Invalid)
          nodeIns.error(`AmqpIn() Could not connect to broker ${e}`, { payload: { error: e, location: ErrorLocationEnum.ConnectError } })
          if (reconnectOnError || recoveringFromDisconnect) {
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
          nodeIns.error(`AmqpIn() ${e}`, {
            payload: { error: e, location: ErrorLocationEnum.ConnectError },
          })
          if (reconnectOnError || recoveringFromDisconnect) {
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
      } finally {
        if (initializationAbortController === abortController) {
          initializationAbortController = null
        }
      }
    }

    function queueInitialization(nodeIns: Node, scheduledReconnect = false): Promise<void> {
      const previous = initializationPromise
      const queuedVersion = initializationVersion
      const startInitialization = (): Promise<void> => {
        if (scheduledReconnect) {
          reconnectScheduled = false
        }
        if (isShuttingDown || queuedVersion !== initializationVersion) {
          return Promise.resolve()
        }
        return initializeNode(nodeIns)
      }
      const operation = previous
        ? previous.catch(() => undefined).then(startInitialization)
        : startInitialization()
      initializationPromise = operation
      void operation
        .finally(() => {
          if (initializationPromise === operation) {
            initializationPromise = null
          }
        })
        .catch(() => undefined)
      return operation
    }

    // call
    void queueInitialization(this)
  }
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  RED.nodes.registerType(NodeType.AmqpIn, AmqpIn)
}
