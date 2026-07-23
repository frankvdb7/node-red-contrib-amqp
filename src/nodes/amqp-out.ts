import { NodeRedApp, EditorNodeProperties, Node, NodeMessageInFlow } from 'node-red'
import { NODE_STATUS } from '../constants'
import { AmqpInNodeDefaults, AmqpOutNodeDefaults, ErrorLocationEnum, ErrorType, NodeType } from '../types'
import Amqp from '../Amqp'
import { Options, Channel, ChannelModel } from 'amqplib'
import ReconnectBackoff from '../reconnect-backoff'

type AmqpOutMessage = NodeMessageInFlow & {
  routingKey?: string
  vhost?: string
  properties?: Options.Publish
}

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

  function AmqpOut(
    config: EditorNodeProperties & {
      exchangeRoutingKey: string
      exchangeRoutingKeyType: string
      amqpProperties: string
    },
  ): void {
    let reconnectTimeout: NodeJS.Timeout
    let reconnect: ((continueUntilConnected?: boolean) => Promise<void>) | null = null
    let reconnectScheduled = false
    let recoveringFromDisconnect = false
    let isShuttingDown = false
    let initializationVersion = 0
    let initializationPromise: Promise<void> | null = null
    let vhostSequence: Promise<void> = Promise.resolve()
    const activePublishes = new Set<Promise<void>>()
    let connection: ChannelModel | null = null
    let channel: Channel | null = null
    let onConnClose: (e: unknown) => Promise<void>
    let onConnError: (e: unknown) => Promise<void>
    let onChannelClose: () => Promise<void>
    let onChannelError: (e: unknown) => Promise<void>
    const me = this
    const reconnectBackoff = new ReconnectBackoff()

    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    RED.nodes.createNode(this, config)
    this.status(NODE_STATUS.Disconnected)
    
    const configAmqp: AmqpInNodeDefaults & AmqpOutNodeDefaults = config;

    const amqp = new Amqp(RED, this, configAmqp)

    const reconnectOnError = configAmqp.reconnectOnError

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

    const setupEventListeners = (nodeIns: Node): void => {
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

      connection.on('close', onConnClose)
      connection.on('error', onConnError)
      channel.on('close', onChannelClose)
      channel.on('error', onChannelError)
    }

    const handleError = async (e: unknown, nodeIns: Node): Promise<void> => {
      const err = isErrorLike(e) ? e : {}
      if (isInvalidLoginError(err)) {
        nodeIns.status(NODE_STATUS.Invalid)
        nodeIns.error(`AmqpOut() Could not connect to broker ${e}`, {
          payload: { error: e, location: ErrorLocationEnum.ConnectError },
        })
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
        nodeIns.error(`AmqpOut() ${e}`, {
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
    }

    // handle input event
    const processInput = async (
      msg: AmqpOutMessage,
      _: unknown,
      done?: (err?: Error) => void,
    ) => {
      const stopIfShuttingDown = async (): Promise<boolean> => {
        if (!isShuttingDown) {
          return false
        }

        await amqp.close().catch(error => {
          me.error(`Could not close AMQP resources during shutdown: ${error}`)
        })
        done && done(new Error('AMQP output node is shutting down'))
        return true
      }

      if (isShuttingDown) {
        done && done(new Error('AMQP output node is shutting down'))
        return
      }
      const { payload, routingKey, vhost, properties: msgProperties } = msg
      const {
        exchangeRoutingKey,
        exchangeRoutingKeyType,
        amqpProperties,
      } = config
      let resolvedRoutingKey = exchangeRoutingKey

      // message properties override config properties
      let properties: Options.Publish
      try {
        properties = {
          ...JSON.parse(amqpProperties),
          ...msgProperties,
        }
      } catch (e) {
        properties = msgProperties
      }

      switch (exchangeRoutingKeyType) {
        case 'msg':
        case 'flow':
        case 'global':
          try {
            resolvedRoutingKey = String(
              RED.util.evaluateNodeProperty(
                exchangeRoutingKey,
                exchangeRoutingKeyType,
                this,
                msg,
              ),
            )
          } catch (err) {
            this.error(`Failed to evaluate routing key: ${err}`)
            done && done(toError(err))
            return
          }
          break
        case 'jsonata': {
          try {
            const expr = RED.util.prepareJSONataExpression(exchangeRoutingKey, this)
            const result = await new Promise<unknown>((resolve, reject) => {
              RED.util.evaluateJSONataExpression(expr, msg, (err, value) => {
                if (err) {
                  reject(err)
                } else {
                  resolve(value)
                }
              })
            })

            if (isShuttingDown && (await stopIfShuttingDown())) {
              return
            }

            if (typeof result !== 'string') {
              this.warn(
                `Routing key JSONata expression returned ${typeof result}; coercing to string`,
              )
            }

            resolvedRoutingKey = String(result)
          } catch (err) {
            this.error(`Failed to evaluate JSONata expression: ${err}`)
            done && done(toError(err))
            return
          }
          break
        }
        case 'str':
        default:
          resolvedRoutingKey = routingKey ?? exchangeRoutingKey
          break
      }

      if (vhost) {
        try {
          clearTimeout(reconnectTimeout)
          reconnectScheduled = false

          removeEventListeners()
          await amqp.setVhost(vhost)

          if (isShuttingDown && (await stopIfShuttingDown())) {
            return
          }

          connection = amqp.getConnection()
          channel = amqp.getChannel()
          setupEventListeners(me)
          amqp.markConnected()
          recoveringFromDisconnect = false
          reconnectBackoff.reset()
        } catch (e) {
          await handleError(e, me)
          done && done(toError(e))
          return
        }
      }

      if (isShuttingDown && (await stopIfShuttingDown())) {
        return
      }

      try {
        await amqp.publish(payload, properties, resolvedRoutingKey)
      } catch (e) {
        done && done(toError(e))
        return
      }

      done && done()
    }

    const inputListener = (
      msg: AmqpOutMessage,
      send: unknown,
      done?: (err?: Error) => void,
    ): void => {
      if (msg.vhost) {
        const precedingPublishes = [...activePublishes]
        const operation = vhostSequence.then(async () => {
          await Promise.allSettled(precedingPublishes)
          await processInput(msg, send, done)
        })
        vhostSequence = operation.catch(error => {
          done && done(toError(error))
        })
        return
      }

      const operation = vhostSequence.then(() => processInput(msg, send, done))
      activePublishes.add(operation)
      void operation
        .finally(() => {
          activePublishes.delete(operation)
        })
        .catch(error => {
          done && done(toError(error))
        })
    }

    this.on('input', inputListener)
    // When the node is re-deployed
    this.on('close', async (removedOrDone: boolean | ((err?: Error) => void), doneMaybe?: (err?: Error) => void): Promise<void> => {
      const removed = typeof removedOrDone === 'boolean' ? removedOrDone : false
      const done = typeof removedOrDone === 'function' ? removedOrDone : doneMaybe
      isShuttingDown = true
      initializationVersion += 1
      clearTimeout(reconnectTimeout)
      removeEventListeners()
      let closeError: unknown
      try {
        await amqp.close(removed ? { removeBindings: true } : undefined)
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

    async function initializeNode(nodeIns: Node) {
      const attemptVersion = initializationVersion
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
        reconnectScheduled = true
        clearTimeout(reconnectTimeout)
        try {
          nodeIns.log('Reconnect requested: closing AMQP resources')
          removeEventListeners()
          await amqp.close(
            new Error('AMQP connection interrupted; reconnecting'),
          )
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
            void queueInitialization(nodeIns)
          }, reconnectDelayMs)
        } catch (error) {
          reconnectScheduled = false
          throw error
        }
      }

      try {
        connection = await amqp.connect()
        if (initializationIsStale()) {
          await amqp.close().catch(() => undefined)
          return
        }
        if (connection) {
          channel = await amqp.initialize()
          if (initializationIsStale()) {
            await amqp.close().catch(() => undefined)
            return
          }
          setupEventListeners(nodeIns)
          amqp.markConnected()
          recoveringFromDisconnect = false
          reconnectBackoff.reset()
        }
      } catch (e) {
        await amqp.close().catch(() => undefined)
        if (
          isShuttingDown ||
          attemptVersion !== initializationVersion
        ) {
          return
        }
        await handleError(e, nodeIns)
      }
    }

    function queueInitialization(nodeIns: Node): Promise<void> {
      const previous = initializationPromise
      const operation = previous
        ? previous.catch(() => undefined).then(() => initializeNode(nodeIns))
        : initializeNode(nodeIns)
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
  RED.nodes.registerType(NodeType.AmqpOut, AmqpOut)
}
