import { NodeRedApp, EditorNodeProperties, Node, NodeMessageInFlow } from 'node-red'
import { NODE_STATUS } from '../constants'
import { AmqpInNodeDefaults, AmqpOutNodeDefaults, ErrorLocationEnum, ErrorType, NodeType } from '../types'
import Amqp from '../Amqp'
import { Options, Channel, ChannelModel } from 'amqplib'
import AmqpLifecycleCoordinator, {
  LifecycleAttempt,
} from '../amqp-lifecycle-coordinator'

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
    let vhostSequence: Promise<void> = Promise.resolve()
    const activePublishes = new Set<Promise<void>>()
    let connection: ChannelModel | null = null
    let channel: Channel | null = null
    let onConnClose: (e: unknown) => Promise<void>
    let onConnError: (e: unknown) => Promise<void>
    let onChannelClose: () => Promise<void>
    let onChannelError: (e: unknown) => Promise<void>
    const me = this
    let lifecycle: AmqpLifecycleCoordinator

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
          await lifecycle.reconnect(true)
        } catch (reconnectError) {
          nodeIns.error(`Reconnect failed after connection close: ${reconnectError}`, {
            payload: { error: reconnectError, location: ErrorLocationEnum.ConnectionErrorEvent },
          })
        }
      }

      onConnError = async e => {
        if (reconnectOnError) {
          try {
            await lifecycle.reconnect()
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
          await lifecycle.reconnect(true)
        } catch (reconnectError) {
          nodeIns.error(`Reconnect failed after channel close: ${reconnectError}`, {
            payload: { error: reconnectError, location: ErrorLocationEnum.ChannelErrorEvent },
          })
        }
      }

      onChannelError = async e => {
        if (reconnectOnError) {
          try {
            await lifecycle.reconnect()
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
        if (reconnectOnError || lifecycle.isRecovering()) {
          let reconnectFailed = false
          await lifecycle.reconnect().catch(reconnectError => {
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
        if (reconnectOnError || lifecycle.isRecovering()) {
          await lifecycle.reconnect().catch(reconnectError => {
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
        if (!lifecycle.isShuttingDown()) {
          return false
        }

        await amqp.close().catch(error => {
          me.error(`Could not close AMQP resources during shutdown: ${error}`)
        })
        done && done(new Error('AMQP output node is shutting down'))
        return true
      }

      if (lifecycle.isShuttingDown()) {
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

            if (lifecycle.isShuttingDown() && (await stopIfShuttingDown())) {
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
          const vhostChanged = amqp.getVhost() !== vhost
          if (vhostChanged) {
            lifecycle.supersede(
              new Error('AMQP initialization cancelled for virtual host switch'),
            )
          }
          await lifecycle.awaitInitialization()

          if (lifecycle.isShuttingDown() && (await stopIfShuttingDown())) {
            return
          }

          const vhostSwitchRequired = !amqp.isInitializedForVhost(vhost)
          if (vhostSwitchRequired) {
            const switched = await lifecycle.replace(
              new Error('AMQP initialization cancelled for virtual host switch'),
              async attempt => {
                removeEventListeners()
                await amqp.setVhost(vhost, { signal: attempt.signal })
                if (attempt.isStale()) return
                connection = amqp.getConnection()
                channel = amqp.getChannel()
                setupEventListeners(me)
                amqp.markConnected()
              },
            )
            if (!switched || (await stopIfShuttingDown())) {
              return
            }
          }
        } catch (e) {
          if (lifecycle.isShuttingDown()) {
            done && done()
            return
          }
          await handleError(e, me)
          done && done(toError(e))
          return
        }
      }

      if (lifecycle.isShuttingDown() && (await stopIfShuttingDown())) {
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
      lifecycle.shutdown(
        new Error('AMQP initialization cancelled during shutdown'),
      )
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

    const initialize = async (attempt: LifecycleAttempt): Promise<void> => {
      connection = await amqp.connect({ signal: attempt.signal })
      if (attempt.isStale()) return
      channel = await amqp.initialize({ signal: attempt.signal })
      if (attempt.isStale()) return
      setupEventListeners(me)
      amqp.markConnected()
    }

    lifecycle = new AmqpLifecycleCoordinator({
      node: me,
      initialize,
      close: interruption => amqp.close(interruption),
      removeEventListeners,
      clearResources: () => {
        connection = null
        channel = null
      },
      onInitializationError: (error, _recovering) => handleError(error, me),
      reconnectInterruption: new Error(
        'AMQP connection interrupted; reconnecting',
      ),
    })

    void lifecycle.start()
  }
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  RED.nodes.registerType(NodeType.AmqpOut, AmqpOut)
}
