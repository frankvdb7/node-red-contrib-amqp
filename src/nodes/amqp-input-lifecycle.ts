import { Node, NodeRedApp } from 'node-red'
import { Channel, ChannelModel } from 'amqplib'
import { NODE_STATUS } from '../constants'
import {
  AmqpInNodeDefaults,
  AmqpOutNodeDefaults,
  ErrorLocationEnum,
  ErrorType,
} from '../types'
import Amqp from '../Amqp'
import AmqpLifecycleCoordinator, {
  LifecycleAttempt,
} from '../amqp-lifecycle-coordinator'
import trackTerminalClose from './flow-close-tracker'

export interface AmqpInputLifecycleOptions {
  RED: NodeRedApp
  node: Node
  config: AmqpInNodeDefaults & AmqpOutNodeDefaults
  amqp: Amqp
  nodeName: string
  registerInput: (lifecycle: AmqpLifecycleCoordinator) => void
}

export default function createAmqpInputLifecycle(
  options: AmqpInputLifecycleOptions,
): AmqpLifecycleCoordinator {
  const { RED, node, config, amqp, nodeName, registerInput } = options
  const terminalClose = trackTerminalClose(RED, node)
  const reconnectOnError = config.reconnectOnError
  const nodeEmitter = node as unknown as {
    on?: (event: string, listener: (...args: unknown[]) => void) => void
    off?: (event: string, listener: (...args: unknown[]) => void) => void
  }
  let connection: ChannelModel | null = null
  let channel: Channel | null = null
  let onConnClose: () => Promise<void>
  let onConnError: (error: unknown) => Promise<void>
  let onChannelClose: () => Promise<void>
  let onChannelError: (error: unknown) => Promise<void>
  let onConsumerCancelled: () => Promise<void>
  let lifecycle: AmqpLifecycleCoordinator

  const isInvalidLoginError = (value: unknown): boolean => {
    const error =
      typeof value === 'object' && value !== null
        ? (value as { code?: string; message?: string })
        : {}
    return (
      error.code === ErrorType.InvalidLogin ||
      /ACCESS_REFUSED/i.test(error.message || '')
    )
  }

  const reportReconnectFailure = (
    message: string,
    error: unknown,
    location: ErrorLocationEnum,
  ): void => {
    node.error(`${message}: ${error}`, { payload: { error, location } })
  }

  const removeEventListeners = (): void => {
    if (typeof onConnClose === 'function') connection?.off?.('close', onConnClose)
    if (typeof onConnError === 'function') connection?.off?.('error', onConnError)
    if (typeof onChannelClose === 'function') channel?.off?.('close', onChannelClose)
    if (typeof onChannelError === 'function') channel?.off?.('error', onChannelError)
    if (typeof onConsumerCancelled === 'function') {
      nodeEmitter.off?.('amqp:consumer-cancelled', onConsumerCancelled)
    }
  }

  const setupEventListeners = (): void => {
    onConnClose = async () => {
      node.warn('AMQP connection closed event received')
      await lifecycle.reconnect(true).catch(error =>
        reportReconnectFailure(
          'Reconnect failed after connection close',
          error,
          ErrorLocationEnum.ConnectionErrorEvent,
        ),
      )
    }
    onConnError = async error => {
      if (reconnectOnError) {
        await lifecycle.reconnect().catch(reconnectError =>
          reportReconnectFailure(
            'Reconnect failed after connection error',
            reconnectError,
            ErrorLocationEnum.ConnectionErrorEvent,
          ),
        )
      }
      node.error(`Connection error ${error}`, {
        payload: { error, location: ErrorLocationEnum.ConnectionErrorEvent },
      })
    }
    onChannelClose = async () => {
      node.warn('AMQP channel closed event received')
      await lifecycle.reconnect(true).catch(error =>
        reportReconnectFailure(
          'Reconnect failed after channel close',
          error,
          ErrorLocationEnum.ChannelErrorEvent,
        ),
      )
    }
    onChannelError = async error => {
      if (reconnectOnError) {
        await lifecycle.reconnect().catch(reconnectError =>
          reportReconnectFailure(
            'Reconnect failed after channel error',
            reconnectError,
            ErrorLocationEnum.ChannelErrorEvent,
          ),
        )
      }
      node.error(`Channel error ${error}`, {
        payload: { error, location: ErrorLocationEnum.ChannelErrorEvent },
      })
    }
    onConsumerCancelled = async () => {
      node.warn('AMQP consumer cancelled event received')
      await lifecycle.reconnect(true).catch(error =>
        reportReconnectFailure(
          'Reconnect failed after consumer cancellation',
          error,
          ErrorLocationEnum.ChannelErrorEvent,
        ),
      )
    }
    connection?.on('close', onConnClose)
    connection?.on('error', onConnError)
    channel?.on('close', onChannelClose)
    channel?.on('error', onChannelError)
    nodeEmitter.on?.('amqp:consumer-cancelled', onConsumerCancelled)
  }

  const initialize = async (attempt: LifecycleAttempt): Promise<void> => {
    connection = await amqp.connect({ signal: attempt.signal })
    if (attempt.isStale()) return
    channel = await amqp.initialize({ signal: attempt.signal })
    if (attempt.isStale()) return
    await amqp.consume()
    if (attempt.isStale()) return
    setupEventListeners()
    amqp.markConnected()
  }

  lifecycle = new AmqpLifecycleCoordinator({
    node,
    initialize,
    close: () => amqp.close(),
    removeEventListeners,
    clearResources: () => {
      connection = null
      channel = null
    },
    onInitializationError: async (error, recovering) => {
      const invalidLogin = isInvalidLoginError(error)
      if (invalidLogin) {
        node.status(NODE_STATUS.Invalid)
        node.error(`${nodeName} Could not connect to broker ${error}`, {
          payload: { error, location: ErrorLocationEnum.ConnectError },
        })
      } else {
        node.error(`${nodeName} ${error}`, {
          payload: { error, location: ErrorLocationEnum.ConnectError },
        })
      }
      if (reconnectOnError || recovering) {
        let failed = false
        await lifecycle.reconnect().catch(reconnectError => {
          failed = true
          node.status(NODE_STATUS.Error)
          reportReconnectFailure(
            'Reconnect failed during initialization',
            reconnectError,
            ErrorLocationEnum.ConnectError,
          )
        })
        if (invalidLogin && !failed) node.status(NODE_STATUS.Invalid)
      } else if (!invalidLogin) {
        node.status(NODE_STATUS.Error)
      }
    },
  })

  registerInput(lifecycle)
  node.on('close', async (removedOrDone: boolean | ((error?: Error) => void), doneMaybe?: (error?: Error) => void) => {
    const removed = typeof removedOrDone === 'boolean' ? removedOrDone : false
    const done = typeof removedOrDone === 'function' ? removedOrDone : doneMaybe
    const removeBindings = terminalClose.shouldRemoveBindings(removed)
    terminalClose.dispose()
    lifecycle.shutdown(new Error('AMQP initialization cancelled during shutdown'))
    try {
      await amqp.close(removeBindings ? { removeBindings: true } : undefined)
      done?.()
    } catch (error) {
      done?.(error instanceof Error ? error : new Error(String(error)))
    } finally {
      if (removed) amqp.removeBrokerNodeState()
    }
  })

  void lifecycle.start()
  return lifecycle
}
