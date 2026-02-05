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

module.exports = function (RED: NodeRedApp): void {
  const isErrorLike = (
    value: unknown,
  ): value is { code?: string; message?: string; isOperational?: boolean } =>
    typeof value === 'object' && value !== null

  function AmqpInManualAck(config: EditorNodeProperties): void {
    let reconnectTimeout: NodeJS.Timeout
    let reconnect: (() => Promise<void>) | null = null
    let connection: ChannelModel | null = null
    let channel: Channel | null = null
    let onConnClose: (e: unknown) => Promise<void>
    let onConnError: (e: unknown) => Promise<void>
    let onChannelError: (e: unknown) => Promise<void>


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
      // handle manual reconnect
      if (msg.payload && msg.payload.reconnectCall && typeof reconnect === 'function') {
        await reconnect()
        done && done()
      } else {
        done && done()
      }
    }
    // receive input reconnectCall
    this.on('input', inputListener)
    // When the server goes down
    this.on('close', async (done: () => void): Promise<void> => {
      clearTimeout(reconnectTimeout)
      removeEventListeners()
      await amqp.close()
      done && done()
    })

    const removeEventListeners = (): void => {
      connection?.off?.('close', onConnClose)
      connection?.off?.('error', onConnError)
      channel?.off?.('error', onChannelError)
    }

    async function initializeNode(nodeIns: Node) {
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
            e && reconnectOnError && (await reconnect())
            nodeIns.error(`Connection error ${e}`, { payload: { error: e, location: ErrorLocationEnum.ConnectionErrorEvent } })
          }

          onChannelError = async e => {
            e && reconnectOnError && (await reconnect())
            nodeIns.error(`Channel error ${e}`, { payload: { error: e, location: ErrorLocationEnum.ChannelErrorEvent } })
          }

          connection.on('close', onConnClose)
          connection.on('error', onConnError)
          channel.on('error', onChannelError)

          nodeIns.status(NODE_STATUS.Connected)
        }
      } catch (e: unknown) {
        const err = isErrorLike(e) ? e : {}
        if (
          err.code === ErrorType.InvalidLogin ||
          /ACCESS_REFUSED/i.test(err.message || '')
        ) {
          nodeIns.status(NODE_STATUS.Invalid)
          nodeIns.error(`AmqpInManualAck() Could not connect to broker ${e}`, { payload: { error: e, location: ErrorLocationEnum.ConnectError } })
        } else if (
          err.code === ErrorType.ConnectionRefused ||
          err.code === ErrorType.DnsResolve ||
          err.code === ErrorType.HostNotFound ||
          err.isOperational
        ) {
          reconnectOnError && (await reconnect())
        } else {
          nodeIns.status(NODE_STATUS.Error)
          nodeIns.error(`AmqpInManualAck() ${e}`, { payload: { error: e, location: ErrorLocationEnum.ConnectError } })
          if (reconnectOnError) {
            await reconnect()
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
