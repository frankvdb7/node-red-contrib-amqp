import type { ConsumeMessage, Options } from 'amqplib'
import type {
  Node,
  NodeAPISettingsWithData,
  NodeDef,
  NodeMessage,
} from 'node-red'

export interface AmqpSettings extends NodeAPISettingsWithData {
  MW_CONTRIB_AMQP_USERNAME?: string
  MW_CONTRIB_AMQP_PASSWORD?: string
}

export interface BrokerConfig
  extends Node<{ username: string; password: string }> {
  host: string
  port: number
  vhost: string
  tls: boolean
  credsFromSettings: boolean
  credentials: {
    username: string
    password: string
  }
}

export interface AmqpBrokerNode extends BrokerConfig {
  nodeStates: Record<string, BrokerNodeState>
  lastError: Record<string, BrokerNodeError>
  publishBlocked: Record<string, string>
}

export type AmqpBrokerNodeDef = NodeDef &
  Pick<BrokerConfig, 'host' | 'port' | 'vhost' | 'tls' | 'credsFromSettings'> &
  Partial<Pick<AmqpBrokerNode, 'nodeStates' | 'lastError'>>

export type AmqpNodeConfig = NodeDef & AmqpInNodeDefaults & AmqpOutNodeDefaults

export type BrokerNodeState = 'connected' | 'disconnected' | 'errored'

export interface BrokerNodeError {
  message: string
  code?: string
  at: string
}

export interface AmqpConfig {
  name?: string
  broker: string
  prefetch: number
  noAck: boolean
  waitForConfirms?: boolean
  exchange: {
    name: string
    type: ExchangeType
    routingKey: string
    durable: boolean
    autoCreate: boolean
  }
  queue: {
    name: string
    exclusive: boolean
    durable: boolean
    autoDelete: boolean
    autoCreate: boolean
    queueType: string
    queueArguments?: GenericJsonObject
  }
  amqpProperties: Options.Publish
  headers: GenericJsonObject
  outputs?: number
  rpcTimeout?: number
}

export interface AmqpInNodeDefaults {
  name?: string
  broker?: string
  prefetch?: number
  noAck?: boolean
  exchangeName?: string
  exchangeType?: ExchangeType
  exchangeRoutingKey?: string
  exchangeDurable?: boolean
  autoCreateExchangeBindings?: boolean
  queueName?: string
  queueType?: string
  queueExclusive?: boolean
  queueDurable?: boolean
  queueAutoDelete?: boolean
  autoCreateQueue?: boolean
  queueArguments?: JsonObject
  headers?: JsonObject
}

export interface AmqpOutNodeDefaults {
  name?: string
  broker?: string
  exchangeName?: string
  exchangeType?: ExchangeType
  exchangeRoutingKey?: string
  exchangeDurable?: boolean
  autoCreateExchangeBindings?: boolean
  amqpProperties?: Options.Publish | JsonObject | string
  outputs?: number
  rpcTimeoutMilliseconds?: number
  queueArguments?: JsonObject
  waitForConfirms?: boolean
}

export type JsonPrimitive = string | number | boolean | null
export type JsonValue = JsonPrimitive | JsonObject | JsonArray
export type JsonArray = JsonValue[]
export type JsonObject = { [key: string]: JsonValue }
export type GenericJsonObject = JsonObject

export type AssembledMessage = ConsumeMessage &
  NodeMessage & {
    payload: JsonValue
    manualAck?: ManualAckFields
  }

export interface ManualAckFields {
  ackMode: ManualAckType
  allUpTo?: boolean
  requeue?: boolean
}

export enum ManualAckType {
  Ack = 'ack',
  AckAll = 'ackAll',
  Nack = 'nack',
  NackAll = 'nackAll',
  Reject = 'reject',
}

export enum NodeType {
  AmqpIn = 'amqp-in',
  AmqpOut = 'amqp-out',
  AmqpInManualAck = 'amqp-in-manual-ack',
}

export enum ExchangeType {
  Direct = 'direct',
  Fanout = 'fanout',
  Topic = 'topic',
  Headers = 'headers',
}

export enum DefaultExchangeName {
  Direct = 'amq.direct',
  Fanout = 'amq.fanout',
  Topic = 'amq.topic',
  Headers = 'amq.headers',
}
