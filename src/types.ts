import { Node } from 'node-red'
import { ConsumeMessage, MessageProperties } from 'amqplib'

export interface BrokerConfig extends Node {
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
  connections: Record<string, boolean>
}

export interface AmqpConfig {
  name?: string
  broker: string
  prefetch: number
  reconnectOnError?: boolean
  noAck: boolean
  waitForConfirms?: boolean
  exchange: {
    name: string
    type: ExchangeType
    routingKey: string
    durable: boolean
  }
  queue: {
    name: string
    exclusive: boolean
    durable: boolean
    autoDelete: boolean
    queueType: string
    queueArguments?: GenericJsonObject
  }
  amqpProperties: MessageProperties
  headers: GenericJsonObject
  outputs?: number
  rpcTimeout?: number
}

export interface AmqpInNodeDefaults {
  name?: string
  broker?: string
  prefetch?: number
  reconnectOnError?: boolean
  noAck?: boolean
  exchangeName?: string
  exchangeType?: ExchangeType
  exchangeRoutingKey?: string
  exchangeDurable?: boolean
  queueName?: string
  queueType?: string
  queueExclusive?: boolean
  queueDurable?: boolean
  queueAutoDelete?: boolean
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
  amqpProperties?: MessageProperties | JsonObject | string
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

export type AssembledMessage = ConsumeMessage & {
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

export enum ErrorType {
  InvalidLogin = 'EACCES',
  ConnectionRefused = 'ECONNREFUSED',
  DnsResolve = 'EAI_AGAIN',
  HostNotFound = 'ENOTFOUND',
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

export enum ErrorLocationEnum {
  ConnectError = 'ConnectError',
  ConnectionErrorEvent = 'ConnectionErrorEvent',
  ChannelErrorEvent = 'ChannelErrorEvent'
}
