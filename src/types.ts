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

export interface AmqpConfig {
  name?: string
  broker: string
  prefetch: number
  reconnectOnError?: boolean
  noAck: boolean
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
  name?: any
  broker?: any
  prefetch?: any
  reconnectOnError?: boolean
  noAck?: any
  exchangeName?: any
  exchangeType?: any
  exchangeRoutingKey?: any
  exchangeDurable?: any
  queueName?: any
  queueType?: any
  queueExclusive?: any
  queueDurable?: any
  queueAutoDelete?: any
  queueArguments?: any
  headers?: any
}

export interface AmqpOutNodeDefaults {
  name?: any
  broker?: any
  exchangeName?: any
  exchangeType?: any
  exchangeRoutingKey?: any
  exchangeDurable?: any
  amqpProperties?: any
  outputs?: any
  rpcTimeoutMilliseconds?: any
  queueArguments?: any
}

export type GenericJsonObject = Record<string, any>

export type AssembledMessage = ConsumeMessage & {
  payload: GenericJsonObject | string
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
