import {
  ErrorType,
  ExchangeType,
  BrokerConfig,
  NodeType,
  AmqpInNodeDefaults,
  AmqpOutNodeDefaults,
} from '../src/types'

export const amqpInFlowFixture = [
  {
    id: 'n1',
    type: NodeType.AmqpIn,
    wires: [['n2']],
    name: '',
    broker: 'n3',
    noAck: true,
    exchangeName: 'testtopic',
    exchangeType: 'topic',
    routingKey: '#',
    durable: true,
    queueName: '',
    exclusive: true,
  },
  { id: 'n2', type: 'helper' },
  {
    id: 'n3',
    type: 'amqp-broker',
    z: '',
    host: 'localhost',
    port: '5672',
  },
]

export const amqpInManualAckFlowFixture = [
  {
    id: 'n1',
    type: NodeType.AmqpInManualAck,
    wires: [['n2']],
    name: '',
    broker: 'n3',
    noAck: true,
    exchangeName: 'testtopic',
    exchangeType: 'topic',
    routingKey: '#',
    durable: true,
    queueName: '',
    exclusive: true,
  },
  { id: 'n2', type: 'helper' },
  {
    id: 'n3',
    type: 'amqp-broker',
    z: '',
    host: 'localhost',
    port: '5672',
  },
]

export const amqpOutFlowFixture = [
  {
    id: 'n1',
    type: NodeType.AmqpOut,
    wires: [['n2']],
    name: '',
    broker: 'n3',
    noAck: true,
    exchangeName: 'testtopic',
    exchangeType: 'topic',
    exchangeRoutingKey: 'test.message.topic',
    exchangeRoutingKeyType: 'str',
    durable: true,
    queueName: '',
    exclusive: true,
    waitForConfirms: false,
  },
  { id: 'n2', type: 'helper' },
  {
    id: 'n3',
    type: 'amqp-broker',
    z: '',
    host: 'localhost',
    port: '5672',
  },
]

export const credentialsFixture = { username: 'username', password: 'password' }

export const nodeConfigFixture: AmqpInNodeDefaults & AmqpOutNodeDefaults = {
  name: 'name',
  broker: 'b1',
  exchangeName: 'exchangeName',
  exchangeType: ExchangeType.Topic,
  noAck: false,
  exchangeRoutingKey: 'routing.key',
  exchangeDurable: true,
  queueName: '',
  queueType: 'quorom',
  queueExclusive: true,
  queueDurable: false,
  queueAutoDelete: true,
  queueArguments: { 'x-dead-letter-exchange': 'dlx-exchange' },
  waitForConfirms: false,
}

export const nodeFixture = {
  status: (): null => null,
  log: (): null => null,
  warn: (): null => null,
  error: (): null => null,
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const brokerConfigFixture: any & BrokerConfig = {
  host: 'host',
  port: 222,
  vhost: '',
  credentials: {
    username: 'username',
    password: 'password',
  },
  connections: {},
}

export class CustomError extends Error {
  constructor(private readonly code: ErrorType, message?: string) {
    super(message)

    // Maintains proper stack trace for where our error was thrown (only available on V8)
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, CustomError)
    }

    this.name = 'CustomError'
  }
}
