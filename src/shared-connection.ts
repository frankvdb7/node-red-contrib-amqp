import {
  type ChannelModel,
  connect,
  type RecoveringChannelModel,
} from 'amqplib'

export interface ConnectionOwner {
  queueName?(): string | undefined
  setup(model: ChannelModel): Promise<void>
  connected(): void
  disconnected(error: unknown): void
  failed(error: unknown): void
  blocked?(reason: string): void
  unblocked?(): void
}

interface ConnectionListeners {
  connection: RecoveringChannelModel
  error: (error: Error) => void
  connectFailed: (error: Error) => void
  disconnect: (error: Error) => void
  reconnectScheduled: (info: {
    attempt: number
    delay: number
    error: Error
  }) => void
  connect: (model: ChannelModel) => void
  blocked: (reason: string) => void
  unblocked: () => void
}

interface ModelListeners {
  model: ChannelModel
  error: (error: Error) => void
  close: () => void
  blocked: (reason: string) => void
  unblocked: () => void
}

// One native recovery loop per broker/vhost. Channels belong to individual nodes.
export default class SharedConnection {
  private owners = new Map<ConnectionOwner, Promise<boolean> | undefined>()
  private model?: ChannelModel
  private ready = false
  private stopped = false
  private blockedReason?: string
  private restarting?: Promise<void>
  private cleanup = Promise.resolve()
  private connection?: Promise<RecoveringChannelModel>
  private connectionListeners?: ConnectionListeners
  private modelListeners?: ModelListeners

  constructor(private readonly url: string) {}

  connect(): Promise<RecoveringChannelModel> {
    this.connection ??= connect(this.url, {
      timeout: 10000,
      recovery: {
        waitForConnect: false,
        maxRetries: Infinity,
        initialDelay: 1000,
        maxDelay: 30000,
        factor: 2,
        jitter: 0.2,
        setup: model => this.setup(model),
      },
    }).then(connection => {
      const listeners: ConnectionListeners = {
        connection,
        error: error => this.notify('failed', error),
        connectFailed: error => {
          this.invalidate()
          this.notify('failed', error)
        },
        disconnect: error => {
          this.invalidate()
          this.notify('disconnected', error)
        },
        reconnectScheduled: ({ attempt, delay, error }) => {
          this.notify(
            'failed',
            new Error(`AMQP retry ${attempt} in ${delay}ms: ${error.message}`),
          )
        },
        connect: model => {
          if (this.stopped || this.model !== model) return
          this.detachModelBlockedListeners(model)
          this.ready = true
          for (const owner of this.owners.keys()) {
            void this.attach(owner).catch(error => owner.failed(error))
          }
        },
        blocked: reason => this.handleBlocked(reason),
        unblocked: () => this.handleUnblocked(),
      }
      this.connectionListeners = listeners
      connection.on('error', listeners.error)
      connection.on('connect-failed', listeners.connectFailed)
      connection.on('disconnect', listeners.disconnect)
      connection.on('reconnect-scheduled', listeners.reconnectScheduled)
      connection.on('connect', listeners.connect)
      connection.on('blocked', listeners.blocked)
      connection.on('unblocked', listeners.unblocked)
      return connection
    })
    return this.connection
  }

  async add(owner: ConnectionOwner): Promise<RecoveringChannelModel> {
    this.owners.set(owner, undefined)
    const connection = await this.connect()
    if (this.ready && this.model && this.owners.has(owner)) {
      await this.attach(owner)
    }
    return connection
  }

  get size(): number {
    return this.owners.size
  }

  hasQueueConsumer(queueName: string): boolean {
    return [...this.owners.keys()].some(
      owner => owner.queueName?.() === queueName,
    )
  }

  async remove(
    owner: ConnectionOwner,
    cleanup: () => Promise<void>,
  ): Promise<void> {
    this.owners.delete(owner)
    const lastOwner = this.owners.size === 0
    if (lastOwner) {
      this.stopped = true
      this.invalidate()
    }
    const cleaning = this.cleanup.then(cleanup)
    this.cleanup = cleaning.catch(() => undefined)
    try {
      await cleaning
    } finally {
      if (lastOwner && this.connection) {
        try {
          await (await this.connection).close()
        } finally {
          this.detachConnectionListeners()
          this.detachModelListeners()
        }
      }
    }
  }

  // Channel-level failures request a disconnect; only amqplib schedules retries.
  async restart(): Promise<void> {
    if (!this.model || this.stopped) return
    if (!this.restarting) {
      const model = this.model
      // A channel can fail after its own setup while a shared peer is still pending.
      this.invalidate()
      this.notify('disconnected', new Error('AMQP channel requires recovery'))
      this.restarting = model.close().finally(() => {
        this.restarting = undefined
      })
    }
    await this.restarting
  }

  private async setup(model: ChannelModel): Promise<void> {
    if (this.stopped) return
    this.detachModelListeners()
    this.model = model
    this.ready = false
    this.blockedReason = undefined
    // Forward setup errors ourselves; native recovery forwards them once connected.
    const listeners: ModelListeners = {
      model,
      error: error => {
        if (this.model === model && !this.ready) this.notify('failed', error)
      },
      close: () => {
        this.detachModelListeners(model)
        if (this.model === model) this.invalidate()
      },
      blocked: reason => this.handleBlocked(reason),
      unblocked: () => this.handleUnblocked(),
    }
    this.modelListeners = listeners
    model.on('error', listeners.error)
    model.once('close', listeners.close)
    model.on('blocked', listeners.blocked)
    model.on('unblocked', listeners.unblocked)
    for (const owner of this.owners.keys()) this.owners.set(owner, undefined)
    // Include owners added while another owner's asynchronous setup was running.
    while (!this.stopped && this.model === model) {
      const pending = [...this.owners].filter(([, setup]) => !setup)
      if (!pending.length) break
      await Promise.all(pending.map(([owner]) => this.setupOwner(owner, model)))
    }
    if (!this.stopped && this.model !== model)
      throw new Error('AMQP connection invalidated during setup')
  }

  private async attach(owner: ConnectionOwner): Promise<void> {
    const model = this.model
    if (!model || !this.owners.has(owner)) return
    const setup = this.setupOwner(owner, model)
    const succeeded = await setup
    if (
      succeeded &&
      this.ready &&
      this.model === model &&
      this.owners.get(owner) === setup
    ) {
      owner.connected()
      if (this.blockedReason !== undefined) {
        owner.blocked?.(this.blockedReason)
      }
    }
  }

  private setupOwner(
    owner: ConnectionOwner,
    model: ChannelModel,
  ): Promise<boolean> {
    let setup = this.owners.get(owner)
    if (!setup) {
      setup = owner.setup(model).then(
        () => true,
        error => {
          // A topology error belongs to this node, not to the shared transport.
          if (this.model === model && this.owners.get(owner) === setup)
            owner.failed(error)
          return false
        },
      )
      this.owners.set(owner, setup)
    }
    return setup
  }

  private invalidate(): void {
    this.ready = false
    this.blockedReason = undefined
    this.detachModelListeners()
    this.model = undefined
    for (const owner of this.owners.keys()) this.owners.set(owner, undefined)
  }

  private handleBlocked(reason: string): void {
    if (this.stopped || this.blockedReason === reason) return
    this.blockedReason = reason
    for (const owner of this.owners.keys()) owner.blocked?.(reason)
  }

  private handleUnblocked(): void {
    if (this.stopped || this.blockedReason === undefined) return
    this.blockedReason = undefined
    for (const owner of this.owners.keys()) owner.unblocked?.()
  }

  private detachConnectionListeners(): void {
    const listeners = this.connectionListeners
    if (!listeners) return
    const { connection } = listeners
    connection.removeListener('error', listeners.error)
    connection.removeListener('connect-failed', listeners.connectFailed)
    connection.removeListener('disconnect', listeners.disconnect)
    connection.removeListener(
      'reconnect-scheduled',
      listeners.reconnectScheduled,
    )
    connection.removeListener('connect', listeners.connect)
    connection.removeListener('blocked', listeners.blocked)
    connection.removeListener('unblocked', listeners.unblocked)
    this.connectionListeners = undefined
  }

  private detachModelListeners(model?: ChannelModel): void {
    const listeners = this.modelListeners
    if (!listeners || (model && listeners.model !== model)) return
    this.detachModelBlockedListeners(model)
    listeners.model.removeListener('error', listeners.error)
    listeners.model.removeListener('close', listeners.close)
    this.modelListeners = undefined
  }

  private detachModelBlockedListeners(model?: ChannelModel): void {
    const listeners = this.modelListeners
    if (!listeners || (model && listeners.model !== model)) return
    listeners.model.removeListener('blocked', listeners.blocked)
    listeners.model.removeListener('unblocked', listeners.unblocked)
  }

  private notify(event: 'failed' | 'disconnected', error: unknown): void {
    for (const owner of this.owners.keys()) owner[event](error)
  }
}
