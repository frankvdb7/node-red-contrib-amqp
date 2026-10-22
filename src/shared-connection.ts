import {
  type ChannelModel,
  connect,
  type RecoveringChannelModel,
} from 'amqplib'

export interface ConnectionOwner {
  setup(model: ChannelModel): Promise<void>
  connected(): void
  disconnected(error: unknown): void
  failed(error: unknown): void
}

// One native recovery loop per broker/vhost. Channels belong to individual nodes.
export default class SharedConnection {
  private owners = new Map<ConnectionOwner, Promise<boolean> | undefined>()
  private model?: ChannelModel
  private ready = false
  private stopped = false
  private restarting?: Promise<void>
  private cleanup = Promise.resolve()
  readonly connection: Promise<RecoveringChannelModel>

  constructor(url: string) {
    this.connection = connect(url, {
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
      connection.on('error', error => this.notify('failed', error))
      connection.on('connect-failed', error => {
        this.invalidate()
        this.notify('failed', error)
      })
      connection.on('disconnect', error => {
        this.invalidate()
        this.notify('disconnected', error)
      })
      connection.on('reconnect-scheduled', ({ attempt, delay, error }) => {
        this.notify(
          'failed',
          new Error(`AMQP retry ${attempt} in ${delay}ms: ${error.message}`),
        )
      })
      connection.on('connect', model => {
        if (this.stopped || this.model !== model) return
        this.ready = true
        for (const owner of this.owners.keys()) {
          void this.attach(owner).catch(error => owner.failed(error))
        }
      })
      return connection
    })
  }

  async add(owner: ConnectionOwner): Promise<RecoveringChannelModel> {
    this.owners.set(owner, undefined)
    const connection = await this.connection
    if (this.ready && this.model && this.owners.has(owner)) {
      await this.attach(owner)
    }
    return connection
  }

  get size(): number {
    return this.owners.size
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
      if (lastOwner) await (await this.connection).close()
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
    this.model = model
    this.ready = false
    // Recovery attaches its raw-model error listener only after setup completes.
    model.on('error', error => {
      if (this.model === model) this.notify('failed', error)
    })
    model.once('close', () => {
      if (this.model === model) this.invalidate()
    })
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
    const succeeded = await this.setupOwner(owner, model)
    if (
      succeeded &&
      this.ready &&
      this.model === model &&
      this.owners.has(owner)
    )
      owner.connected()
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
          if (this.owners.has(owner)) owner.failed(error)
          return false
        },
      )
      this.owners.set(owner, setup)
    }
    return setup
  }

  private invalidate(): void {
    this.ready = false
    this.model = undefined
    for (const owner of this.owners.keys()) this.owners.set(owner, undefined)
  }

  private notify(event: 'failed' | 'disconnected', error: unknown): void {
    for (const owner of this.owners.keys()) owner[event](error)
  }
}
