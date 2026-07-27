import { Node } from 'node-red'
import ReconnectBackoff from './reconnect-backoff'

export interface LifecycleAttempt {
  signal: AbortSignal
  isStale: () => boolean
}

interface LifecycleCoordinatorOptions {
  node: Node
  initialize: (attempt: LifecycleAttempt) => Promise<void>
  close: (interruption?: Error) => Promise<void>
  removeEventListeners: () => void
  clearResources: () => void
  onInitializationError: (error: unknown, recovering: boolean) => Promise<void>
  reconnectInterruption?: Error
}

export default class AmqpLifecycleCoordinator {
  private generation = 0
  private initializationPromise: Promise<void> | null = null
  private abortController: AbortController | null = null
  private reconnectTimeout?: NodeJS.Timeout
  private reconnectScheduled = false
  private recovering = false
  private shuttingDown = false
  private readonly backoff = new ReconnectBackoff()

  public constructor(private readonly options: LifecycleCoordinatorOptions) {}

  public start(): Promise<void> {
    return this.queueInitialization()
  }

  public isShuttingDown(): boolean {
    return this.shuttingDown
  }

  public isRecovering(): boolean {
    return this.recovering
  }

  public async awaitInitialization(): Promise<void> {
    await this.initializationPromise?.catch(() => undefined)
  }

  public supersede(reason: Error): void {
    this.generation += 1
    clearTimeout(this.reconnectTimeout)
    this.reconnectScheduled = false
    this.abortController?.abort(reason)
  }

  public async replace(
    reason: Error,
    operation: (attempt: LifecycleAttempt) => Promise<void>,
  ): Promise<boolean> {
    this.supersede(reason)
    await this.awaitInitialization()
    if (this.shuttingDown) {
      return false
    }

    this.supersede(reason)
    const attemptGeneration = this.generation
    const abortController = new AbortController()
    this.abortController = abortController
    const isStale = (): boolean =>
      this.shuttingDown || attemptGeneration !== this.generation

    try {
      await operation({ signal: abortController.signal, isStale })
      if (isStale()) {
        await this.options.close().catch(() => undefined)
        return false
      }
      this.markConnected()
      return true
    } finally {
      if (this.abortController === abortController) {
        this.abortController = null
      }
    }
  }

  public async reconnect(continueUntilConnected = false): Promise<void> {
    this.recovering ||= continueUntilConnected
    if (this.shuttingDown || this.reconnectScheduled) {
      if (this.shuttingDown) {
        this.options.node.log('Reconnect skipped: node is shutting down')
      }
      return
    }

    this.generation += 1
    const reconnectGeneration = this.generation
    this.abortController?.abort(
      new Error('AMQP initialization cancelled for reconnect'),
    )
    this.reconnectScheduled = true
    clearTimeout(this.reconnectTimeout)

    try {
      this.options.node.log('Reconnect requested: closing AMQP resources')
      this.options.removeEventListeners()
      await this.options.close(this.options.reconnectInterruption)
      if (
        this.shuttingDown ||
        reconnectGeneration !== this.generation ||
        !this.reconnectScheduled
      ) {
        this.reconnectScheduled = false
        this.options.node.log(
          'Reconnect aborted: request was superseded while closing AMQP resources',
        )
        return
      }

      this.options.clearResources()
      const reconnectDelayMs = this.backoff.nextDelayMs()
      this.options.node.log(`Reconnect scheduled in ${reconnectDelayMs}ms`)
      this.reconnectTimeout = setTimeout(() => {
        if (this.shuttingDown) {
          this.reconnectScheduled = false
          this.options.node.log('Reconnect timer fired but node is shutting down')
          return
        }
        this.options.node.log('Reconnect timer fired: re-initializing AMQP node')
        void this.queueInitialization(true)
      }, reconnectDelayMs)
    } catch (error) {
      this.reconnectScheduled = false
      throw error
    }
  }

  public shutdown(reason: Error): void {
    this.shuttingDown = true
    this.supersede(reason)
    this.options.removeEventListeners()
  }

  public markConnected(): void {
    this.recovering = false
    this.backoff.reset()
  }

  private async initialize(): Promise<void> {
    const attemptGeneration = this.generation
    const abortController = new AbortController()
    this.abortController = abortController
    const isStale = (): boolean =>
      this.shuttingDown || attemptGeneration !== this.generation

    try {
      await this.options.initialize({
        signal: abortController.signal,
        isStale,
      })
      if (isStale()) {
        await this.options.close().catch(() => undefined)
        return
      }
      this.markConnected()
    } catch (error) {
      await this.options.close().catch(() => undefined)
      if (isStale()) {
        return
      }
      await this.options.onInitializationError(error, this.recovering)
    } finally {
      if (this.abortController === abortController) {
        this.abortController = null
      }
    }
  }

  private queueInitialization(scheduledReconnect = false): Promise<void> {
    const previous = this.initializationPromise
    const queuedGeneration = this.generation
    const startInitialization = (): Promise<void> => {
      if (scheduledReconnect) {
        this.reconnectScheduled = false
      }
      if (this.shuttingDown || queuedGeneration !== this.generation) {
        return Promise.resolve()
      }
      return this.initialize()
    }
    const operation = previous
      ? previous.catch(() => undefined).then(startInitialization)
      : startInitialization()
    this.initializationPromise = operation
    void operation
      .finally(() => {
        if (this.initializationPromise === operation) {
          this.initializationPromise = null
        }
      })
      .catch(() => undefined)
    return operation
  }
}
