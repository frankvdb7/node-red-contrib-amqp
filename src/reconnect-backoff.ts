import { RECONNECT_BACKOFF } from './constants'

export default class ReconnectBackoff {
  private delayMs: number = RECONNECT_BACKOFF.initialMs

  public nextDelayMs(): number {
    const currentDelay = this.delayMs
    this.delayMs = Math.min(this.delayMs * 2, RECONNECT_BACKOFF.maxMs)
    return currentDelay
  }

  public reset(): void {
    this.delayMs = RECONNECT_BACKOFF.initialMs
  }
}
