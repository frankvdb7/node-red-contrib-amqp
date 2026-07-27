import { Node } from 'node-red'
import { Channel } from 'amqplib'
import { v4 as uuidv4 } from 'uuid'
import { AssembledMessage } from './types'

export const DELIVERY_TOKEN: unique symbol = Symbol('amqp-delivery-token')
type TrackedAssembledMessage = AssembledMessage & { [DELIVERY_TOKEN]?: string }

export interface TrackedDelivery {
  message: AssembledMessage
  channel: Channel
  deliveryTag: number
}

export default class DeliverySettlementTracker {
  private readonly deliveries = new Map<string, TrackedDelivery>()

  public constructor(private readonly node: Node) {}

  public track(
    message: AssembledMessage,
    channel: Channel,
    manualAck: boolean,
  ): AssembledMessage {
    if (!manualAck) return message
    const token = uuidv4()
    ;(message as TrackedAssembledMessage)[DELIVERY_TOKEN] = token
    this.deliveries.set(token, {
      message,
      channel,
      deliveryTag: message.fields.deliveryTag,
    })
    return message
  }

  public resolve(
    message: AssembledMessage,
    operation: string,
    channel: Channel,
    manualAck: boolean,
  ): (TrackedDelivery & { token?: string }) | null {
    if (!manualAck) {
      return { message, channel, deliveryTag: message.fields?.deliveryTag ?? 0 }
    }
    const token = (message as TrackedAssembledMessage)[DELIVERY_TOKEN]
    const delivery = token ? this.deliveries.get(token) : undefined
    if (!delivery || delivery.channel !== channel) {
      this.node.error(
        `Could not ${operation} message: delivery does not belong to the active AMQP channel`,
      )
      return null
    }
    return { ...delivery, token }
  }

  public remove(
    delivery: TrackedDelivery & { token?: string },
    allUpTo: boolean,
    manualAck: boolean,
  ): void {
    if (!manualAck) return
    if (!allUpTo) {
      if (delivery.token) this.deliveries.delete(delivery.token)
      return
    }
    for (const [token, tracked] of this.deliveries) {
      if (
        tracked.channel === delivery.channel &&
        tracked.deliveryTag <= delivery.deliveryTag
      ) {
        this.deliveries.delete(token)
      }
    }
  }

  public clear(): void {
    this.deliveries.clear()
  }
}
