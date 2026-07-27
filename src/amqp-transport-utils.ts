import { BrokerConfig, JsonObject, JsonValue } from './types'

export function brokerUrl(
  broker: BrokerConfig,
  credentials: { username: string; password: string },
): string {
  const protocol = broker.tls ? 'amqps' : 'amqp'
  const vhostPath = broker.vhost ? `/${encodeURIComponent(broker.vhost)}` : '/'
  return `${protocol}://${encodeURIComponent(credentials.username)}:${encodeURIComponent(credentials.password)}@${broker.host}:${broker.port}${vhostPath}`
}

export function routingKeys(
  routingKeyArg: string | undefined,
  configured: string | undefined,
  queue: string | undefined,
): string[] {
  return (routingKeyArg || configured || queue || '').split(',').map(key => key.trim())
}

export function parseJson(jsonInput: unknown): JsonValue {
  if (typeof jsonInput !== 'string') return jsonInput as JsonValue
  try { return JSON.parse(jsonInput) as JsonValue } catch { return jsonInput as JsonValue }
}

export function parseJsonObject(jsonInput: unknown): JsonObject {
  let output: JsonValue
  try { output = parseJson(jsonInput) } catch { return {} }
  return typeof output === 'object' && output !== null && !Array.isArray(output)
    ? output as JsonObject
    : {}
}
