# Node-RED Contrib AMQP

This repository provides a set of Node-RED nodes for interacting with RabbitMQ and other AMQP-compliant message brokers. It is a fork of `@Stormpass/node-red-contrib-amqp`, adapted to work with modern RabbitMQ features like Quorum Queues.

## Features

- **AMQP In:** Consume messages from a queue.
- **AMQP Out:** Publish messages to an exchange.
- **AMQP In Manual Ack:** Consume messages with manual acknowledgment.
- **Dynamic Virtual Host:** Change the vhost at runtime for the `amqp-out` node by setting `msg.vhost`.
- **Reconnect Support:** Supports automatic reconnect behavior and input-triggered reconnect calls for inbound nodes.

## Prerequisites

- Node.js 20 or newer
- Node-RED 4.0 or newer

## Installation

You can install the nodes using the Node-RED Palette Manager or by running the following command in your Node-RED user directory (typically `~/.node-red`):

```bash
npm install @frankvdb/node-red-contrib-amqp
```

## Usage

After installation, the following nodes will be available in your Node-RED editor:

- `amqp-in`
- `amqp-out`
- `amqp-in-manual-ack`
- `amqp-broker` (configuration node)

For more detailed information, please see the **Node Help** section in the Node-RED editor.

### Runtime Message Controls

The nodes support runtime control via `msg`:

- `amqp-out`
  - `msg.routingKey`: overrides routing key when node routing key mode is `str`.
  - `msg.vhost`: changes virtual host at runtime and reconnects with the new vhost.
  - `msg.properties`: AMQP message properties merged over configured JSON properties.
- `amqp-in` and `amqp-in-manual-ack`
  - `msg.payload.reconnectCall = true`: triggers reconnect logic from a flow.
- `amqp-in-manual-ack`
  - `msg.manualAck.ackMode`: explicit ack mode (`ack`, `ackAll`, `nack`, `nackAll`, `reject`).
  - `msg.manualAck.requeue`: used by `nack`, `nackAll`, and `reject` (defaults to `true`).
  - The node keeps the original AMQP delivery behind an internal token so acknowledgments can survive normal Node-RED message cloning. Tokens are tied to the channel that delivered the message; acknowledgments from before a reconnect are rejected locally.

### Examples

Sample flows are provided in [`examples/`](./examples):

| Example | Purpose |
|---|---|
| `AMQP General Usage.json` | Direct, topic, fanout, and headers exchange basics. |
| `AMQP Out With RPC Pattern.json` | Request/response messaging using AMQP RPC pattern. |
| `Manual Ack With Complete.json` | Manual ack flow using a complete-trigger pattern. |
| `Manual Ack With Links.json` | Manual ack flow using link nodes. |
| `Manual NAck With Links.json` | Manual nack/requeue/dead-letter control patterns. |

## Operational Notes

### Health Endpoint

The broker config node exposes `/amqp-broker/health` from Node-RED admin HTTP:

- Returns HTTP `200` when all known brokers are connected.
- Returns HTTP `503` when one or more brokers are not connected.

Example response:

```json
{
  "overallStatus": "healthy",
  "brokers": [
    {
      "id": "broker-node-id",
      "name": "Primary RabbitMQ",
      "status": "connected"
    }
  ]
}
```

### Troubleshooting

- Reconnect loops:
  - Verify host/port/TLS/vhost values and broker reachability.
  - Check whether `reconnectOnError` is enabled when desired.
- Login or permission errors:
  - Confirm credentials and vhost access rights on the broker.
- Queue type mismatch:
  - Ensure producer/consumer agree on queue type (`quorum` vs `classic`) and queue arguments.
- RPC timeouts:
  - Verify consumer sends replies using `replyTo` and `correlationId`.
  - Increase `rpcTimeoutMilliseconds` for slower consumers.

### Migration Notes

When importing older flows, ensure node configs include current fields:

- `amqp-out`: `exchangeRoutingKeyType`, `waitForConfirms`, `reconnectOnError`
- `amqp-in` and `amqp-in-manual-ack`: `queueType`, `queueArguments`
- JSON fields such as `headers` should be valid JSON strings (for example `"{}"`).

## Development

To contribute to the development of these nodes, please follow these steps:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/frankvdb7/node-red-contrib-amqp.git
   cd node-red-contrib-amqp
   ```

2. **Install dependencies:**
   ```bash
   npm install
   ```

3. **Build the project:**
   ```bash
   npm run build
   ```

### Development Scripts

- `npm start`: Watch for changes and automatically rebuild.
- `npm run build`: Compile the TypeScript source code.
- `npm run build:production`: Build without source maps.
- `npm run copyassets`: Copy node editor HTML and icons into the build output.
- `npm run lint`: Format code and check for linting errors.
- `npm test`: Run the test suite.
- `npm run test:watch`: Run tests in watch mode.
- `npm run test:cov`: Generate a test coverage report.

## Testing Notes

- The test suite currently has known pre-existing failures.
- Changes should not add new test failures.
- For Node-RED node tests, prefer helper event assertions (for example `node.on('call:error', ...)`) over direct Sinon stubs on node methods to avoid instrumentation conflicts.

## Contributing

Contributions are welcome! Please open an issue or pull request to discuss any changes.

## License

ISC
