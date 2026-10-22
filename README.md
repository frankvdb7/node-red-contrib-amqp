# Node-RED Contrib AMQP

This repository provides a set of Node-RED nodes for interacting with RabbitMQ and other AMQP-compliant message brokers. It is a fork of `@Stormpass/node-red-contrib-amqp`, adapted to work with modern RabbitMQ features like Quorum Queues.

## Features

- **AMQP In:** Consume messages from a queue.
- **AMQP Out:** Publish messages to an exchange.
- **AMQP In Manual Ack:** Consume messages with manual acknowledgment.
- **Dynamic Virtual Host:** Change the vhost at runtime for the `amqp-out` node by setting `msg.vhost`.
- **Reconnect Support:** Supports automatic reconnect behavior and input-triggered reconnect calls for inbound nodes.

## Prerequisites

- Node.js 20 or newer (Node.js 22 or newer for development tooling)
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
  - The node keeps the original AMQP message mapped by `deliveryTag`, so manual acknowledgment can operate correctly after flow processing.

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
  - Recovery is always enabled, including when the broker is unavailable at startup.
- Login or permission errors:
  - Confirm credentials and vhost access rights on the broker.
- Queue type mismatch:
  - Ensure producer/consumer agree on queue type (`quorum` vs `classic`) and queue arguments.
- RPC timeouts:
  - Verify consumer sends replies using `replyTo` and `correlationId`.
  - Increase `rpcTimeoutMilliseconds` for slower consumers.

### Automatic recovery

The nodes use amqplib native recovery with unlimited retries and exponential backoff
from 1 second to 30 seconds with 20% jitter. Each successful connection recreates
channels, topology and consumers before reporting Connected or healthy. Nodes sharing
a broker and vhost share one recovering connection. Closing a node releases only its
resources; the last node closing cancels native recovery.

A topology setup failure marks only the affected node as errored. Healthy nodes on
the shared connection remain usable. Failed setup is attempted again on the next
native connection recovery or a manual reconnect request.

Publishing while disconnected fails through the node's error/completion path. Recovery
does not replay messages or guarantee delivery of a publish whose confirmation was
interrupted. Applications must handle such uncertain outcomes themselves.

A runtime `msg.vhost` switch waits up to 10 seconds for readiness. If the destination
remains unavailable, the input completes with an error and the message is not
published. Native recovery continues for the destination, and closing the node
cancels its pending readiness waits.

Inbound `msg.payload.reconnectCall` remains a manual recovery request. While connected,
it closes the shared transport to trigger native recovery for its broker/vhost. During
an outage it leaves the existing native retry schedule running. Completion acknowledges
the request, not broker readiness.

Older flows with the removed reconnect checkbox still load; the saved property is
ignored. Recovery is now always enabled.

### Migration Notes

When importing older flows, ensure node configs include current fields:

- `amqp-out`: `exchangeRoutingKeyType`, `waitForConfirms`
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
- `npm run fallow`: Report unused code, dependency issues, duplication, and complexity hotspots.
- `npm run fallow:audit`: Fail only when a change introduces new Fallow findings; this runs in CI.
- `npm test`: Run the fast unit suite without Docker.
- `npm run test:integration`: Build and run the compiled nodes against an ephemeral RabbitMQ container.
- `npm run test:watch`: Run tests in watch mode.
- `npm run test:cov`: Generate a test coverage report.

## Testing Notes

Integration tests require Node.js 22 or newer and a running Docker-compatible
daemon with Linux containers. Run `npm install`, then `npm run test:integration`.
The first run pulls the pinned `rabbitmq:4.3.6-management` image. No broker,
credentials, or fixed host ports need to be configured. Testcontainers starts one
broker per suite and removes it on teardown; queues and exchanges use unique names.
Set `DEBUG=testcontainers*` for container startup diagnostics.

The separate `RabbitMQ integration` GitHub Actions workflow runs on pull requests
to `main` and pushes to `main`, with separate jobs for Node-RED 4 and 5 on Node.js
24. Each job installs the latest release of its Node-RED major without changing
the lockfile. Repository branch protection must require both
`integration (Node-RED 4)` and `integration (Node-RED 5)` checks to enforce them as
merge gates.

The initial suite covers real publish/consume, publisher confirms, classic and
quorum declarations, consume-only topology, manual ack and nack/requeue, health
and consumer shutdown. Delivery, publisher confirmation and acknowledgement are
separate assertions; these tests do not promise exactly-once processing. Known
reliability regressions are explicitly pending with issue references. RPC,
dead-letter rejection and outage/recovery scenarios remain follow-up coverage
under #192.

- The test suite currently has known pre-existing failures.
- Changes should not add new test failures.
- For Node-RED node tests, prefer helper event assertions (for example `node.on('call:error', ...)`) over direct Sinon stubs on node methods to avoid instrumentation conflicts.

## Contributing

Contributions are welcome! Please open an issue or pull request to discuss any changes.

## License

ISC
