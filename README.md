# Node-RED Contrib AMQP

This repository provides a set of Node-RED nodes for interacting with RabbitMQ and other AMQP-compliant message brokers. It is a fork of `@Stormpass/node-red-contrib-amqp`, adapted to work with modern RabbitMQ features like Quorum Queues.

## Features

- **AMQP In:** Consume messages from a queue.
- **AMQP Out:** Publish messages to an exchange.
- **AMQP In Manual Ack:** Consume messages with manual acknowledgment.
- **Dynamic Virtual Host:** Change the vhost at runtime for the `amqp-out` node by setting `msg.vhost`.

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
- `npm run lint`: Format code and check for linting errors.
- `npm test`: Run the test suite.
- `npm run test:cov`: Generate a test coverage report.

## Contributing

Contributions are welcome! Please open an issue or pull request to discuss any changes.

## License

ISC
