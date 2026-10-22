# AGENTS

These instructions apply to the entire repository.

## Development
- Write source code in TypeScript under `src/`; generated files in `build/` are not tracked.
- The project targets Node.js 20 or newer.
- Keep modules small and focused; aim for roughly 200 lines or fewer.
- Run `npm install` to set up the development environment and install dependencies.
- Use `npm run build` to compile the TypeScript code.

## Code Style
- Code style is enforced by Biome.
- Run `npm run lint` to format code and check for linting errors before committing.

## Testing
- The project uses Mocha, Chai, and Sinon for testing, along with the `node-red-node-test-helper` library.
- Add unit tests in `test/` for any code change.
- Run the unit suite with `npm test`; it excludes integration tests and does not require Docker.
- Generate a unit test coverage report with `npm run test:cov`.
- The test suite has pre-existing failures. New changes should not introduce additional failures.

### RabbitMQ Integration Tests
- Run `npm run test:integration` with Node.js 22 or newer and a running Docker daemon supporting Linux containers.
- Keep integration tests in `test/integration/`. Load the compiled nodes from `build/src/` in the Node-RED test runtime and use the ephemeral Testcontainers RabbitMQ broker.
- Assert real broker delivery, acknowledgements, queue and consumer state. Do not stub AMQP connections, channels, publishing or consumption in integration tests.
- Use unique topology names, bounded polling or event assertions, and teardown that closes runtime and broker resources even when a test fails.
- CI runs separate Node-RED 4 and 5 jobs on Node.js 24. For runtime or harness changes, validate both majors using `npm install --no-save --package-lock=false node-red@4` or `node-red@5` before running the integration suite. Restore the locked dependencies afterward; do not save the temporary version override.
- Keep known regressions explicitly pending with issue references until their fixes land. Report passing and pending integration counts separately from unit test results.

### Testing Node-RED Nodes
- When testing Node-RED nodes, direct stubbing of node methods (e.g., `node.error()`) with Sinon can conflict with `node-red-node-test-helper`'s instrumentation. A more reliable pattern is to use the helper's event-based assertions, like `node.on('call:error', ...)`, to verify method calls and avoid race conditions.
- When testing asynchronous operations that do not use `async/await`, use a `setTimeout` with a `done` callback to prevent race conditions and ensure assertions are checked after the operation completes.
- The `amqp-in-manual-ack` node requires the original AMQP message for acknowledgment. The `Amqp` class caches the assembled message in a Map, keyed by its `deliveryTag`, to preserve the message across the Node-RED flow so it can be retrieved for acknowledgment operations.

## Git
- Follow the [Conventional Commits](https.org/conventionalcommits.org/) specification for commit messages.
- Do not commit build artifacts or generated files from the `build/` directory.
