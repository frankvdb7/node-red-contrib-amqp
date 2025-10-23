# AGENTS

These instructions apply to the entire repository.

## Development
- Write source code in TypeScript under `src/`; generated files in `build/` are not tracked.
- The project targets Node.js 20 or newer.
- Keep modules small and focused; aim for roughly 200 lines or fewer.
- Run `npm install` to set up the development environment and install dependencies.
- Use `npm run build` to compile the TypeScript code.

## Code Style
- Code style is enforced by ESLint and Prettier.
- Run `npm run lint` to format code and check for linting errors before committing.

## Testing
- The project uses Mocha, Chai, and Sinon for testing, along with the `node-red-node-test-helper` library.
- Add unit tests in `test/` for any code change.
- Run the full test suite with `npm test`.
- Generate a test coverage report with `npm run test:cov`.
- The test suite has pre-existing failures. New changes should not introduce additional failures.

### Testing Node-RED Nodes
- When testing Node-RED nodes, direct stubbing of node methods (e.g., `node.error()`) with Sinon can conflict with `node-red-node-test-helper`'s instrumentation. A more reliable pattern is to use the helper's event-based assertions, like `node.on('call:error', ...)`, to verify method calls and avoid race conditions.
- When testing asynchronous operations that do not use `async/await`, use a `setTimeout` with a `done` callback to prevent race conditions and ensure assertions are checked after the operation completes.
- The `amqp-in-manual-ack` node requires the original AMQP message for acknowledgment. The `Amqp` class caches the assembled message in a Map, keyed by its `deliveryTag`, to preserve the message across the Node-RED flow so it can be retrieved for acknowledgment operations.

## Git
- Follow the [Conventional Commits](https.org/conventionalcommits.org/) specification for commit messages.
- Do not commit build artifacts or generated files from the `build/` directory.
