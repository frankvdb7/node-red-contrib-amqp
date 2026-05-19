# Contribution Guidelines

These instructions apply to the entire repository.

## Development
- Write source code in TypeScript under `src/`; generated files in `build/` are not tracked.
- Target Node.js 20 or newer and modern ES features.
- Keep modules small and focused; favor clarity over cleverness.
- Install dependencies with `npm install`.
- Build with `npm run build`.

## Code Style
- Use the existing ESLint and Prettier configuration.
- Format and lint your changes with `npm run lint`.

## Testing
- Add unit tests in `test/` for any code change.
- Tests should cover multiple scenarios and handle both success and failure paths.
- Run `npm test` before committing code.
- Generate coverage with `npm run test:cov` when needed.
- The test suite currently has pre-existing failures; changes must not introduce additional failures.
- Documentation-only changes may skip test and lint runs.

### Node-RED Node Test Guidance

- Avoid direct Sinon stubs on Node-RED node methods like `node.error()` because they can conflict with `node-red-node-test-helper` instrumentation.
- Prefer helper event assertions, for example `node.on('call:error', ...)`.
- For async flows that do not use `async/await`, use `setTimeout` plus `done` to avoid race conditions.
- `amqp-in-manual-ack` acknowledgment relies on the original AMQP message; the `Amqp` class caches assembled messages by `deliveryTag` for later manual ack operations.

## Git
- Follow the [Conventional Commits](https://www.conventionalcommits.org/) specification for commit messages.
- Use descriptive commit messages and avoid amending published commits.
- Do not commit build artefacts or generated files.
