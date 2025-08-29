# AGENTS

These instructions apply to the entire repository.

## Development
- Write source code in TypeScript under `src/`; generated files in `build/` are not tracked.
- Target Node.js 20 or newer and modern ECMAScript features.
- Keep modules small and focused; aim for roughly 200 lines or fewer.

## Code Style
- Use the existing ESLint and Prettier configuration.
- Format and lint changes with `npm run lint`.

## Testing
- Add unit tests in `test/` for any code change.
- Tests should cover multiple scenarios and include success and failure paths.
- Run `npm test` before committing code.
- Documentation-only changes may skip tests and lint.

## Git
- Follow the [Conventional Commits](https://www.conventionalcommits.org/) specification for commit messages.
- Use descriptive commit messages and avoid amending published commits.
- Do not commit build artifacts or generated files.
