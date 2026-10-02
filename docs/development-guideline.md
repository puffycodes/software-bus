# Development and Testing Approach

## Development

### Spec first
- Keep one design spec per area in `docs/design/`, each mapping to one module.
- Keep specs and code in step both ways: a spec change means a code change, and the reverse. Change the spec, code, tests and test catalogue together.
- Write down *why* a rule exists when it looks simplifiable, so nobody "cleans it up" later.

### Structure
- Build layers by composition: a higher layer contains a lower-layer object and plugs in its behaviour through callbacks rather than subclassing.
- Make behaviour replaceable through registered callbacks instead of hardcoding it.
- Keep command-line scripts thin: argument parsing wraps a testable `run(...)` function, and shared options and helpers live in one module.

### One path per concern
- Send every failure of a kind through a single handler instead of handling it at each call site.
- Reuse shared helpers rather than re-inlining them.
- Report script failures through one error base class that a single wrapper turns into a one-line message and a non-zero exit status.

### Error handling
- A failure in user-supplied code (e.g. a callback) loses only that one unit of work, never the connection or process.
- Malformed input raises a specific error, which is logged and skipped.
- Validate before changing state, so a rejected input leaves nothing behind.

### Portability and dependencies
- State the supported runtime versions and operating systems, and test on the oldest and newest.
- Keep runtime dependencies to a minimum. Make optional ones extras, imported only where they're used.
- Document platform- and version-specific behaviour where the code handles it.

### Documentation
- Keep the README (usage) and contributor/agent notes (architecture, conventions) current with each feature.
- Build features in small steps, each one complete with spec, code, tests and docs.

## Testing

### Real over mocked
- Test against real resources (e.g. real sockets on OS-assigned ports) where practical. Put the setup helpers in a shared test module.
- Use fakes only where a real resource is impractical. Keep that logic pure so it can be tested with fake data, and isolate the code that touches the real resource.
- Tests that need an optional dependency skip themselves when it's missing.

### Timing
- Bound every wait with a timeout so a broken test fails instead of hanging.
- To check that something does *not* happen, wait briefly and expect a timeout.
- Expose timing constants as attributes that tests can shrink.

### Coverage
- Test each layer separately: core classes, shared helpers, and each script's `run()` function.
- Test options that every script must support across all scripts.
- Use parametrized, table-driven tests for input variations.
- Give every subtle fix or safeguard a regression test that fails without it.
- Check output and logging explicitly (e.g. `capsys`, `caplog`).

### Test catalogue
- Keep `docs/test-cases.md` listing every automated test in plain English, grouped by file with counts. Update it in the same change as the tests.
- Describe tests that can't be automated in a "Manual tests" section: what each needs, the steps, the expected result, clean-up, and the date and result of its last run. When a change affects one, flag it for re-running.
