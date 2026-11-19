# Contributing to MiniRedis

Thank you for your interest in contributing to MiniRedis! MiniRedis is an educational, in-memory key-value database built from scratch in Go.

## Guiding Principles

1. **Learning First**: Code readability, clarity, and architectural understanding take priority over clever micro-optimizations.
2. **Standard Library Only**: We avoid external third-party dependencies unless explicitly discussed and justified.
3. **Zero Data Races**: Every new feature must be tested under Go's race detector (`go test -race ./...`).

## Development Workflow

1. Fork the repository and create a descriptive feature branch:
   ```bash
   git checkout -b feature/my-new-feature
   ```

2. Verify existing tests pass before making changes:
   ```bash
   go test -race ./...
   ```

3. Ensure all Go code is properly formatted:
   ```bash
   gofmt -s -w .
   ```

4. Run static analysis:
   ```bash
   go vet ./...
   ```

5. Submit a pull request with an explanation of:
   - What problem the change solves.
   - The execution flow of the changes.
   - Any new test cases added.
