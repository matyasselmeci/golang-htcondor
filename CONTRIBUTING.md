# Contributing to golang-htcondor

Thank you for your interest in contributing to golang-htcondor!

## Development Setup

1. Clone the repository:
```bash
git clone https://github.com/bbockelm/golang-htcondor.git
cd golang-htcondor
```

2. Ensure you have Go 1.21 or later installed:
```bash
go version
```

3. Build the project:
```bash
go build ./...
```

4. Run tests:
```bash
go test ./...
```

## Project Structure

- `collector.go` - Collector client implementation
- `schedd.go` - Schedd client implementation
- `htcondor.go` - Main package file
- `*_test.go` - Test files
- `examples/` - Example usage code

## Dependencies

The project requires:
- `github.com/bbockelm/cedar` - Cedar protocol bindings
- `github.com/PelicanPlatform/classad` - ClassAd language implementation

These dependencies are currently under development. The code uses placeholder types until the dependencies are fully available.

## Testing

Run all tests:
```bash
go test ./...
```

Run tests with coverage:
```bash
go test -cover ./...
```

## Code Style

- Follow standard Go conventions and formatting (use `gofmt`)
- Write clear, concise comments
- Include tests for new functionality
- Keep API compatibility with HTCondor Python bindings where possible

## Submitting Changes

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and ensure they pass
5. Submit a pull request

## Questions?

Feel free to open an issue for questions or discussions.
