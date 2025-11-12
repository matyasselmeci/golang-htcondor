# Dependency Setup Instructions

This project requires two external dependencies that need to be set up:

## Required Dependencies

1. **github.com/bbockelm/cedar** - Cedar protocol bindings for HTCondor
2. **github.com/PelicanPlatform/classad** - ClassAd language implementation

## Setup Options

### Option 1: Using Published Modules (Recommended)

Once the dependencies are published to their respective repositories with proper Go module support:

```bash
# Remove the TODO comments from go.mod
# Update go.mod with actual versions:
go get github.com/bbockelm/cedar@latest
go get github.com/PelicanPlatform/classad@latest
go mod tidy
```

### Option 2: Using Local Replacements (Development)

If you have local copies of the dependencies:

1. Clone the dependencies to sibling directories:
```bash
cd /path/to/projects
git clone https://github.com/bbockelm/cedar.git
git clone https://github.com/PelicanPlatform/classad.git
```

2. Update `go.mod` to use local replacements:
```go
require (
    github.com/bbockelm/cedar v0.0.0
    github.com/PelicanPlatform/classad v0.0.0
)

replace github.com/bbockelm/cedar => ../cedar
replace github.com/PelicanPlatform/classad => ../classad
```

3. Run:
```bash
go mod tidy
```

### Option 3: Fork and Publish

If the dependencies don't have proper Go module support:

1. Fork the repositories
2. Add proper `go.mod` files
3. Tag releases
4. Update this project's `go.mod` to point to your forks

## Current Status

The project currently uses placeholder types (e.g., `type ClassAd map[string]interface{}`) to allow compilation without the actual dependencies. Once the dependencies are available:

1. Uncomment the import statements in `collector.go` and `schedd.go`
2. Remove the placeholder `ClassAd` type definition
3. Update `go.mod` with proper dependency versions
4. Implement the actual protocol logic using cedar and classad packages

## Verifying Setup

After setting up dependencies, verify everything works:

```bash
go build ./...
go test ./...
cd examples/basic && go build
```
