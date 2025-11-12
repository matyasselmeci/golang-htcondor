# golang-htcondor Project Instructions

This project provides a Go client library for HTCondor software that mimics the Python bindings API.

## Project Structure
- Uses cedar protocol bindings from github.com/bbockelm/cedar
- Uses classad language implementation from github.com/PelicanPlatform/classad
- Implements collector and schedd functionality

## Key Features
- Query collector for daemon ads
- Advertise to collector
- Locate daemons
- Query schedd for jobs

## Development Guidelines
- Follow Go best practices and conventions
- Maintain API compatibility with HTCondor Python bindings where possible
- Use structured logging and proper error handling
- Include comprehensive tests and documentation
