# HTCondor File Transfer Demo

This example demonstrates how to use the HTCondor file transfer protocol to upload files to a schedd using cedar v0.0.2+ APIs.

## Overview

This implementation uses the following CEDAR APIs (available in cedar v0.0.2+):

- **Stream.PutFile()**: Efficient file streaming (no need to load entire file to memory)
- **Stream.PutSecret()**: Secure transfer key transmission with automatic encryption
- **Stream.GetFile()**: Efficient file receiving for downloads
- **Stream.GetSecret()**: Secure transfer key reception

### Current Limitations

- **No StartCommand API**: Manually sends the FILETRANS_UPLOAD command code as int32 (workaround in place until CEDAR adds higher-level command API)
- **Hardcoded Transfer Key**: Uses a demo transfer key (real usage would get this from job submission response)

The implementation demonstrates the complete protocol flow with production-ready file streaming and secret handling.

## Building

```bash
cd examples/file_transfer_demo
go build
```

## Usage

```bash
./file_transfer_demo <schedd_host> <schedd_port> <file_path>
```

### Example

```bash
# Upload a test file to a local HTCondor schedd
./file_transfer_demo localhost 9618 /tmp/testfile.txt

# Upload to a remote schedd (e.g., CHTC)
./file_transfer_demo submit.chtc.wisc.edu 9618 mydata.csv
```

## Protocol Flow

The demo implements the following HTCondor file transfer protocol:

1. **Gather File Metadata**
   - Read file metadata (size, permissions)
   - Calculate SHA256 checksum via streaming

2. **Connect to Schedd**
   - Establish TCP connection
   - Create CEDAR stream

3. **Security Handshake**
   - Negotiate authentication (SSL, Token, or None)
   - Negotiate encryption (AES or None)

4. **Send Transfer Command**
   - Send FILETRANS_UPLOAD command code (61000)

5. **Send Transfer Key**
   - Use Stream.PutSecret() for secure, encrypted transmission

6. **Send File Metadata**
   - Filename, size, permissions, checksum
   - Sent as ClassAd for structured data exchange

7. **Stream File Data**
   - Send CommandXferFile (1)
   - Use Stream.PutFile() for efficient streaming
   - Handles arbitrarily large files without loading to memory

8. **Finish Transfer**
   - Send CommandFinished (0)

## Output

The demo provides detailed progress information:

```
🚀 HTCondor File Transfer Demo
📁 File: /tmp/testfile.txt
📡 Schedd: localhost:9618

📖 Reading file metadata...
   File: testfile.txt
   Size: 1024 bytes
   Mode: 644
   SHA256: a1b2c3d4e5f6...

🔌 Connecting to schedd...
   Connected!

🔐 Performing security handshake...
   Authentication: NONE

📤 Sending FILETRANS_UPLOAD command...
   Transfer key: demo-transfer-1699724800

📋 Sending file metadata...

📦 Sending file data...
   Sent 1024 bytes

✓ Finishing transfer...

✅ Upload completed successfully!
```

## Testing Without a Schedd

If you don't have access to an HTCondor schedd, you can test the protocol flow by:

1. Running a mock server that logs the protocol messages
2. Using `nc -l` to capture raw protocol data
3. Examining the code to understand the message structure

## Next Steps

To make this production-ready:

1. **Add CEDAR APIs**: Work with cedar maintainer to add:
   - `Daemon.StartCommand()` for proper command invocation (higher-level API)

2. **Implement Server Side**:
   - Command registration for FILETRANS_UPLOAD/DOWNLOAD
   - Server handlers in schedd

3. **Add Features**:
   - Progress reporting callbacks
   - Resume on failure with partial transfer support
   - Concurrent transfers with throttling
   - Directory support with recursive transfers
   - Checksum verification
   - Transfer statistics and monitoring

## See Also

- [FILE_TRANSFER_DESIGN.md](../../FILE_TRANSFER_DESIGN.md) - Complete protocol design
- [file_transfer.go](../../file_transfer.go) - Core implementation
- [HTCondor Manual](https://htcondor.readthedocs.io/) - Official documentation
