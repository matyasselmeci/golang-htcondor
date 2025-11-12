package htcondor

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// TestTransferCommandConstants verifies the transfer command enum values
// match HTCondor's C++ implementation.
func TestTransferCommandConstants(t *testing.T) {
	tests := []struct {
		name     string
		cmd      TransferCommand
		expected int32
	}{
		{"Finished", CommandFinished, 0},
		{"XferFile", CommandXferFile, 1},
		{"EnableEncryption", CommandEnableEncryption, 2},
		{"DisableEncryption", CommandDisableEncryption, 3},
		{"XferX509", CommandXferX509, 4},
		{"DownloadUrl", CommandDownloadUrl, 5},
		{"Mkdir", CommandMkdir, 6},
		{"Other", CommandOther, 999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int32(tt.cmd) != tt.expected {
				t.Errorf("TransferCommand %s = %d, want %d", tt.name, tt.cmd, tt.expected)
			}
		})
	}
}

// TestFileTransferItemMetadata tests serialization and deserialization of
// file metadata using ClassAds.
func TestFileTransferItemMetadata(t *testing.T) {
	// Create a test file item
	originalItem := FileTransferItem{
		SrcPath:      "/local/path/test.txt",
		DestPath:     "test.txt",
		FileSize:     1024,
		FileMode:     0644,
		Checksum:     "a1b2c3d4e5f6789",
		ChecksumType: "SHA256",
	}

	// Serialize to ClassAd
	metadataAd := classad.New()
	_ = metadataAd.Set("FileName", originalItem.DestPath)
	_ = metadataAd.Set("FileSize", originalItem.FileSize)
	_ = metadataAd.Set("FileMode", int64(originalItem.FileMode))
	_ = metadataAd.Set("Checksum", originalItem.Checksum)
	_ = metadataAd.Set("ChecksumType", originalItem.ChecksumType)

	// Deserialize from ClassAd
	reconstructedItem := &FileTransferItem{}

	if fileName := metadataAd.EvaluateAttr("FileName"); !fileName.IsError() && fileName.IsString() {
		if str, err := fileName.StringValue(); err == nil {
			reconstructedItem.DestPath = str
		}
	}

	if fileSize := metadataAd.EvaluateAttr("FileSize"); !fileSize.IsError() && fileSize.IsInteger() {
		if num, err := fileSize.IntValue(); err == nil {
			reconstructedItem.FileSize = num
		}
	}

	if fileMode := metadataAd.EvaluateAttr("FileMode"); !fileMode.IsError() && fileMode.IsInteger() {
		if num, err := fileMode.IntValue(); err == nil {
			reconstructedItem.FileMode = uint32(num)
		}
	}

	if checksum := metadataAd.EvaluateAttr("Checksum"); !checksum.IsError() && checksum.IsString() {
		if str, err := checksum.StringValue(); err == nil {
			reconstructedItem.Checksum = str
		}
	}

	if checksumType := metadataAd.EvaluateAttr("ChecksumType"); !checksumType.IsError() && checksumType.IsString() {
		if str, err := checksumType.StringValue(); err == nil {
			reconstructedItem.ChecksumType = str
		}
	}

	// Verify round-trip
	if reconstructedItem.DestPath != originalItem.DestPath {
		t.Errorf("DestPath mismatch: got %q, want %q", reconstructedItem.DestPath, originalItem.DestPath)
	}

	if reconstructedItem.FileSize != originalItem.FileSize {
		t.Errorf("FileSize mismatch: got %d, want %d", reconstructedItem.FileSize, originalItem.FileSize)
	}

	if reconstructedItem.FileMode != originalItem.FileMode {
		t.Errorf("FileMode mismatch: got %o, want %o", reconstructedItem.FileMode, originalItem.FileMode)
	}

	if reconstructedItem.Checksum != originalItem.Checksum {
		t.Errorf("Checksum mismatch: got %q, want %q", reconstructedItem.Checksum, originalItem.Checksum)
	}

	if reconstructedItem.ChecksumType != originalItem.ChecksumType {
		t.Errorf("ChecksumType mismatch: got %q, want %q", reconstructedItem.ChecksumType, originalItem.ChecksumType)
	}
}

// TestFileTransferClientCreation tests basic client creation
func TestFileTransferClientCreation(t *testing.T) {
	client := NewFileTransferClient("localhost", 9618, "test-key-123")

	if client.daemonAddr != "localhost" {
		t.Errorf("daemonAddr = %q, want %q", client.daemonAddr, "localhost")
	}

	if client.daemonPort != 9618 {
		t.Errorf("daemonPort = %d, want %d", client.daemonPort, 9618)
	}

	if client.transKey != "test-key-123" {
		t.Errorf("transKey = %q, want %q", client.transKey, "test-key-123")
	}
}

// TestFileTransferServerCreation tests basic server creation
func TestFileTransferServerCreation(t *testing.T) {
	server := NewFileTransferServer("test-key-456", "/var/lib/condor/spool")

	if server.transKey != "test-key-456" {
		t.Errorf("transKey = %q, want %q", server.transKey, "test-key-456")
	}

	if server.baseDir != "/var/lib/condor/spool" {
		t.Errorf("baseDir = %q, want %q", server.baseDir, "/var/lib/condor/spool")
	}
}

// TestGetCommandCodes verifies the CEDAR command codes
func TestGetCommandCodes(t *testing.T) {
	codes := GetCommandCodes()

	// FILETRANS_BASE is 61000 in commands.go
	expectedUpload := 61000   // FILETRANS_BASE + 0
	expectedDownload := 61001 // FILETRANS_BASE + 1

	if codes.Upload != expectedUpload {
		t.Errorf("Upload command code = %d, want %d", codes.Upload, expectedUpload)
	}

	if codes.Download != expectedDownload {
		t.Errorf("Download command code = %d, want %d", codes.Download, expectedDownload)
	}
}

// TestFileTransferItemEmpty tests handling of empty/minimal metadata
func TestFileTransferItemEmpty(t *testing.T) {
	// Create an item with minimal metadata
	item := FileTransferItem{
		DestPath: "file.txt",
		FileSize: 0, // Empty file
		FileMode: 0644,
	}

	// Serialize to ClassAd
	ad := classad.New()
	_ = ad.Set("FileName", item.DestPath)
	_ = ad.Set("FileSize", item.FileSize)
	_ = ad.Set("FileMode", int64(item.FileMode))
	// Note: Checksum fields intentionally omitted

	// Deserialize
	reconstructed := &FileTransferItem{}
	if fileName := ad.EvaluateAttr("FileName"); !fileName.IsError() && fileName.IsString() {
		if str, err := fileName.StringValue(); err == nil {
			reconstructed.DestPath = str
		}
	}

	if fileSize := ad.EvaluateAttr("FileSize"); !fileSize.IsError() && fileSize.IsInteger() {
		if num, err := fileSize.IntValue(); err == nil {
			reconstructed.FileSize = num
		}
	}

	if fileMode := ad.EvaluateAttr("FileMode"); !fileMode.IsError() && fileMode.IsInteger() {
		if num, err := fileMode.IntValue(); err == nil {
			reconstructed.FileMode = uint32(num)
		}
	}

	// Verify
	if reconstructed.DestPath != item.DestPath {
		t.Errorf("DestPath mismatch: got %q, want %q", reconstructed.DestPath, item.DestPath)
	}

	if reconstructed.FileSize != item.FileSize {
		t.Errorf("FileSize mismatch: got %d, want %d", reconstructed.FileSize, item.FileSize)
	}

	if reconstructed.FileMode != item.FileMode {
		t.Errorf("FileMode mismatch: got %o, want %o", reconstructed.FileMode, item.FileMode)
	}

	// Checksum should be empty
	if reconstructed.Checksum != "" {
		t.Errorf("Expected empty Checksum, got %q", reconstructed.Checksum)
	}

	if reconstructed.ChecksumType != "" {
		t.Errorf("Expected empty ChecksumType, got %q", reconstructed.ChecksumType)
	}
}
