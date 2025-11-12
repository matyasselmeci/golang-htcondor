//go:build windows

package config

import (
	"runtime"
)

// getUnameValues returns ARCH and OPSYS values on Windows (no uname available)
func getUnameValues() (arch string, opsys string) {
	// Windows doesn't have uname, so fallback to ARCH and OPSYS
	return goArchToHTCondorArch(runtime.GOARCH), goOSToHTCondorOS(runtime.GOOS)
}
