//go:build unix

package config

import (
	"runtime"
	"strings"

	"golang.org/x/sys/unix"
)

// getUnameValues returns uname -m and uname -s equivalent values using syscall
func getUnameValues() (arch string, opsys string) {
	var utsname unix.Utsname
	if err := unix.Uname(&utsname); err != nil {
		// Fallback to ARCH and OPSYS
		return goArchToHTCondorArch(runtime.GOARCH), goOSToHTCondorOS(runtime.GOOS)
	}

	// Convert byte arrays to strings
	arch = strings.ToUpper(strings.TrimRight(string(utsname.Machine[:]), "\x00"))
	opsys = strings.ToUpper(strings.TrimRight(string(utsname.Sysname[:]), "\x00"))

	return arch, opsys
}
