// Package version holds build-time version information injected by the linker.
package version

// These values are overridden with -ldflags "-X ..." in release builds.
var (
	Version   = "dev"
	Commit    = "none"
	BuildDate = "unknown"
)

// String returns a short human-readable version.
func String() string {
	return Version + " (" + Commit + ")"
}
