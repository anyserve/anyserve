package version

import "fmt"

var (
	// Version holds the semantic version (e.g., "v1.0.0"). Set at build time.
	Version = "dev"
	// GitCommit holds the short Git commit hash. Set at build time.
	GitCommit = "unknown"
	// BuildDate holds the build timestamp. Set at build time.
	BuildDate = "unknown"
	// GoVersion holds the Go version used for the build. Set at build time.
	GoVersion = "unknown"
)

func VersionString() string {
	return fmt.Sprintf("version=%s commit=%s build_date=%s go_version=%s", Version, GitCommit, BuildDate, GoVersion)
}
