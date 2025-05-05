package cmd

import (
	"github.com/urfave/cli/v3"
)

func cmdQueue() *cli.Command {
	return &cli.Command{
		Name:      "queue",
		Usage:     "Queue operations",
		ArgsUsage: "META-URI",
		Flags:     []cli.Flag{},
		Commands: []*cli.Command{
			queueCreateCommand(),
			queueListCommand(),
			queueStatsCommand(),
			queueDeleteCommand(),
		},
		Description: `
Queue operations.
`,
	}
}
