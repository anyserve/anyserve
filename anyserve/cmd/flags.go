package cmd

import "github.com/urfave/cli/v3"

func globalFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"debug", "v"},
			Usage:   "enable debug log",
		},
		&cli.StringFlag{
			Name:  "log-level",
			Usage: "set log level (debug, info, warn, error, error)",
			Value: "info",
		},
	}
}

func expandFlags(compoundFlags ...[]cli.Flag) []cli.Flag {
	var flags []cli.Flag
	for _, flag := range compoundFlags {
		flags = append(flags, flag...)
	}
	return flags
}
