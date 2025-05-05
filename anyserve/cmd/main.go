package cmd

import (
	"context"
	"fmt"
	"log"

	"github.com/anyserve/anyserve/internal/version"
	"github.com/anyserve/anyserve/pkg/utils"

	"github.com/urfave/cli/v3"
)

func init() {
	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Aliases: []string{"V"},
		Usage:   "print version information and exit",
	}
}

func Main(args []string) error {

	cmd := &cli.Command{
		Name:  "anyserve",
		Usage: "Serve anywhere. Infer everywhere. Anyserve.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return cli.ShowAppHelp(cmd)
		},
		Copyright:       fmt.Sprintf("Apache License 2.0 @ %s", "https://github.com/anyserve/anyserve"),
		HideHelpCommand: true,
		Version:         version.VersionString(),
		Flags:           globalFlags(),
		Commands: []*cli.Command{
			cmdInit(),
			cmdQueueGroup(),
			cmdServe(),
		},
	}

	if err := cmd.Run(context.Background(), args); err != nil {
		log.Fatal(err)
	}
	return nil
}

func setup(cmd *cli.Command) {
	if cmd.Bool("verbose") {
		utils.SetLevel("debug")
	} else {
		utils.SetLevel(cmd.String("log-level"))
	}
}
