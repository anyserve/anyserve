package cmd

import (
	"context"

	"github.com/urfave/cli/v3"
)

func cmdInit() *cli.Command {
	return &cli.Command{
		Name:      "init",
		Usage:     "Initialize a new anyserve volume",
		Action:    initFunc,
		ArgsUsage: "META-URL",
		Flags:     expandFlags(initFlags()),
	}
}

func initFunc(ctx context.Context, cmd *cli.Command) error {
	setup(cmd)

	return nil
}

func initFlags() []cli.Flag {

	return []cli.Flag{}
}
