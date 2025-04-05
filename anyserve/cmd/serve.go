package cmd

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"
)

func cmdServe() *cli.Command {
	return &cli.Command{
		Name:      "serve",
		Action:    servefunc,
		Usage:     "Serve a volume",
		ArgsUsage: "META-URL MOUNTPOINT",
		CustomHelpTemplate: `NAME:
   {{.Name}} - {{.Usage}}

USAGE:
   {{.HelpName}} [global options] command [command options] [arguments...]

`,
	}
}

func servefunc(ctx context.Context, cmd *cli.Command) error {
	fmt.Println("serve")
	return nil
}
