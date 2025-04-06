package cmd

import (
	"context"
	"fmt"

	"github.com/anyserve/anyserve/pkg/meta"
	"github.com/urfave/cli/v3"
)

func cmdInit() *cli.Command {
	return &cli.Command{
		Name:      "init",
		Usage:     "Initialize Anyserve system",
		Action:    initFunc,
		ArgsUsage: "META-URL NAME",
		Flags:     expandFlags(initFlags()),
		Description: `
Create a new Anyserve system. 
META-URL is used to set up the metadata engine (embedded NATS, sqlite, redis, NATS, etc.) ,
NAME is the name of the system.

Examples:
# Use embedded NATS
$ anyserve init embedded_nats:///tmp/nats myserve

# Use SQLite
$ anyserve init sqlite://myjfs.db myserve

# Use Redis
$ anyserve init redis://localhost myserve

# Use NATS
$ anyserve init nats://localhost:4222 myserve

`,
	}
}

func initFunc(ctx context.Context, cmd *cli.Command) error {
	setup(cmd)

	metaURL := cmd.Args().Get(0)
	name := cmd.Args().Get(1)

	if metaURL == "" || name == "" {
		return fmt.Errorf("META-URL and NAME are required")
	}

	m, err := meta.NewMeta(metaURL)
	if err != nil {
		return fmt.Errorf("failed to create meta: %w", err)
	}

	m.Init(meta.Format{
		Name: name,
	}, cmd.Bool("force"))

	return nil
}

func initFlags() []cli.Flag {

	return []cli.Flag{
		&cli.BoolFlag{
			Name:  "force",
			Usage: "Force initialization",
			Value: false,
		},
	}
}
