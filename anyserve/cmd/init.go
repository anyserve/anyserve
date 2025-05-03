package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/anyserve/anyserve/pkg/meta"
	"github.com/google/uuid"
	"github.com/urfave/cli/v3"
	"go.uber.org/zap"
)

func cmdInit() *cli.Command {
	return &cli.Command{
		Name:      "init",
		Usage:     "Initialize Anyserve system",
		Action:    initFunc,
		ArgsUsage: "META-URI NAME",
		Flags:     expandFlags(initFlags()),
		Description: `
Create a new Anyserve system. 
META-URI is used to set up the metadata engine (embedded NATS, sqlite, redis, NATS, etc.) ,
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

	metaURI := cmd.Args().Get(0)
	name := cmd.Args().Get(1)

	if metaURI == "" || name == "" {
		return fmt.Errorf("META-URI and NAME are required")
	}

	m, err := meta.NewMeta(metaURI)
	if err != nil {
		return fmt.Errorf("meta client: %w", err)
	}

	format, err := m.Load()
	if err == nil {
		logger.Warn("Meta already initialized. Updating now")
		format.Name = name
	} else if strings.HasPrefix(err.Error(), "backend is not formatted") {
		logger.Info("Backend is not formatted. Initializing")
		format = &meta.Format{
			Name: name,
			UUID: uuid.New().String(),
		}
	} else {
		logger.Error("Load meta", zap.Error(err))
		return fmt.Errorf("load meta: %w", err)
	}

	_ = m.Init(format, cmd.Bool("force"))

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
