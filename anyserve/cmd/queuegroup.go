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

func cmdQueueGroup() *cli.Command {
	return &cli.Command{
		Name:      "queuegroup",
		Usage:     "Queue group operations",
		Action:    queueGroupFunc,
		ArgsUsage: "META-URI NAME",
		Flags:     expandFlags(queueGroupFlags()),
		Description: `
Queue Group operations.

Examples:
# Use Redis
$ anyserve queuegroup redis://localhost create --name mymodels --index "@model"
$ anyserve queuegroup redis://localhost create --name priority --index "@priority,@model"

# Use NATS as Streaming Engine
$ anyserve queuegroup redis://localhost create --name priority --index "@priority,@model" --streaming nats://localhost:4222

# Use S3 as Storage Engine
$ anyserve queuegroup redis://localhost create --name priority --index "@priority,@model" --storage s3://mybucket
`,
	}
}

func queueGroupFunc(ctx context.Context, cmd *cli.Command) error {
	setup(cmd)

	metaURI := cmd.Args().Get(0)

	if metaURI == "" {
		return fmt.Errorf("META-URI is required")
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

func queueGroupFlags() []cli.Flag {

	return []cli.Flag{
		&cli.BoolFlag{
			Name:  "force",
			Usage: "Force initialization",
			Value: false,
		},
	}
}
