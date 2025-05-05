package cmd

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"
)

func cmdQueue() *cli.Command {
	return &cli.Command{
		Name:      "queue",
		Usage:     "Queue operations",
		ArgsUsage: "META-URI",
		Flags:     expandFlags(queueFlags()),
		Commands: []*cli.Command{
			{
				Name:      "list",
				Usage:     "List all queues",
				ArgsUsage: "META-URI",
				Action:    queueListFunc,
				Flags:     expandFlags(queueFlags()),
			},
			{
				Name:      "create",
				Usage:     "Create a new queue",
				ArgsUsage: "META-URI",
				Action:    queueCreateFunc,
				Flags:     expandFlags(queueCreateFlags()),
			},
			{
				Name:      "status",
				Usage:     "Get queue status",
				ArgsUsage: "META-URI NAME",
				Action:    queueStatusFunc,
				Flags:     expandFlags(queueFlags()),
			},
			{
				Name:      "delete",
				Usage:     "Delete a queue",
				ArgsUsage: "META-URI NAME",
				Action:    queueDeleteFunc,
				Flags:     expandFlags(queueFlags()),
			},
		},
		Description: `
Queue operations.

Examples:
# Use Redis
$ anyserve queue redis://localhost create --name myqueue --index "@model"
$ anyserve queue redis://localhost create --name priorityqueue --index "@priority,@model"

# Use NATS as Streaming Engine
$ anyserve queue redis://localhost create --name priorityqueue --index "@priority,@model" --streaming nats://localhost:4222

# Use S3 as Storage Engine
$ anyserve queue redis://localhost create --name priorityqueue --index "@priority,@model" --storage s3://mybucket
`,
	}
}

func queueListFunc(ctx context.Context, cmd *cli.Command) error {
	setup(cmd)

	metaURI := cmd.Args().Get(0)
	if metaURI == "" {
		return fmt.Errorf("META-URI is required")
	}

	logger.Sugar().Infof("Listing queues for %s", metaURI)
	// TODO: Implement list queues functionality

	return nil
}

func queueCreateFunc(ctx context.Context, cmd *cli.Command) error {
	setup(cmd)

	metaURI := cmd.Args().Get(0)
	if metaURI == "" {
		return fmt.Errorf("META-URI is required")
	}

	name := cmd.String("name")
	if name == "" {
		return fmt.Errorf("queue name is required")
	}

	// TODO: Implement create queue functionality with indexing, streaming, and storage options

	return nil
}

func queueStatusFunc(ctx context.Context, cmd *cli.Command) error {
	setup(cmd)

	metaURI := cmd.Args().Get(0)
	if metaURI == "" {
		return fmt.Errorf("META-URI is required")
	}

	name := cmd.Args().Get(1)
	if name == "" {
		return fmt.Errorf("queue NAME is required")
	}

	// TODO: Implement queue status functionality

	return nil
}

func queueDeleteFunc(ctx context.Context, cmd *cli.Command) error {
	setup(cmd)

	metaURI := cmd.Args().Get(0)
	if metaURI == "" {
		return fmt.Errorf("META-URI is required")
	}

	name := cmd.Args().Get(1)
	if name == "" {
		return fmt.Errorf("queue NAME is required")
	}

	// TODO: Implement queue delete functionality

	return nil
}

func queueFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:  "force",
			Usage: "Force initialization",
			Value: false,
		},
	}
}

func queueCreateFlags() []cli.Flag {
	return append(queueFlags(),
		&cli.StringFlag{
			Name:  "name",
			Usage: "Queue name",
		},
		&cli.StringFlag{
			Name:  "index",
			Usage: "Index fields (comma separated)",
		},
		&cli.StringFlag{
			Name:  "streaming",
			Usage: "Streaming engine URI",
		},
		&cli.StringFlag{
			Name:  "storage",
			Usage: "Storage engine URI",
		},
	)
}
