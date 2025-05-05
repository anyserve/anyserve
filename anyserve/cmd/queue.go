package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/anyserve/anyserve/pkg/meta"
	"github.com/urfave/cli/v3"
)

func cmdQueue() *cli.Command {
	return &cli.Command{
		Name:      "queue",
		Usage:     "Queue operations",
		ArgsUsage: "META-URI",
		Flags:     []cli.Flag{},
		Commands: []*cli.Command{
			{
				Name:      "list",
				Usage:     "List all queue",
				ArgsUsage: "META-URI",
				Description: `
List all queue.

Examples:
$ anyserve queue list redis://localhost
$ anyserve queue list sqlite:///tmp/anyserve.db
`,
				Action:  queueListFunc,
				Flags:   expandFlags(queueListFlags()),
				Aliases: []string{"ps"},
			},
			{
				Name:      "create",
				Usage:     "Create a new queue",
				ArgsUsage: "META-URI NAME",
				Description: `
Create a new queue.

Examples:
# Use Redis
$ anyserve queue create redis://localhost myqueue --index "@model"
$ anyserve queue create redis://localhost priorityqueue --index "@priority,@model"

# Use NATS as Streaming Engine
$ anyserve queue create redis://localhost priorityqueue --index "@priority,@model" --streaming nats://localhost:4222

# Use S3 as Storage Engine
$ anyserve queue create redis://localhost priorityqueue --index "@priority,@model" --storage s3://mybucket
`,
				Action: queueCreateFunc,
				Flags:  expandFlags(queueCreateFlags()),
			},
			{
				Name:      "stats",
				Usage:     "Get queue stats",
				ArgsUsage: "META-URI NAME",
				Action:    queueStatsFunc,
				Flags:     expandFlags(queueStatsFlags()),
			},
			{
				Name:      "remove",
				Usage:     "Remove a queue",
				ArgsUsage: "META-URI NAME",
				Action:    queueDeleteFunc,
				Flags:     expandFlags(queueDeleteFlags()),
				Description: `
Remove a queue.

Examples:
$ anyserve queue remove redis://localhost myqueue
$ anyserve queue remove redis://localhost priorityqueue
`,
				Aliases: []string{"rm"},
			},
		},
		Description: `
Queue operations.
`,
	}
}

func queueListFunc(ctx context.Context, cmd *cli.Command) error {
	setup(cmd)

	metaURI := cmd.Args().Get(0)
	if metaURI == "" {
		return fmt.Errorf("META-URI is required")
	}

	metaEngine, err := meta.NewMeta(metaURI)
	if err != nil {
		return err
	}

	format, err := metaEngine.Load()
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("name: %s, uuid: %s", format.Name, format.UUID))

	queues, err := metaEngine.ListQueues(ctx)
	if err != nil {
		return err
	}

	for _, queue := range queues {
		logger.Info(fmt.Sprintf("queue: %s", queue))
	}
	return nil
}

func queueCreateFunc(ctx context.Context, cmd *cli.Command) error {
	setup(cmd)

	metaURI := cmd.Args().Get(0)
	if metaURI == "" {
		return fmt.Errorf("META-URI is required")
	}

	name := cmd.Args().Get(1)
	if name == "" {
		return fmt.Errorf("queue name is required")
	}

	metaEngine, err := meta.NewMeta(metaURI)
	if err != nil {
		return err
	}

	format, err := metaEngine.Load()
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("name: %s, uuid: %s", format.Name, format.UUID))

	queue := meta.Queue{Name: name}

	if index := cmd.String("index"); index != "" {
		queue.Index = func(index string) string {
			var temp []string
			for field := range strings.SplitSeq(index, ",") {
				field = strings.TrimSpace(field)
				if field == "" {
					continue
				}
				// add @ prefix if not present
				if !strings.HasPrefix(field, "@") {
					logger.Warn(fmt.Sprintf("index field '%s' does not have @ prefix, auto adding it", field))
					field = fmt.Sprintf("@%s", field)
				}
				temp = append(temp, field)
			}
			return strings.Join(temp, ",")
		}(index)
	} else {
		return fmt.Errorf("index is required")
	}
	if streaming := cmd.String("streaming"); streaming != "" {
		queue.Streaming = streaming
	}
	if storage := cmd.String("storage"); storage != "" {
		queue.Storage = storage
	}

	err = metaEngine.CreateQueue(ctx, queue)
	if err != nil {
		return err
	}

	return nil
}

func queueStatsFunc(ctx context.Context, cmd *cli.Command) error {
	setup(cmd)

	metaURI := cmd.Args().Get(0)
	if metaURI == "" {
		return fmt.Errorf("META-URI is required")
	}

	name := cmd.Args().Get(1)
	if name == "" {
		return fmt.Errorf("queue NAME is required")
	}

	metaEngine, err := meta.NewMeta(metaURI)
	if err != nil {
		return err
	}

	format, err := metaEngine.Load()
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("name: %s, uuid: %s", format.Name, format.UUID))

	// TODO: Implement queue stats functionality
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

	metaEngine, err := meta.NewMeta(metaURI)
	if err != nil {
		return err
	}
	format, err := metaEngine.Load()
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("name: %s, uuid: %s", format.Name, format.UUID))

	return metaEngine.DeleteQueue(ctx, name)
}

func queueListFlags() []cli.Flag {
	return []cli.Flag{}
}

func queueCreateFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "index",
			Usage: "Index fields (comma separated) with '@' prefix, e.g. @model, @priority",
		},
		&cli.StringFlag{
			Name:  "streaming",
			Usage: "Streaming Engine URI",
		},
		&cli.StringFlag{
			Name:  "storage",
			Usage: "Storage Engine URI",
		},
	}
}

func queueStatsFlags() []cli.Flag {
	return []cli.Flag{}
}

func queueDeleteFlags() []cli.Flag {
	return []cli.Flag{}
}
