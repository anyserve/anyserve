package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/anyserve/anyserve/pkg/meta"
	"github.com/urfave/cli/v3"
)

func queueCreateCommand() *cli.Command {
	return &cli.Command{
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
	}
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
