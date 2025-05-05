package cmd

import (
	"context"
	"fmt"

	"github.com/anyserve/anyserve/pkg/meta"
	"github.com/pterm/pterm"
	"github.com/urfave/cli/v3"
)

func queueListCommand() *cli.Command {
	return &cli.Command{
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
		Aliases: []string{"ps", "ls"},
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

	table := pterm.TableData{
		{"#", "Name", "Index", "Streaming", "Storage"},
	}
	for i, queue := range queues {
		table = append(table, []string{fmt.Sprintf("%d", i), queue.Name, queue.Index, queue.Streaming, queue.Storage})
	}
	pterm.DefaultTable.WithHasHeader().WithData(table).Render()
	return nil
}

func queueListFlags() []cli.Flag {
	return []cli.Flag{}
}
