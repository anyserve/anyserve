package cmd

import (
	"context"
	"fmt"

	"github.com/anyserve/anyserve/pkg/meta"
	"github.com/urfave/cli/v3"
)

func queueDeleteCommand() *cli.Command {
	return &cli.Command{
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
	}
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

func queueDeleteFlags() []cli.Flag {
	return []cli.Flag{}
}
