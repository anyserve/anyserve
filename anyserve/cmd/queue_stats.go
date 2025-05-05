package cmd

import (
	"context"
	"fmt"

	"github.com/anyserve/anyserve/pkg/meta"
	"github.com/urfave/cli/v3"
)

func queueStatsCommand() *cli.Command {
	return &cli.Command{
		Name:      "stats",
		Usage:     "Get queue stats",
		ArgsUsage: "META-URI NAME",
		Description: `
Get queue stats.

Examples:
$ anyserve queue stats redis://localhost myqueue
`,
		Action: queueStatsFunc,
		Flags:  expandFlags(queueStatsFlags()),
	}
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

func queueStatsFlags() []cli.Flag {
	return []cli.Flag{}
}
