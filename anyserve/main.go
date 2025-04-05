package main

import (
	"os"

	"github.com/anyserve/anyserve/cmd"
	"github.com/anyserve/anyserve/pkg/utils"
	"go.uber.org/zap"
)

var logger = utils.GetLogger("anyserve")

func main() {
	err := cmd.Main(os.Args)
	if err != nil {
		logger.Fatal("failed to run command", zap.Error(err))
	}
}
