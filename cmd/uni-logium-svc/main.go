package main

import (
	"fmt"
	"os"

	"github.com/paranoideed/uni-logium-svc/internal/cli"
)

func main() {
	if err := cli.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}
