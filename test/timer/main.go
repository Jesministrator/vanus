// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// vsctl is a command line application that controls vanus.
package main

import (
	"os"

	"github.com/fatih/color"
	"github.com/linkall-labs/vanus/test/timer/performance"
	"github.com/spf13/cobra"
)

const (
	cliName        = "vsctl-timer"
	cliDescription = "the command-line application for timer"
)

var (
	EtcdEndpoints = []string{"vanus-controller-0.vanus-controller:2379", "vanus-controller-1.vanus-controller:2379", "vanus-controller-2.vanus-controller:2379"}
	CtrlEndpoints = []string{"vanus-controller-0.vanus-controller.vanus.svc:2048", "vanus-controller-1.vanus-controller.vanus.svc:2048", "vanus-controller-2.vanus-controller.vanus.svc:2048"}
	rootCmd       = &cobra.Command{
		Use:        cliName,
		Short:      cliDescription,
		SuggestFor: []string{"vsctl-timer"},
	}
)

func main() {
	cobra.EnablePrefixMatching = true
	cobra.EnableCommandSorting = false

	rootCmd.AddCommand(
		// tools.NewMetaCommand(),
		// tools.NewEventCommand(),
		// tools.NewEventbusCommand(),
		// function.NewFunctionTestCommand(),
		performance.NewPerformanceTestCommand(),
	)
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	if err := rootCmd.Execute(); err != nil {
		color.Red("vsctl-timer run error: %s", err)
		os.Exit(-1)
	}
}
