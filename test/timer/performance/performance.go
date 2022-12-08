package performance

import (
	"github.com/spf13/cobra"
)

const (
	xceVanusEventbus                     = "xvanuseventbus"
	xceVanusDeliveryTime                 = "xvanusdeliverytime"
	timerBuiltInEventbusReceivingStation = "__Timer_RS"
)

func NewPerformanceTestCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pt sub-command ",
		Short: "convenient operations for performance testing",
	}
	cmd.AddCommand(testcase1())
	// cmd.AddCommand(testcase2())
	cmd.AddCommand(testcase3())
	return cmd
}
