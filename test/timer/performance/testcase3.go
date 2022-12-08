package performance

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/test/timer/utils"
	"github.com/spf13/cobra"
)

func testcase3() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "testcase3",
		Short: "exec testcase1 of performance testing",
		Run: func(cmd *cobra.Command, args []string) {
			// 1. environmental preparation
			ctx := context.Background()

			// 2. test data injection
			// record the time spent writing messages
			now := time.Now().UTC()
			numberOfEvents := 32
			var wg sync.WaitGroup
			for i := 0; i < numberOfEvents; i++ {
				wg.Add(1)
				go func(slot int64) {
					defer wg.Done()
					ebName := fmt.Sprintf("__Timer_1_%d", slot)
					busWriter := utils.NewEventbusClient(ctx, ebName)
					if busWriter == nil {
						utils.CmdFailedf(cmd, "pt testcase3 new eventbus client failed")
					}
				}(int64(i))
			}
			wg.Wait()
			log.Info(ctx, "time spent, unit: millisecounds", map[string]interface{}{
				"time": time.Now().UTC().Sub(now).Milliseconds(),
			})

			// 3. wait for program processing to complete
			log.Info(ctx, "waitting 3s until all event expired", nil)
			utils.WaitUntilTestcaseIsCompleted(3 * time.Second)

			// 4. result verification
			// 4.1 check the number of events received

			// 4.2 check timing message delivery time delay
			// todo, after long polling option merge

			// 5. environmental cleaning

			data, _ := json.Marshal(map[string]interface{}{"Result": 200})
			color.Green(string(data))
		},
	}
	return cmd
}
