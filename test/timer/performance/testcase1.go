package performance

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/fatih/color"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/test/timer/utils"
	"github.com/spf13/cobra"
)

func testcase1() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "testcase1",
		Short: "exec testcase1 of performance testing",
		Run: func(cmd *cobra.Command, args []string) {
			// 1. environmental preparation
			var err error
			ctx := context.Background()
			eventbus := "performance-1"
			err = utils.CreateEventbus(ctx, eventbus)
			if err != nil {
				log.Error(ctx, "create eventbus failed", map[string]interface{}{
					"eventbus": eventbus,
				})
				utils.CmdFailedf(cmd, "create eventbus failed, eventbus: %s, err: %s", eventbus, err.Error())
			}
			ebClient := utils.NewEventbusClient(ctx, timerBuiltInEventbusReceivingStation)

			// start
			now := time.Now().UTC()
			var success int64
			wg := sync.WaitGroup{}
			wg.Add(16)
			ctx, cancel := context.WithCancel(context.Background())
			for p := 0; p < 16; p++ {
				go func() {
					// run(cmd, endpoint, eb, int(number)/(eventbusNum*parallelism), &success, false)
					failed := 0
					for idx := 0; idx < 1000; idx++ {
						event := ce.NewEvent()
						event.SetSource("performance.benchmark.vanus")
						event.SetType("performance.benchmark.vanus")
						event.SetTime(time.Now())
						event.SetData(ce.ApplicationJSON, genData())
						off, err := ebClient.AppendOne(ctx, &event)
						if err != nil {
							fmt.Printf("pt testcase1 put event failed, off: %s, err: %s", off, err.Error())
						}
						if err == nil {
							atomic.AddInt64(&success, 1)
						} else {
							failed++
						}
					}

					if failed != 0 {
						fmt.Printf("%s sent done, failed number: %d\n", eventbus, failed)
					}
					wg.Done()
				}()
			}

			go func() {
				var prev int64
				tick := time.NewTicker(time.Second)
				defer tick.Stop()
				for prev < 16000 {
					select {
					case <-tick.C:
						cur := atomic.LoadInt64(&success)
						tps := cur - prev
						log.Info(nil, fmt.Sprintf("Sent: %d, TPS: %d\n", cur, tps), nil)
						prev = cur
					case <-ctx.Done():
						return
					}
				}
			}()
			wg.Wait()
			cancel()
			log.Info(ctx, "time spent, unit: millisecounds", map[string]interface{}{
				"time": time.Now().UTC().Sub(now).Milliseconds(),
			})

			data, _ := json.Marshal(map[string]interface{}{"Result": 200})
			color.Green(string(data))
		},
	}
	return cmd
}

var (
	once = sync.Once{}
	ch   = make(chan map[string]interface{}, 512)
)

func genData() map[string]interface{} {
	once.Do(func() {
		for idx := 0; idx < 1; idx++ {
			go func() {
				rd := rand.New(rand.NewSource(time.Now().UnixNano()))
				for {
					m := map[string]interface{}{
						"data": genStr(rd, 1024),
					}
					ch <- m
				}
			}()
		}
	})
	return <-ch
}

func genStr(rd *rand.Rand, size int) string {
	str := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	data := ""
	for idx := 0; idx < size; idx++ {
		data = fmt.Sprintf("%s%c", data, str[rd.Int31n(int32(len(str)))])
	}
	return data
}
