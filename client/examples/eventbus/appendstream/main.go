// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	// standard libraries.
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	// third-party project.
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"

	// this project.
	"github.com/linkall-labs/vanus/client"
)

var (
	eventNum     int64  = 10000
	eventbusNum  int    = 1
	parallelism  int    = 1
	payloadSize  int    = 1024
	success      int64  = 0
	failed       int64  = 0
	eventbusName        = "performance-1"
	enabledelay  string = "false"

	once = sync.Once{}

	ch = make(chan map[string]interface{}, 512)

	CtrlEndpoints = []string{"vanus-controller-0.vanus-controller.vanus.svc:2048", "vanus-controller-1.vanus-controller.vanus.svc:2048", "vanus-controller-2.vanus-controller.vanus.svc:2048"}
)

func main() {
	ctx := context.Background()

	if len(os.Args) != 3 {
		fmt.Println("param error, p[1]=parallelism, p[2]=printlog")
		return
	}
	parallelism, _ = strconv.Atoi(os.Args[1])
	enabledelay = os.Args[2]

	c := client.Connect(CtrlEndpoints)
	bus := c.Eventbus(ctx, eventbusName)
	w := bus.Writer()

	wg := sync.WaitGroup{}
	wg.Add(eventbusNum * parallelism)
	ctx, cancel := context.WithCancel(context.Background())
	for idx := 1; idx <= eventbusNum; idx++ {
		// eventbusName := fmt.Sprintf("performance-%d", idx)
		for p := 0; p < parallelism; p++ {
			go func() {
				// run(cmd, endpoint, eb, int(number)/(eventbusNum*parallelism), &success, false)
				for idx := int64(0); idx < eventNum; idx++ {
					event := ce.NewEvent()
					event.SetID(uuid.NewString())
					event.SetSource("performance.benchmark.vanus")
					event.SetType("performance.benchmark.vanus")
					event.SetTime(time.Now())
					event.SetData(ce.ApplicationJSON, genData())
					now := time.Now()
					err := w.AppendOneStream(ctx, &event, func(err error) {
						fmt.Printf("sent event failed, event: %+v, err: %+v\n", event, err)
					})
					if err != nil {
						atomic.AddInt64(&failed, 1)
						fmt.Printf("AppendOneStream failed, err: %+v\n", err)
						return
					}
					if enabledelay == "true" {
						fmt.Printf("time spent append unary, time: %+v\n", time.Since(now).Microseconds())
					}
					atomic.AddInt64(&success, 1)
				}
				wg.Done()
			}()
		}
	}

	go func() {
		var prev int64
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for prev < eventNum {
			select {
			case <-tick.C:
				cur := atomic.LoadInt64(&success)
				tps := cur - prev
				fmt.Printf("Sent: %d, TPS: %d\n", cur, tps)
				prev = cur
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()
	cancel()
}

func genData() map[string]interface{} {
	once.Do(func() {
		for idx := 0; idx < eventbusNum; idx++ {
			go func() {
				rd := rand.New(rand.NewSource(time.Now().UnixNano()))
				for {
					m := map[string]interface{}{
						"data": genStr(rd, payloadSize),
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
