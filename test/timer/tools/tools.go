package tools

// import (
// 	"context"
// 	"encoding/json"
// 	"os"
// 	"reflect"
// 	"strconv"
// 	"sync"
// 	"time"

// 	ce "github.com/cloudevents/sdk-go/v2"
// 	"github.com/fatih/color"
// 	"github.com/jedib0t/go-pretty/v6/table"
// 	"github.com/jedib0t/go-pretty/v6/text"
// 	"github.com/linkall-labs/vanus/internal/kv/etcd"
// 	"github.com/linkall-labs/vanus/observability/log"
// 	"github.com/linkall-labs/vanus/test/timer/utils"
// 	"github.com/spf13/cobra"
// )

// const (
// 	xceVanusEventbus                     = "xvanuseventbus"
// 	xceVanusDeliveryTime                 = "xvanusdeliverytime"
// 	timerBuiltInEventbusReceivingStation = "__Timer_RS"
// )

// var (
// 	EtcdEndpoints = []string{"vanus-controller-0.vanus-controller:2379", "vanus-controller-1.vanus-controller:2379", "vanus-controller-2.vanus-controller:2379"}
// 	CtrlEndpoints = []string{"vanus-controller-0.vanus-controller.vanus.svc:2048", "vanus-controller-1.vanus-controller.vanus.svc:2048", "vanus-controller-2.vanus-controller.vanus.svc:2048"}
// )

// var (
// 	delay             int
// 	offset            int64
// 	number            int16
// 	batch             int64
// 	eventDeliveryTime string
// 	eventDelayTime    string
// )

// func NewMetaCommand() *cobra.Command {
// 	cmd := &cobra.Command{
// 		Use:   "meta sub-command ",
// 		Short: "convenient operations for list",
// 	}
// 	cmd.AddCommand(listMetaCommand())
// 	return cmd
// }

// func listMetaCommand() *cobra.Command {
// 	cmd := &cobra.Command{
// 		Use:     "list <path> ",
// 		Short:   "list metadata from etcd",
// 		Example: "timer meta list time/pointer/offset",
// 		Run: func(cmd *cobra.Command, args []string) {
// 			if len(args) == 0 {
// 				utils.CmdFailedWithHelpNotice(cmd, "metadata name can't be empty\n")
// 			}
// 			ctx := context.Background()
// 			store, err := etcd.NewEtcdClientV3(EtcdEndpoints, "/vanus")
// 			if err != nil {
// 				utils.CmdFailedf(cmd, "new etcd client failed, err: %s", err.Error())
// 			}
// 			path := "/vanus/internal/resource/timer/metadata/" + args[0]
// 			pairs, err := store.List(ctx, path)
// 			if err != nil {
// 				utils.CmdFailedf(cmd, "list failed, err: %s", err.Error())
// 			}
// 			for idx, pair := range pairs {
// 				data, _ := json.Marshal(map[string]interface{}{
// 					"No.":   idx,
// 					"Event": string(pair.Value),
// 				})
// 				color.Yellow(string(data))
// 			}
// 		},
// 	}
// 	return cmd
// }

// func NewEventCommand() *cobra.Command {
// 	cmd := &cobra.Command{
// 		Use:   "event sub-command ",
// 		Short: "convenient operations for get/put",
// 	}
// 	cmd.AddCommand(getEventCommand())
// 	cmd.AddCommand(putEventCommand())
// 	cmd.AddCommand(putScheduledEventCommand())
// 	cmd.AddCommand(putBatchEventCommand())
// 	return cmd
// }

// const (
// 	xVanusEventbus     = "xvanuseventbus"
// 	xVanusDeliveryTime = "xvanusdeliverytime"
// )

// func getEventCommand() *cobra.Command {
// 	cmd := &cobra.Command{
// 		Use:     "get <eventbus-name> ",
// 		Short:   "get a event from specified eventbus",
// 		Example: "timer event get eventbus-name --offset 0 --number 1",
// 		Run: func(cmd *cobra.Command, args []string) {
// 			if len(args) == 0 {
// 				utils.CmdFailedWithHelpNotice(cmd, "eventbus name can't be empty\n")
// 			}
// 			eventbus := args[0]
// 			ctx := context.Background()
// 			elClient := utils.NewEventlogClient(ctx, eventbus)
// 			now := time.Now().UTC()
// 			events, err := elClient.GetEvent(ctx, offset, number)
// 			log.Info(ctx, "time spent, unit: millisecounds", map[string]interface{}{
// 				"time": time.Now().UTC().Sub(now).Milliseconds(),
// 			})
// 			if err != nil {
// 				utils.CmdFailedf(cmd, "get event failed, number: %s, err: %s", number, err.Error())
// 			}

// 			for idx, event := range events {
// 				extensions := event.Extensions()
// 				if deliveryTime, ok := extensions[xVanusDeliveryTime]; ok {
// 					log.Warning(ctx, "xvanusdeliverytime", map[string]interface{}{
// 						"deliveryTime": deliveryTime,
// 						"type":         reflect.TypeOf(deliveryTime),
// 					})
// 					switch value := deliveryTime.(type) {
// 					case string:
// 						log.Warning(ctx, "xvanusdeliverytime type is string", map[string]interface{}{
// 							"value": value,
// 						})
// 					case ce.Timestamp:
// 						log.Warning(ctx, "xvanusdeliverytime type is timestamp", map[string]interface{}{
// 							"value": value,
// 						})
// 					default:
// 						log.Error(ctx, "xvanusdeliverytime type unknown, set to current time", nil)
// 					}
// 				}
// 				data, _ := json.Marshal(map[string]interface{}{
// 					"No.":   idx,
// 					"Event": event.String(),
// 				})
// 				color.Yellow(string(data))
// 			}
// 		},
// 	}
// 	// TODO cmd.Flags().String("eventlog", "", "specified eventlog id get from")
// 	cmd.Flags().Int64Var(&offset, "offset", 0, "which position you want to start get")
// 	cmd.Flags().Int16Var(&number, "number", 1, "the number of event you want to get")
// 	return cmd
// }

// func putEventCommand() *cobra.Command {
// 	cmd := &cobra.Command{
// 		Use:     "put <eventbus-name> ",
// 		Short:   "send a event to eventbus",
// 		Example: "vsctl-timer event put eventbus-name",
// 		Run: func(cmd *cobra.Command, args []string) {
// 			if len(args) == 0 {
// 				utils.CmdFailedWithHelpNotice(cmd, "eventbus name can't be empty\n")
// 			}
// 			eventbus := args[0]
// 			ctx := context.Background()
// 			event := ce.NewEvent()
// 			event.SetExtension(xceVanusEventbus, eventbus)
// 			timestamp := ce.Timestamp{
// 				Time: time.Now(),
// 			}
// 			log.Info(ctx, "send timestamp", map[string]interface{}{
// 				"type":      reflect.TypeOf(timestamp),
// 				"timestamp": timestamp,
// 			})
// 			event.SetExtension(xVanusDeliveryTime, timestamp)
// 			ebClient := utils.NewEventbusClient(ctx, eventbus)
// 			defer ebClient.Close(ctx)
// 			now := time.Now().UTC()
// 			off, err := ebClient.Append(ctx, &event)
// 			log.Info(ctx, "time spent, unit: millisecounds", map[string]interface{}{
// 				"time": time.Now().UTC().Sub(now).Milliseconds(),
// 			})
// 			if err != nil {
// 				utils.CmdFailedf(cmd, "put event failed, off: %s, err: %s", off, err.Error())
// 			}
// 			data, _ := json.Marshal(map[string]interface{}{"Result": 200})
// 			color.Green(string(data))
// 		},
// 	}
// 	return cmd
// }

// func putScheduledEventCommand() *cobra.Command {
// 	cmd := &cobra.Command{
// 		Use:     "put-timer <eventbus-name> ",
// 		Short:   "send a scheduled event to eventbus",
// 		Example: "vsctl-timer event put-timer eventbus-name --delay-time 60",
// 		Run: func(cmd *cobra.Command, args []string) {
// 			if len(args) == 0 {
// 				utils.CmdFailedWithHelpNotice(cmd, "eventbus name can't be empty\n")
// 			}
// 			eventbus := args[0]
// 			ctx := context.Background()
// 			delay, _ = strconv.Atoi(eventDelayTime)
// 			event := ce.NewEvent()
// 			event.SetExtension(xceVanusEventbus, eventbus)
// 			deliveryTime := time.Now().Add(time.Duration(delay) * time.Second).UTC().Format(time.RFC3339)
// 			event.SetExtension(xceVanusDeliveryTime, deliveryTime)
// 			ebClient := utils.NewEventbusClient(ctx, timerBuiltInEventbusReceivingStation)
// 			defer ebClient.Close(ctx)
// 			now := time.Now().UTC()
// 			off, err := ebClient.Append(ctx, &event)
// 			log.Info(ctx, "time spent, unit: millisecounds", map[string]interface{}{
// 				"time": time.Now().UTC().Sub(now).Milliseconds(),
// 			})
// 			if err != nil {
// 				utils.CmdFailedf(cmd, "put scheduled event failed, off: %s, err: %s", off, err.Error())
// 			}
// 			data, _ := json.Marshal(map[string]interface{}{"Result": 200})
// 			color.Green(string(data))
// 		},
// 	}
// 	cmd.Flags().StringVar(&eventDeliveryTime, "delivery-time", "", "event delivery time of CloudEvent, only support the time layout of RFC3339, for example: 2022-01-01T08:00:00Z")
// 	cmd.Flags().StringVar(&eventDelayTime, "delay-time", "", "event delay delivery time of CloudEvent, only support the unit of seconds, for example: 60")
// 	return cmd
// }

// func putBatchEventCommand() *cobra.Command {
// 	cmd := &cobra.Command{
// 		Use:     "put-batch <eventbus-name> ",
// 		Short:   "send batch event to eventbus",
// 		Example: "vsctl-timer event put eventbus-name --delay-time 60 --batch 1",
// 		Run: func(cmd *cobra.Command, args []string) {
// 			if len(args) == 0 {
// 				utils.CmdFailedWithHelpNotice(cmd, "eventbus name can't be empty\n")
// 			}
// 			eventbus := args[0]
// 			ctx := context.Background()
// 			ebClient := utils.NewEventbusClient(ctx, eventbus)
// 			defer ebClient.Close(ctx)
// 			now := time.Now().UTC()
// 			delay, _ = strconv.Atoi(eventDelayTime)
// 			deliveryTime := time.Now().Add(time.Duration(delay) * time.Second).UTC().Format(time.RFC3339)
// 			var wg sync.WaitGroup
// 			glimitC := make(chan struct{}, 1000)
// 			for i := int64(0); i < batch; i++ {
// 				wg.Add(1)
// 				glimitC <- struct{}{}
// 				go func() {
// 					defer wg.Done()
// 					event := ce.NewEvent()
// 					event.SetExtension(xceVanusEventbus, eventbus)
// 					event.SetExtension(xceVanusDeliveryTime, deliveryTime)
// 					nowPut := time.Now().UTC()
// 					_, err := ebClient.Append(ctx, &event)
// 					log.Info(ctx, "time spent of put event, unit: millisecounds", map[string]interface{}{
// 						"time": time.Now().UTC().Sub(nowPut).Milliseconds(),
// 					})
// 					if err != nil {
// 						log.Info(ctx, "put event failed", map[string]interface{}{
// 							log.KeyError: err,
// 						})
// 					}
// 					<-glimitC
// 				}()
// 			}
// 			wg.Wait()
// 			log.Info(ctx, "time spent, unit: millisecounds", map[string]interface{}{
// 				"time": time.Now().UTC().Sub(now).Milliseconds(),
// 			})

// 			data, _ := json.Marshal(map[string]interface{}{"Result": 200})
// 			color.Green(string(data))
// 		},
// 	}
// 	cmd.Flags().StringVar(&eventDeliveryTime, "delivery-time", "", "event delivery time of CloudEvent, only support the time layout of RFC3339, for example: 2022-01-01T08:00:00Z")
// 	cmd.Flags().StringVar(&eventDelayTime, "delay-time", "", "event delay delivery time of CloudEvent, only support the unit of seconds, for example: 60")
// 	cmd.Flags().Int64Var(&batch, "batch", 1, "the number of event you want to put")
// 	return cmd
// }

// func NewEventbusCommand() *cobra.Command {
// 	cmd := &cobra.Command{
// 		Use:   "eventbus sub-command ",
// 		Short: "convenient operations for create/delete",
// 	}
// 	cmd.AddCommand(listEventbusCommand())
// 	cmd.AddCommand(createEventbusCommand())
// 	cmd.AddCommand(deleteEventbusCommand())
// 	return cmd
// }

// func listEventbusCommand() *cobra.Command {
// 	cmd := &cobra.Command{
// 		Use:     "list",
// 		Short:   "list eventbus",
// 		Example: "vsctl-timer eventbus list eventbus-name",
// 		Run: func(cmd *cobra.Command, args []string) {
// 			ctx := context.Background()
// 			rsp, err := utils.ListEventbus(ctx)
// 			if err != nil {
// 				utils.CmdFailedWithHelpNotice(cmd, "create eventbus failed\n")
// 			}

// 			t := table.NewWriter()
// 			t.AppendHeader(table.Row{"Name", "ID", "Eventlog", "Segments"})
// 			for idx := range rsp.Eventbus {
// 				eb := rsp.Eventbus[idx]
// 				if len(eb.Logs) == 0 {
// 					t.AppendRow(table.Row{eb.Name, eb.Id})
// 				} else {
// 					t.AppendRow(table.Row{eb.Name, eb.Id, eb.Logs[0].EventLogId, eb.Logs[0].CurrentSegmentNumbers})
// 					for sIdx := 1; sIdx < len(eb.Logs); sIdx++ {
// 						t.AppendSeparator()
// 						t.AppendRow(table.Row{eb.Name, eb.Id, eb.Logs[idx].EventLogId, eb.Logs[idx].CurrentSegmentNumbers})
// 					}
// 				}
// 				t.AppendSeparator()
// 			}
// 			t.SetColumnConfigs([]table.ColumnConfig{
// 				{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
// 				{Number: 2, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
// 				{Number: 3, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
// 				{Number: 4, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
// 			})
// 			t.SetOutputMirror(os.Stdout)
// 			t.Render()
// 		},
// 	}
// 	return cmd
// }

// func createEventbusCommand() *cobra.Command {
// 	cmd := &cobra.Command{
// 		Use:     "create <eventbus-name> ",
// 		Short:   "create eventbus",
// 		Example: "vsctl-timer eventbus create eventbus-name",
// 		Run: func(cmd *cobra.Command, args []string) {
// 			if len(args) == 0 {
// 				utils.CmdFailedWithHelpNotice(cmd, "eventbus name can't be empty\n")
// 			}
// 			eventbus := args[0]
// 			ctx := context.Background()
// 			err := utils.CreateEventbus(ctx, eventbus)
// 			if err != nil {
// 				utils.CmdFailedWithHelpNotice(cmd, "create eventbus failed\n")
// 			}

// 			data, _ := json.Marshal(map[string]interface{}{"Result": 200})
// 			color.Green(string(data))
// 		},
// 	}
// 	return cmd
// }

// func deleteEventbusCommand() *cobra.Command {
// 	cmd := &cobra.Command{
// 		Use:     "delete <eventbus-name> ",
// 		Short:   "delete eventbus",
// 		Example: "vsctl-timer eventbus delete eventbus-name",
// 		Run: func(cmd *cobra.Command, args []string) {
// 			if len(args) == 0 {
// 				utils.CmdFailedWithHelpNotice(cmd, "eventbus name can't be empty\n")
// 			}
// 			eventbus := args[0]
// 			ctx := context.Background()
// 			err := utils.DeleteEventbus(ctx, eventbus)
// 			if err != nil {
// 				utils.CmdFailedWithHelpNotice(cmd, "delete eventbus failed\n")
// 			}

// 			data, _ := json.Marshal(map[string]interface{}{"Result": 200})
// 			color.Green(string(data))
// 		},
// 	}
// 	return cmd
// }
