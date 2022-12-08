package utils

import (
	"context"
	"time"

	client "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/api"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/pkg/controller"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	meta "github.com/linkall-labs/vanus/proto/pkg/meta"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var (
	EtcdEndpoints = []string{"vanus-controller-0.vanus-controller:2379", "vanus-controller-1.vanus-controller:2379", "vanus-controller-2.vanus-controller:2379"}
	CtrlEndpoints = []string{"vanus-controller-0.vanus-controller.vanus.svc:2048", "vanus-controller-1.vanus-controller.vanus.svc:2048", "vanus-controller-2.vanus-controller.vanus.svc:2048"}
)

func ListEventbus(ctx context.Context) (*ctrlpb.ListEventbusResponse, error) {
	eventbusCtrlCli := controller.NewEventbusClient(CtrlEndpoints, insecure.NewCredentials())
	return eventbusCtrlCli.ListEventBus(ctx, &emptypb.Empty{})
}

func CreateEventbus(ctx context.Context, eventbus string) error {
	// eventbusCtrlCli := NewClient(CtrlEndpoints).makeSureClient(ctx, true)
	eventbusCtrlCli := controller.NewEventbusClient(CtrlEndpoints, insecure.NewCredentials())
	_, err := eventbusCtrlCli.CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
		Name: eventbus,
	})
	if err != nil {
		log.Error(ctx, "create eventbus failed", map[string]interface{}{
			log.KeyError: err,
			"eventbus":   eventbus,
		})
		return err
	}
	log.Info(ctx, "create eventbus success.", map[string]interface{}{
		"eventbus": eventbus,
	})
	return nil
}

func DeleteEventbus(ctx context.Context, eventbus string) error {
	eventbusCtrlCli := controller.NewEventbusClient(CtrlEndpoints, insecure.NewCredentials())
	_, err := eventbusCtrlCli.DeleteEventBus(ctx, &meta.EventBus{
		Name: eventbus,
	})
	if err != nil {
		log.Error(ctx, "delete eventbus failed", map[string]interface{}{
			log.KeyError: err,
			"eventbus":   eventbus,
		})
		return err
	}
	log.Info(ctx, "delete eventbus success.", map[string]interface{}{
		"eventbus": eventbus,
	})
	return nil
}

// type ebClient struct {
// 	eventbus       string
// 	eventlogWriter eventlog.LogWriter
// 	eventlogReader eventlog.LogReader
// }

// func NewEventlogClient(ctx context.Context, eventbus string) *ebClient {
// 	vrn := fmt.Sprintf("vanus:///eventbus/%s?controllers=%s", eventbus, strings.Join(CtrlEndpoints, ","))
// 	ls, _ := client.LookupReadableLogs(ctx, vrn)
// 	eventlogReader, _ := client.OpenLogReader(ctx, ls[0].VRN)
// 	eventlogWriter, _ := client.OpenLogWriter(ctx, ls[0].VRN)
// 	return &ebClient{
// 		eventbus:       eventbus,
// 		eventlogReader: eventlogReader,
// 		eventlogWriter: eventlogWriter,
// 	}
// }

func NewEventbusClient(ctx context.Context, eventbus string) api.BusWriter {
	now := time.Now()
	eventbusWriter := client.Connect(CtrlEndpoints).Eventbus(ctx, eventbus).Writer()
	log.Info(ctx, "timespent of NewEventbusClient.", map[string]interface{}{
		"time": time.Since(now).Milliseconds(),
	})
	return eventbusWriter
}
