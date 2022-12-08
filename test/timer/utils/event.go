package utils

// import (
// 	"context"
// 	"errors"
// 	"io"

// 	"github.com/linkall-labs/vanus/observability/log"

// 	ce "github.com/cloudevents/sdk-go/v2"
// 	es "github.com/linkall-labs/vanus/client/pkg/errors"
// )

// func (eb *ebClient) GetEvent(ctx context.Context, offset int64, number int16) ([]*ce.Event, error) {
// 	var err error
// 	_, err = eb.eventlogReader.Seek(ctx, offset, io.SeekStart)
// 	if err != nil {
// 		log.Error(ctx, "seek failed", map[string]interface{}{
// 			log.KeyError: err,
// 			"offset":     offset,
// 		})
// 		return nil, err
// 	}

// 	events, err := eb.eventlogReader.Read(ctx, number)
// 	if err != nil {
// 		if !errors.Is(err, es.ErrOnEnd) && !errors.Is(ctx.Err(), context.Canceled) {
// 			log.Error(ctx, "Read failed", map[string]interface{}{
// 				log.KeyError: err,
// 				"offset":     offset,
// 			})
// 		}
// 		return nil, err
// 	}

// 	log.Debug(ctx, "get event success", map[string]interface{}{
// 		"eventbus": eb.eventbus,
// 		"offset":   offset,
// 		"number":   number,
// 	})
// 	return events, nil
// }

// func (eb *ebClient) PutEvent(ctx context.Context, event *ce.Event) (int64, error) {
// 	offset, err := eb.eventlogWriter.Append(ctx, event)
// 	if err != nil {
// 		log.Error(ctx, "append event to failed", map[string]interface{}{
// 			log.KeyError: err,
// 			"eventbus":   eb.eventbus,
// 		})
// 		return offset, err
// 	}
// 	log.Debug(ctx, "put event success", map[string]interface{}{
// 		"eventbus": eb.eventbus,
// 		"offset":   offset,
// 	})
// 	return offset, nil
// }
