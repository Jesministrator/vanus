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

package store

import (
	// standard libraries
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/observability/tracing"
	"go.opentelemetry.io/otel/trace"

	// third-party libraries
	cepb "cloudevents.io/genproto/v1"
	ce "github.com/cloudevents/sdk-go/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	// first-party libraries
	errpb "github.com/linkall-labs/vanus/proto/pkg/errors"
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"

	// this project
	"github.com/linkall-labs/vanus/client/internal/vanus/codec"
	"github.com/linkall-labs/vanus/client/internal/vanus/net/rpc"
	"github.com/linkall-labs/vanus/client/internal/vanus/net/rpc/bare"
	"github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/client/pkg/primitive"
	"github.com/linkall-labs/vanus/observability/log"
)

func newBlockStore(endpoint string) (*BlockStore, error) {
	var err error
	s := &BlockStore{
		RefCount: primitive.RefCount{},
		client: bare.New(endpoint, rpc.NewClientFunc(func(conn *grpc.ClientConn) interface{} {
			return segpb.NewSegmentServerClient(conn)
		})),
		tracer: tracing.NewTracer("internal.store.BlockStore", trace.SpanKindClient),
	}
	s.stream, err = s.connect(context.Background())
	if err != nil {
		// TODO: check error
		return nil, err
	}
	go s.receive(context.Background())
	return s, nil
}

type BlockStore struct {
	primitive.RefCount
	client rpc.Client
	tracer *tracing.Tracer
	stream segpb.SegmentServer_HandlerClient
	mu     sync.Mutex
	m      sync.Map
}

type callback func(*segpb.Response)

func (s *BlockStore) receive(ctx context.Context) {
	for {
		res, err := s.stream.Recv()
		if err != nil {
			log.Error(ctx, "===client=== stream receive failed", map[string]interface{}{
				log.KeyError: err,
			})
			continue
		}
		c, _ := s.m.LoadAndDelete(res.ResponseId)
		if c != nil {
			c.(callback)(res)
		}
	}
}

func (s *BlockStore) Endpoint() string {
	return s.client.Endpoint()
}

func (s *BlockStore) Close() {
	s.client.Close()
}

func (s *BlockStore) connect(ctx context.Context) (segpb.SegmentServer_HandlerClient, error) {
	if s.stream != nil {
		return s.stream, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stream != nil { //double check
		return s.stream, nil
	}

	client, err := s.client.Get(ctx)
	if err != nil {
		return nil, err
	}

	stream, err := client.(segpb.SegmentServerClient).Handler(context.Background())
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (s *BlockStore) Append(ctx context.Context, block uint64, event *ce.Event) (int64, error) {
	_ctx, span := s.tracer.Start(ctx, "Append")
	defer span.End()

	var (
		err  error
		wg   sync.WaitGroup
		resp *segpb.Response
	)

	now := time.Now()

	if s.stream == nil {
		s.stream, err = s.connect(_ctx)
		if err != nil {
			return -1, err
		}
	}

	// generate unique RequestId
	requestID := rand.Uint64()

	wg.Add(1)
	s.m.Store(requestID, callback(func(res *segpb.Response) {
		resp = res
		wg.Done()
	}))

	go func() {
		s.send(ctx, block, event, requestID)
	}()

	wg.Wait()

	if resp.Error.Internal != nil {
		log.Warning(ctx, "block append failed cause internal error", map[string]interface{}{
			"reason": resp.Error.SegmentFull.Reason,
		})
		return -1, errors.ErrUnknown
	}

	if resp.Error.SegmentFull != nil {
		log.Warning(ctx, "block append failed cause the segment is full", map[string]interface{}{
			"reason": resp.Error.SegmentFull.Reason,
		})
		return -1, errors.ErrNoSpace
	}

	log.Warning(ctx, "===client=== block append delay time", map[string]interface{}{
		"spent": time.Since(now).Milliseconds(),
	})

	return resp.Append.Offsets[0], nil

	// eventpb, err := codec.ToProto(event)
	// if err != nil {
	// 	return -1, err
	// }
	// req := &segpb.Request{
	// 	RequestId:   requestID,
	// 	RequestCode: segpb.RequestCode_RequestCodeAppendToBlock,
	// 	Append: &segpb.AppendToBlockRequest{
	// 		BlockId: block,
	// 		Events: &cepb.CloudEventBatch{
	// 			Events: []*cepb.CloudEvent{eventpb},
	// 		},
	// 	},
	// }
	// if err = s.stream.Send(req); err != nil {
	// 	log.Warning(ctx, "===client=== stream send failed", map[string]interface{}{
	// 		log.KeyError: err,
	// 	})
	// 	s.stream.CloseSend()
	// 	s.stream = nil
	// 	s.stream, err = s.connect(ctx)
	// 	if err != nil {
	// 		return -1, err
	// 	}
	// 	if err = s.stream.Send(req); err != nil {
	// 		log.Warning(ctx, "===client=== stream double send failed", map[string]interface{}{
	// 			log.KeyError: err,
	// 		})
	// 		return -1, err
	// 	}
	// }

	// var resp *segpb.Response
	// wg.Add(1)
	// s.m.Store(req.RequestId, callback(func(res *segpb.Response) {
	// 	resp = res
	// 	wg.Done()
	// }))
	// wg.Wait()

	// if resp.Error.SegmentFull != nil {
	// 	log.Warning(ctx, "block append failed cause the segment is full", map[string]interface{}{
	// 		"reason": resp.Error.SegmentFull.Reason,
	// 	})
	// 	return -1, errors.ErrNoSpace
	// }
	// return resp.Append.Offsets[0], nil
}

func (s *BlockStore) send(ctx context.Context, block uint64, event *ce.Event, requestID uint64) {
	cleanFunc := func(err error) {
		s.m.Delete(requestID)
		c, _ := s.m.LoadAndDelete(requestID)
		if c != nil {
			c.(callback)(&segpb.Response{
				Error: &errpb.StreamError{
					Internal: &errpb.Internal{
						Reason: err.Error(),
					},
				},
			})
		}
	}
	eventpb, err := codec.ToProto(event)
	if err != nil {
		cleanFunc(err)
	}
	req := &segpb.Request{
		RequestId:   requestID,
		RequestCode: segpb.RequestCode_RequestCodeAppendToBlock,
		Append: &segpb.AppendToBlockRequest{
			BlockId: block,
			Events: &cepb.CloudEventBatch{
				Events: []*cepb.CloudEvent{eventpb},
			},
		},
	}
	if err = s.stream.Send(req); err != nil {
		log.Warning(ctx, "===client=== stream send failed", map[string]interface{}{
			log.KeyError: err,
		})
		s.stream.CloseSend()
		s.stream = nil
		s.stream, err = s.connect(ctx)
		if err != nil {
			cleanFunc(err)
		}
		if err = s.stream.Send(req); err != nil {
			log.Warning(ctx, "===client=== stream double send failed", map[string]interface{}{
				log.KeyError: err,
			})
			cleanFunc(err)
		}
	}
}

func (s *BlockStore) Read(
	ctx context.Context, block uint64, offset int64, size int16, pollingTimeout uint32,
) ([]*ce.Event, error) {
	ctx, span := s.tracer.Start(ctx, "Append")
	defer span.End()

	req := &segpb.ReadFromBlockRequest{
		BlockId:        block,
		Offset:         offset,
		Number:         int64(size),
		PollingTimeout: pollingTimeout,
	}

	client, err := s.client.Get(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := client.(segpb.SegmentServerClient).ReadFromBlock(ctx, req)
	if err != nil {
		// TODO: convert error
		if errStatus, ok := status.FromError(err); ok {
			errMsg := errStatus.Message()
			if strings.Contains(errMsg, "the offset on end") {
				err = errors.ErrOnEnd
			} else if strings.Contains(errMsg, "the offset exceeded") {
				err = errors.ErrOverflow
			} else if errStatus.Code() == codes.DeadlineExceeded {
				err = errors.ErrTimeout
			}
		}
		return nil, err
	}

	if batch := resp.GetEvents(); batch != nil {
		if eventpbs := batch.GetEvents(); len(eventpbs) > 0 {
			events := make([]*ce.Event, 0, len(eventpbs))
			for _, eventpb := range eventpbs {
				event, err2 := codec.FromProto(eventpb)
				if err2 != nil {
					// TODO: return events or error?
					return events, err2
				}
				events = append(events, event)
			}
			return events, nil
		}
	}

	return []*ce.Event{}, err
}

func (s *BlockStore) LookupOffset(ctx context.Context, blockID uint64, t time.Time) (int64, error) {
	ctx, span := s.tracer.Start(ctx, "LookupOffset")
	defer span.End()

	req := &segpb.LookupOffsetInBlockRequest{
		BlockId: blockID,
		Stime:   t.UnixMilli(),
	}

	client, err := s.client.Get(ctx)
	if err != nil {
		return -1, err
	}

	res, err := client.(segpb.SegmentServerClient).LookupOffsetInBlock(ctx, req)
	if err != nil {
		return -1, err
	}
	return res.Offset, nil
}
