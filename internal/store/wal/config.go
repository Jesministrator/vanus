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

package wal

import (
	// standard libraries.
	"time"

	// this project.
	ioengine "github.com/linkall-labs/vanus/internal/store/io/engine"
	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

const (
	defaultBlockSize        = 4 * 1024
	defaultFileSize         = 128 * 1024 * 1024
	defaultFlushTimeout     = 3 * time.Millisecond
	defaultAppendBufferSize = 64
	defaultFlushBufferSize  = 64
	defaultWakeupBufferSize = defaultFlushBufferSize * 2
)

type config struct {
	pos                int64
	cb                 OnEntryCallback
	blockSize          int
	fileSize           int64
	flushTimeout       time.Duration
	appendBufferSize   int
	callbackBufferSize int
	flushBufferSize    int
	wakeupBufferSize   int
	engine             ioengine.Interface
}

func defaultConfig() config {
	cfg := config{
		blockSize:          defaultBlockSize,
		fileSize:           defaultFileSize,
		flushTimeout:       defaultFlushTimeout,
		appendBufferSize:   defaultAppendBufferSize,
		callbackBufferSize: (defaultBlockSize + record.HeaderSize - 1) / record.HeaderSize,
		flushBufferSize:    defaultFlushBufferSize,
		wakeupBufferSize:   defaultWakeupBufferSize,
	}
	return cfg
}

type Option func(*config)

func makeConfig(opts ...Option) config {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.engine == nil {
		cfg.engine = defaultIOEngine()
	}
	return cfg
}

func FromPosition(pos int64) Option {
	return func(cfg *config) {
		cfg.pos = pos
	}
}

func WithRecoveryCallback(cb OnEntryCallback) Option {
	return func(cfg *config) {
		cfg.cb = cb
	}
}

func WithBlockSize(blockSize int) Option {
	return func(cfg *config) {
		cfg.blockSize = blockSize
		cfg.callbackBufferSize = (blockSize + record.HeaderSize - 1) / record.HeaderSize
	}
}

func WithFileSize(fileSize int64) Option {
	return func(cfg *config) {
		cfg.fileSize = fileSize
	}
}

func WithFlushTimeout(d time.Duration) Option {
	return func(cfg *config) {
		cfg.flushTimeout = d
	}
}

func WithAppendBufferSize(size int) Option {
	return func(cfg *config) {
		cfg.appendBufferSize = size
	}
}

func WithCallbackBufferSize(size int) Option {
	return func(cfg *config) {
		cfg.callbackBufferSize = size
	}
}

func WithFlushBufferSize(size int) Option {
	return func(cfg *config) {
		cfg.flushBufferSize = size
	}
}

func WithWakeupBufferSize(size int) Option {
	return func(cfg *config) {
		cfg.wakeupBufferSize = size
	}
}

func WithIOEngine(engine ioengine.Interface) Option {
	return func(cfg *config) {
		cfg.engine = engine
	}
}
