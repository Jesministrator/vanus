package utils

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

type Bar struct {
	mu      sync.Mutex
	graph   string
	rate    string
	percent int
	current int
	total   int
	start   time.Time
}

func NewBar(current, total int) *Bar {
	bar := new(Bar)
	bar.current = current
	bar.total = total
	bar.start = time.Now()
	if bar.graph == "" {
		bar.graph = "█"
	}
	bar.percent = bar.getPercent()
	for i := 0; i < bar.percent; i += 2 {
		bar.rate += bar.graph
	}
	return bar
}

func NewBarWithGraph(start, total int, graph string) *Bar {
	bar := NewBar(start, total)
	bar.graph = graph
	return bar
}

func (bar *Bar) getPercent() int {
	return int((float64(bar.current) / float64(bar.total)) * 100)
}

func (bar *Bar) getTime() (s string) {
	u := time.Since(bar.start).Seconds()
	h := int(u) / 3600
	m := int(u) % 3600 / 60
	if h > 0 {
		s += strconv.Itoa(h) + "h "
	}
	if h > 0 || m > 0 {
		s += strconv.Itoa(m) + "m "
	}
	s += strconv.Itoa(int(u)%60) + "s"
	return
}

func (bar *Bar) load() {
	last := bar.percent
	bar.percent = bar.getPercent()
	if bar.percent != last && bar.percent%2 == 0 {
		bar.rate += bar.graph
	}
	fmt.Printf("\r[%-50s]% 3d%%    %2s   %d/%d", bar.rate, bar.percent, bar.getTime(), bar.current, bar.total)
}

func (bar *Bar) Reset(current int) {
	bar.mu.Lock()
	defer bar.mu.Unlock()
	bar.current = current
	bar.load()

}

func (bar *Bar) Add(i int) {
	bar.mu.Lock()
	defer bar.mu.Unlock()
	bar.current += i
	bar.load()
}

func WaitUntilTestcaseIsCompleted(t time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	waitTime := t
	b := NewBar(0, 100)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(waitTime / 100)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.Add(1)
			}
		}
	}()
	waitCtx, waitCancel := context.WithCancel(ctx)
	wait.Until(func() {
		if time.Since(b.start) > waitTime {
			cancel()
			waitCancel()
		}
	}, 100*time.Millisecond, waitCtx.Done())
	wg.Wait()
}
