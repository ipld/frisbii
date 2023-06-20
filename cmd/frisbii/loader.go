package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
)

type Loader struct {
	writer       io.Writer
	ticker       *time.Ticker
	isRunning    bool
	tickDuration time.Duration
	wg           sync.WaitGroup
	lk           sync.Mutex
	status       string
}

func NewLoader(w io.Writer, tickDuration ...time.Duration) *Loader {
	duration := 50 * time.Millisecond
	if len(tickDuration) > 0 && tickDuration[0] > 0 {
		duration = tickDuration[0]
	}
	return &Loader{
		writer:       w,
		tickDuration: duration,
	}
}

var winTerm = runtime.GOOS == "windows" && len(os.Getenv("WT_SESSION")) > 0

func (l *Loader) Start() {
	if !winTerm {
		fmt.Fprint(l.writer, "\033[?25l") // hide cursor
	}
	l.isRunning = true
	l.ticker = time.NewTicker(l.tickDuration)
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		spinnerChars := []string{
			// "▒▒▒▒▒▒▒▒▒▒", "█▒▒▒▒▒▒▒▒▒", "██▒▒▒▒▒▒▒▒", "███▒▒▒▒▒▒▒", "████▒▒▒▒▒▒", "████▒▒▒▒▒▒", "█████▒▒▒▒▒", "██████▒▒▒▒", "███████▒▒▒", "████████▒▒", "█████████▒",
			// "██████████", "▒█████████", "▒▒████████", "▒▒▒███████", "▒▒▒▒██████", "▒▒▒▒██████", "▒▒▒▒▒█████", "▒▒▒▒▒▒████", "▒▒▒▒▒▒▒███", "▒▒▒▒▒▒▒▒██", "▒▒▒▒▒▒▒▒▒█",
			"🌑", "🌘", "🌗", "🌖", "🌕", "🌔", "🌓", "🌒",
		}
		i := 0
		for range l.ticker.C {
			if l.isRunning {
				l.lk.Lock()
				i = i % len(spinnerChars)
				fmt.Fprintf(l.writer, "\r\033[K %s Preparing to throw Frisbii — %s", spinnerChars[i], l.status)
				i++
				l.lk.Unlock()
			} else {
				l.ticker.Stop()
				break
			}
		}
	}()
}

func (l *Loader) SetStatus(status string) {
	l.lk.Lock()
	defer l.lk.Unlock()
	l.status = status
}

func (l *Loader) Stop() {
	if l.isRunning {
		l.isRunning = false
		l.wg.Wait()
		if !winTerm {
			fmt.Fprint(l.writer, "\r\033[K\033[?25h") // show cursor
		}
	}
}

func (l *Loader) IsRunning() bool {
	return l.isRunning
}
