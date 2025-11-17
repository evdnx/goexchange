package logutil

import (
	"fmt"
	"sync"

	"github.com/evdnx/golog"
)

var (
	sharedLogger     *golog.Logger
	sharedLoggerOnce sync.Once
	sharedLoggerErr  error
)

// Default returns a lazily constructed shared logger.
func Default() *golog.Logger {
	sharedLoggerOnce.Do(func() {
		sharedLogger, sharedLoggerErr = golog.NewLogger(
			golog.WithStdOutProvider(golog.ConsoleEncoder),
			golog.WithLevel(golog.InfoLevel),
		)
	})

	if sharedLoggerErr != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", sharedLoggerErr))
	}

	return sharedLogger
}
