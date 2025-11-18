package common

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

// DefaultLogger returns a lazily initialized shared logger instance.
func DefaultLogger() *golog.Logger {
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
