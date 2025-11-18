package exchange

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

func defaultLogger() *golog.Logger {
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
