package exchange

import (
	"github.com/evdnx/goexchange/internal/logutil"
	"github.com/evdnx/golog"
)

func defaultLogger() *golog.Logger {
	return logutil.Default()
}
