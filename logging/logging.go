package logging

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport"
)

type Logging func(ctx context.Context, req interface{}, latency float64, handleErr error)

func Server(logger log.Logger) Logging {
	return func(ctx context.Context, req interface{}, latency float64, err error) {
		var (
			code   int32
			reason string
			kind   string
		)
		if info, ok := transport.FromServerContext(ctx); ok {
			kind = info.Kind().String()
		}
		if se := errors.FromError(err); se != nil {
			code = se.Code
			reason = se.Reason
		}
		level, stack := extractError(err)
		_ = log.WithContext(ctx, logger).Log(level,
			"kind", "rocketmq",
			"component", kind,
			"args", extractArgs(req),
			"code", code,
			"reason", reason,
			"stack", stack,
			"latency", latency,
		)
		return
	}
}

// extractArgs returns the string of the req
func extractArgs(req interface{}) string {
	bytes, _ := json.Marshal(req)
	return string(bytes)
}

// extractError returns the string of the error
func extractError(err error) (log.Level, string) {
	if err != nil {
		return log.LevelError, fmt.Sprintf("%+v", err)
	}
	return log.LevelInfo, ""
}
