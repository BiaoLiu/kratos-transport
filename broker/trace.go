package broker

import (
	"context"

	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	TraceID = "traceid"
)

type tracerKey struct{}
type spanKey struct{}

func ContextWithMessageHeader(ctx context.Context, header Headers) context.Context {
	if len(header) > 0 {
		traceID, err := trace.TraceIDFromHex(header[TraceID])
		if err == nil {
			spanContextConfig := trace.SpanContextConfig{
				TraceID:    traceID,
				TraceFlags: 01,
				Remote:     false,
			}
			spanContext := trace.NewSpanContext(spanContextConfig)
			ctx = trace.ContextWithSpanContext(ctx, spanContext)
		}
	}
	return ctx
}

func StartTrace(ctx context.Context, operation string, header Headers, carrier propagation.TextMapCarrier) context.Context {
	ctx = ContextWithMessageHeader(ctx, header)
	tracer := tracing.NewTracer(trace.SpanKindServer)
	if carrier == nil {
		carrier = make(propagation.MapCarrier)
	}
	ctx, span := tracer.Start(ctx, operation, carrier)
	ctx = newTraceContext(ctx, tracer)
	ctx = newSpanContext(ctx, span)
	return ctx
}

func EndTrace(ctx context.Context, err error) {
	tracer := fromTracerContext(ctx)
	span := fromSpanContext(ctx)
	if tracer != nil && span != nil {
		tracer.End(ctx, span, nil, err)
	}
}

func newTraceContext(ctx context.Context, tracer *tracing.Tracer) context.Context {
	ctx = context.WithValue(ctx, tracerKey{}, tracer)
	return ctx
}

func newSpanContext(ctx context.Context, span trace.Span) context.Context {
	ctx = context.WithValue(ctx, spanKey{}, span)
	return ctx
}

func fromTracerContext(ctx context.Context) *tracing.Tracer {
	if tracer, ok := ctx.Value(tracerKey{}).(*tracing.Tracer); ok {
		return tracer
	}
	return nil
}

func fromSpanContext(ctx context.Context) trace.Span {
	if span, ok := ctx.Value(spanKey{}).(trace.Span); ok {
		return span
	}
	return nil
}
