package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func Setup(ctx context.Context, serviceName string) (shutdown func(context.Context) error, err error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
		resource.WithProcess(),
		resource.WithOS(),
	)
	if err != nil {
		return nil, fmt.Errorf("create otel resource: %w", err)
	}

	traceShutdown, err := setupTracer(ctx, res)
	if err != nil {
		return nil, err
	}

	metricShutdown, err := setupMeter(ctx, res)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context) error {
		var errs []error
		for _, fn := range []func(context.Context) error{traceShutdown, metricShutdown} {
			if err := fn(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("otel shutdown: %v", errs)
		}
		return nil
	}, nil
}

func setupTracer(ctx context.Context, res *resource.Resource) (func(context.Context) error, error) {
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

func setupMeter(ctx context.Context, res *resource.Resource) (func(context.Context) error, error) {
	exp, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create metric exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	return mp.Shutdown, nil
}
