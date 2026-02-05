package metrics

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// MetricsConfig holds configuration for the metrics system.
type MetricsConfig struct {
	OTLPEndpoint     string
	ExportIntervalMS int
	ServiceName      string
	GlobalTags       map[string]string
	Enabled          bool
}

// MetricCreator provides thread-safe metric recording capabilities.
// All methods are safe for concurrent use by multiple goroutines.
type MetricCreator struct {
	meterProvider  *sdkmetric.MeterProvider
	meter          metric.Meter
	counterCache   sync.Map // map[string]metric.Int64Counter
	histogramCache sync.Map // map[string]metric.Float64Histogram
	globalTags     map[string]string // Immutable after initialization
}

var (
	instance *MetricCreator
	once     sync.Once
	initErr  error
)

// InitMetricCreator initializes the global MetricCreator singleton.
// This must be called before GetMetricCreator. It is safe to call multiple
// times; only the first call will initialize the singleton.
func InitMetricCreator(config MetricsConfig) error {
	once.Do(func() {
		mc, err := newMetricCreator(config)
		if err != nil {
			initErr = err
			return
		}
		instance = mc
	})
	return initErr
}

// GetMetricCreator returns the global MetricCreator singleton.
// Returns nil if InitMetricCreator has not been called or failed.
func GetMetricCreator() *MetricCreator {
	return instance
}

func newMetricCreator(config MetricsConfig) (*MetricCreator, error) {
	ctx := context.Background()

	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(config.OTLPEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(config.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
			exporter,
			sdkmetric.WithInterval(time.Duration(config.ExportIntervalMS)*time.Millisecond),
		)),
		sdkmetric.WithResource(res),
	)

	// Defensive copy of global tags to ensure immutability
	globalTags := make(map[string]string, len(config.GlobalTags))
	for k, v := range config.GlobalTags {
		globalTags[k] = v
	}

	return &MetricCreator{
		meterProvider: provider,
		meter:         provider.Meter(config.ServiceName),
		globalTags:    globalTags,
	}, nil
}

// RecordCounter records an integer counter metric.
// Safe for concurrent use by multiple goroutines.
func (mc *MetricCreator) RecordCounter(ctx context.Context, name string, value int64, unit, description string, tags map[string]string) error {
	if mc == nil {
		return nil // Graceful degradation if metrics not initialized
	}

	counter, err := mc.getOrCreateCounter(name, unit, description)
	if err != nil {
		return err
	}

	attrs := mc.buildAttributes(tags)
	counter.Add(ctx, value, metric.WithAttributes(attrs...))
	return nil
}

// RecordHistogram records a floating-point histogram metric.
// Safe for concurrent use by multiple goroutines.
func (mc *MetricCreator) RecordHistogram(ctx context.Context, name string, value float64, unit, description string, tags map[string]string) error {
	if mc == nil {
		return nil
	}

	histogram, err := mc.getOrCreateHistogram(name, unit, description)
	if err != nil {
		return err
	}

	attrs := mc.buildAttributes(tags)
	histogram.Record(ctx, value, metric.WithAttributes(attrs...))
	return nil
}

func (mc *MetricCreator) getOrCreateCounter(name, unit, description string) (metric.Int64Counter, error) {
	// Fast path: instrument already cached
	if cached, ok := mc.counterCache.Load(name); ok {
		return cached.(metric.Int64Counter), nil
	}

	// Slow path: create the instrument
	counter, err := mc.meter.Int64Counter(
		name,
		metric.WithUnit(unit),
		metric.WithDescription(description),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create counter %s: %w", name, err)
	}

	// Atomic store-if-absent handles race with other goroutines
	actual, _ := mc.counterCache.LoadOrStore(name, counter)
	return actual.(metric.Int64Counter), nil
}

func (mc *MetricCreator) getOrCreateHistogram(name, unit, description string) (metric.Float64Histogram, error) {
	if cached, ok := mc.histogramCache.Load(name); ok {
		return cached.(metric.Float64Histogram), nil
	}

	histogram, err := mc.meter.Float64Histogram(
		name,
		metric.WithUnit(unit),
		metric.WithDescription(description),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create histogram %s: %w", name, err)
	}

	actual, _ := mc.histogramCache.LoadOrStore(name, histogram)
	return actual.(metric.Float64Histogram), nil
}

func (mc *MetricCreator) buildAttributes(callTags map[string]string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(mc.globalTags)+len(callTags))

	// Global tags first
	for k, v := range mc.globalTags {
		attrs = append(attrs, attribute.String(k, v))
	}

	// Call-specific tags (may override globals)
	for k, v := range callTags {
		attrs = append(attrs, attribute.String(k, v))
	}

	return attrs
}

// Shutdown gracefully shuts down the meter provider, flushing any pending metrics.
func (mc *MetricCreator) Shutdown(ctx context.Context) error {
	if mc == nil || mc.meterProvider == nil {
		return nil
	}
	return mc.meterProvider.Shutdown(ctx)
}

// LoadMetricsConfig loads metrics configuration from environment variables.
func LoadMetricsConfig() MetricsConfig {
	endpoint := os.Getenv("OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4317" // Default gRPC port
	}

	intervalStr := os.Getenv("OTLP_EXPORT_INTERVAL_MS")
	interval := 6000 // Default 6 seconds
	if intervalStr != "" {
		if val, err := strconv.Atoi(intervalStr); err == nil {
			interval = val
		}
	}

	enabled := os.Getenv("OTLP_ENABLED")
	isEnabled := enabled == "" || enabled == "true" // Enabled by default

	return MetricsConfig{
		OTLPEndpoint:     endpoint,
		ExportIntervalMS: interval,
		ServiceName:      "osmo-operator",
		GlobalTags:       make(map[string]string),
		Enabled:          isEnabled,
	}
}
