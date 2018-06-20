package kinesis

import "github.com/aws/aws-sdk-go/service/kinesis"

type ProducerConfig struct {
	Stream string
	Concurrency int
	LogPrefix string
	MetricPrefix string
	BatchSize int
	Type string
	Client Client
}

type Client interface {
	PutRecord(inp *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error)
}

const (
	// DefaultLogPrefix is the default prefix for log messages.
	DefaultLogPrefix = "kinesis-producer"

	// DefaultMetricPrefix is the default prefix for metrics.
	DefaultMetricPrefix = "producer"

	DefaultConcurrency = 1

	DefaultBatchSize = 10

	DefaultProducerType = "record"
)

func (conf *ProducerConfig) defaults() (error) {
	if conf.Stream == "" {
		return NoStreamNameErr
	}
	if conf.Concurrency == 0 {
		conf.Concurrency = DefaultConcurrency
	}
	if conf.LogPrefix == "" {
		conf.LogPrefix = DefaultLogPrefix
	}
	if conf.MetricPrefix == "" {
		conf.MetricPrefix = DefaultMetricPrefix
	}
	if conf.BatchSize == 0 {
		conf.BatchSize = DefaultBatchSize
	}
	if conf.Type == "" {
		conf.Type = DefaultProducerType
	}
	return nil
}
