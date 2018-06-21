package kinesis

type ProducerConfig struct {
	Stream       string
	Concurrency  int
	LogPrefix    string
	MetricPrefix string
	BatchSize    int
	Type         string
	Client       Client
	BackLogSize  int
}

const (
	DefaultLogPrefix = "kinesis-producer"

	DefaultMetricPrefix = "producer"

	DefaultConcurrency = 3

	DefaultProducerType = "record"

	DefaultBackLogSize = 10
)

func (conf *ProducerConfig) defaults() error {
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
	if conf.Type == "" {
		conf.Type = DefaultProducerType
	}
	if conf.BackLogSize == 0 {
		conf.BackLogSize = DefaultBackLogSize
	}
	return nil
}
