package kinesis

import "testing"

func TestInvalidProducerConfig(t *testing.T) {
	config := &ProducerConfig{}
	err := config.defaults()

	if err == nil {
		t.Errorf("error expected for missing stream name")
	}
	if err != NoStreamNameErr {
		t.Errorf("%v error expected for missing stream name. Got %v", NoStreamNameErr, err)
	}
}

func TestProducerConfig(t *testing.T) {
	config := &ProducerConfig{
		Stream: "mock-stream",
	}
	err := config.defaults()

	if err != nil {
		t.Errorf("error not expected %v", err)
	}

	if config.Concurrency != DefaultConcurrency {
		t.Errorf("Concurrency : %v != %v", config.Concurrency, DefaultConcurrency)
	}
	if config.BackLogSize != DefaultBackLogSize {
		t.Errorf("BackLogSize : %v != %v", config.BackLogSize, DefaultBackLogSize)
	}
	if config.Type != DefaultProducerType {
		t.Errorf("Type : %v != %v", config.Type, DefaultProducerType)
	}
	if config.LogPrefix != DefaultLogPrefix {
		t.Errorf("LogPrefix : %v != %v", config.LogPrefix, DefaultLogPrefix)
	}
	if config.MetricPrefix != DefaultMetricPrefix {
		t.Errorf("MetricPrefix : %v != %v", config.MetricPrefix, DefaultMetricPrefix)
	}
}
