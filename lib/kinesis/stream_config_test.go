package kinesis

import "testing"

func TestInvalidStreamConfig(t *testing.T) {
	config := &StreamConfig{}
	err := config.defaults()

	if err == nil {
		t.Errorf("error expected for missing stream name")
	}
	if err != NoStreamNameErr {
		t.Errorf("%v error expected for missing stream name. Got %v", NoStreamNameErr, err)
	}
}

func TestStreamConfig(t *testing.T) {
	config := &StreamConfig{
		Stream: "mock-stream",
	}
	err := config.defaults()

	if err != nil {
		t.Errorf("error not expected %v", err)
	}

	if config.Shards != DefaultShardCount {
		t.Errorf("Shards : %v != %v", config.Shards, DefaultShardCount)
	}
}
