package kinesis

import "testing"

func TestCreateProducer(t *testing.T) {
	config := &ProducerConfig{
		Stream: "mock-stream",
	}
	p, err := CreateProducer(config)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

	ty, ok := p.(*RecordProducer)
	if !ok {
		t.Errorf("unexpected producer type %v", ty)
	}
}

func TestInvalidCreateProducer(t *testing.T) {
	config := &ProducerConfig{
		Stream: "mock-stream",
		Type:   "invalid",
	}
	_, err := CreateProducer(config)
	if err == nil {
		t.Errorf("expected error as producer type is invalid")
	}
	if err != InvalidProducerErr {
		t.Errorf("%v error expected. Got %v", InvalidProducerErr, err)
	}
}
