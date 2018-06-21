package kinesis

import "errors"

var (
	NotImplementedErr  = errors.New("producer type not implemented")
	InvalidProducerErr = errors.New("producer type invalid")
)

type Producer interface {
	Start()
	Produce(pkey string, msg []byte) error
	Stop()
}

type ProduceRequest struct {
	Message      []byte
	PartitionKey string
	Response     chan<- error
}

//Finish request to unblock the caller
func (m ProduceRequest) finishRequest(err error) {
	if m.Response != nil {
		m.Response <- err
	}
}

func CreateProducer(conf *ProducerConfig) (Producer, error) {
	err := conf.defaults()
	if err != nil {
		return nil, err
	}

	var producer Producer

	switch conf.Type {

	case "record":
		producer, err = NewRecordProducer(conf)
		if err != nil {
			return nil, err
		}
		return producer, nil

	case "batch":
		return nil, NotImplementedErr

	default:
		return nil, InvalidProducerErr
	}
}
