package kinesis

import "errors"

type Producer interface {
	Start()
	Produce(pkey string, msg []byte) (error)
	Stop()
}

type ProduceMessage struct {
	Message []byte
	PartitionKey string
	Response chan<- error
}


var (
	NotImplementedErr = errors.New("producer type not implemented")
	InvalidProducerErr = errors.New("producer type invalid")
)

func CreateProducer (conf *ProducerConfig) (Producer, error) {
	err := conf.defaults()
	if err != nil {
		return nil, err
	}

	var producer Producer

	switch  conf.Type {

	case "record":
		producer, err = NewSimpleProducer(conf)
		if err != nil{
			return nil, err
		}
		return producer,nil

	case "batch":
		return nil, NotImplementedErr

	default:
		return nil, InvalidProducerErr
	}
}


