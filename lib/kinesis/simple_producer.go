package kinesis

import (
	"github.com/aws/aws-sdk-go/service/kinesis"
	"sync"
	"errors"
	"github.com/segmentio/events"
	"time"
	"github.com/aws/aws-sdk-go/aws"
)


type SimpleProducer struct {
	stream string
	batchSize int

	//client to connect to kinesis
	client	Client
	//communication channels
	requests chan ProduceMessage
	done chan struct {}
	join sync.WaitGroup
}


//Creates a new Producer
func NewSimpleProducer(config *ProducerConfig) (*SimpleProducer, error) {

	return &SimpleProducer{
		stream:		config.Stream,
		requests:	make(chan ProduceMessage, config.Concurrency),
		done:		make(chan struct{}),
		client:		config.Client,
	}, nil
}

//Publish a msg to the Kinesis stream
func (producer *SimpleProducer)Produce (pkey string, msg []byte) (error) {
	if len(msg) == 0 || pkey == "" {
		return InvalidPublishRequest
	}
	response := make(chan error)
	producer.requests <- ProduceMessage{
		Message:		msg,
		PartitionKey:	pkey,
		Response:		response,
	}
	return <-response
}

//Starts producers to produce to Kinesis
func (producer *SimpleProducer) Start() {
	numProducers := cap(producer.requests)
	producer.join.Add(numProducers)

	//TODO : Check if stream exists in Kinesis before producing

	events.Debug("starting %v producer threads", numProducers)
	for i := 0; i < numProducers; i ++ {
		//Run producer threads
		go producer.run(i)
	}

}

//Stops the producers
func (producer *SimpleProducer) Stop() {
	err := errors.New("stopping producers")

	close(producer.done)
	time.Sleep(500 * time.Millisecond)
	close(producer.requests)

	for i := range producer.requests {
		i.finishRequest(err)
	}

	producer.join.Wait()
}


func (producer *SimpleProducer) run(id int) {

	var reqChan <-chan ProduceMessage
	var kconn *kinesis.Kinesis

	setup := func() (err error) {
		events.Log("[%v] setting up kinesis connection", id)

		kconn, err = GetKinesisConnection()
		reqChan = producer.requests


		if err != nil || kconn == nil {
			events.Log("error setting up kinesis connection")
			return
		}
		return
	}

	cleanup := func() {
		kconn = nil
		reqChan = nil
		//cleanup buffered messages
	}


	defer producer.join.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	setup()

	for {
		select {
		case req, ok := <- reqChan :
			if ! ok {
				events.Log("[%v]error reading from request channel. exiting!", id)
				return
			}

			events.Log("[%v]Received request to send %v",id, req.PartitionKey)
			//Implement send logic here
			_, err := producer.write(kconn, req)
			if err != nil {
				req.finishRequest(err)
				cleanup()
				events.Log("[%v] error writing single record %{error}v. reconnecting to kinesis", err, id)
				continue
			}
			req.finishRequest(nil)


		case <-producer.done :
			events.Log("[%v] Received done signal", id)
			return

		case _ = <-ticker.C :
			if kconn == nil {
				setup()
			}
		}
	}
}


//Finish request to unblock the caller
func (m ProduceMessage) finishRequest(err error) {
	if m.Response != nil {
		m.Response <- err
	}
}


func (producer *SimpleProducer) write(kconn *kinesis.Kinesis, request ProduceMessage) (resp *kinesis.PutRecordOutput, err error) {
	putRecordInput := &kinesis.PutRecordInput{
		Data:			request.Message,
		PartitionKey:	aws.String(request.PartitionKey),
		StreamName:		aws.String(producer.stream),
	}
	return kconn.PutRecord(putRecordInput)
}