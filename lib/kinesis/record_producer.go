package kinesis

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/segmentio/events"
	"sync"
	"time"
)

type RecordProducer struct {
	mtx     sync.Mutex
	stream  string
	started bool

	//kinesis client
	client Client

	//communication channels
	requests  chan ProduceRequest
	done      chan struct{}
	semaphore chan struct{}
	join      sync.WaitGroup
}

//Creates a new Producer
func NewRecordProducer(config *ProducerConfig) (*RecordProducer, error) {
	err := config.defaults()

	if err != nil {
		return nil, err
	}

	return &RecordProducer{
		stream:    config.Stream,
		requests:  make(chan ProduceRequest, config.BackLogSize),
		done:      make(chan struct{}),
		client:    config.Client,
		semaphore: make(chan struct{}, config.Concurrency),
	}, nil
}

//Publish a single record to the Kinesis stream
func (producer *RecordProducer) Produce(pkey string, msg []byte) error {

	if !producer.started {
		return ProducerNotStartedErr
	}

	if len(msg) == 0 || pkey == "" {
		return InvalidPublishRequest
	}
	response := make(chan error)
	producer.requests <- ProduceRequest{
		Message:      msg,
		PartitionKey: pkey,
		Response:     response,
	}
	return <-response
}

//Starts producer
func (producer *RecordProducer) Start() {
	producer.mtx.Lock()
	defer producer.mtx.Unlock() // only 1 thread should be calling this

	if producer.started {
		return
	}
	producer.started = true
	producer.join.Add(1) //1 producer

	//TODO : Check if stream exists in Kinesis before producing

	events.Debug("starting producer to consume from stream :  %v", producer.stream)
	go producer.run()

}

//Stops the producer
func (producer *RecordProducer) Stop() {
	producer.mtx.Lock()
	defer producer.mtx.Unlock() //ensures channel is only closed once

	events.Debug("stopping producer")

	if !producer.started {
		return
	}

	producer.started = false

	close(producer.done)
	time.Sleep(500 * time.Millisecond)
	close(producer.requests)

	//Wait for all go routines to return
	for i := 0; i < cap(producer.semaphore); i++ {
		producer.semaphore <- struct{}{}
	}

	close(producer.semaphore)
	producer.join.Wait()
}

func (producer *RecordProducer) run() {

	reqChan := producer.requests

	send := func(req ProduceRequest) {
		//get semaphore slot
		producer.semaphore <- struct{}{}

		go producer.write(req)
	}

	defer producer.join.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case req, ok := <-reqChan:
			if !ok {
				events.Log("error reading from request channel. exiting!")
				return
			}

			events.Debug("received request to send partitionkey : %v", req.PartitionKey)
			//Implement send logic here
			send(req)

		case <-producer.done:
			events.Log("received done signal. stopping producer")
			return

		case _ = <-ticker.C:
			//TODO
		}
	}
}

//function to write record to Kinesis
func (producer *RecordProducer) write(request ProduceRequest) {
	defer func() {
		<-producer.semaphore //release slot
	}()

	putRecordInput := &kinesis.PutRecordInput{
		Data:         request.Message,
		PartitionKey: aws.String(request.PartitionKey),
		StreamName:   aws.String(producer.stream),
	}

	output, err := producer.client.PutRecord(putRecordInput)
	if err != nil {
		request.finishRequest(err)
		events.Log("[%v] error writing single record %{error}v", err)
		return
	}
	events.Debug("record written successfully. sequence id %v, shard id %v", output.SequenceNumber, output.ShardId)
	request.finishRequest(nil)
}
