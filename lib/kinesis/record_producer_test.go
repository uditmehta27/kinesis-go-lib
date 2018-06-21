package kinesis

import (
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"
	"sync"
	"testing"
)

type MockResponse struct {
	output *kinesis.PutRecordOutput
	err    error
}

type MockRequest struct {
	key  string
	data []byte
}

type MockKinesisClient struct {
	response *MockResponse
	requests map[string][]byte
}

func (client *MockKinesisClient) PutRecord(inp *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
	resp := client.response

	if resp.err != nil {
		return nil, resp.err
	}

	//Store all requests
	client.requests[*inp.PartitionKey] = inp.Data

	return resp.output, nil
}

type test struct {
	name   string
	conf   *ProducerConfig
	client *MockKinesisClient
	record []MockRequest
}

var cases = []test{
	{
		"test1 : write single record",
		&ProducerConfig{Stream: "mock-stream"},
		&MockKinesisClient{
			&MockResponse{
				&kinesis.PutRecordOutput{},
				nil,
			},
			make(map[string][]byte),
		},
		[]MockRequest{
			MockRequest{"key1", []byte("data1")},
		},
	},
	{
		"test2 : write multiple records",
		&ProducerConfig{Stream: "mock-stream"},
		&MockKinesisClient{
			&MockResponse{
				&kinesis.PutRecordOutput{},
				nil,
			},
			make(map[string][]byte),
		},
		[]MockRequest{
			MockRequest{"key1", []byte("data1")},
			MockRequest{"key2", []byte("data2")},
			MockRequest{"key3", []byte("data3")},
		},
	},
}

var putErrCase = test{

	"Kinesis Put Error",
	&ProducerConfig{Stream: "invalid-stream"},
	&MockKinesisClient{
		&MockResponse{
			&kinesis.PutRecordOutput{},
			errors.New(kinesis.ErrCodeResourceNotFoundException),
		},
		make(map[string][]byte),
	},
	[]MockRequest{
		MockRequest{"key1", []byte("data1")},
	},
}

var invalidMsgCase = []test{
	{
		"Invalid key",
		&ProducerConfig{Stream: "invalid-stream"},
		&MockKinesisClient{
			&MockResponse{
				&kinesis.PutRecordOutput{},
				nil,
			},
			make(map[string][]byte),
		},
		[]MockRequest{
			MockRequest{"", []byte("foo")},
		},
	},
	{
		"Invalid Msg",
		&ProducerConfig{Stream: "invalid-stream"},
		&MockKinesisClient{
			&MockResponse{
				&kinesis.PutRecordOutput{},
				nil,
			},
			make(map[string][]byte),
		},
		[]MockRequest{
			MockRequest{"foo", []byte("")},
		},
	},
}

func TestNewRecordProducerWithoutStreamName(t *testing.T) {
	config := &ProducerConfig{}
	p, err := NewRecordProducer(config)

	if p != nil {
		t.Errorf("producer should be nil")
	}
	if err == nil {
		t.Errorf("creating producer without stream shold cause error. err == nil")
	}
}

func TestNewRecordProducerWithValidStreamName(t *testing.T) {
	config := &ProducerConfig{
		Stream: "mock-stream",
	}
	p, err := NewRecordProducer(config)

	if p == nil {
		t.Errorf("producer == nil")
	}
	if err != nil {
		t.Errorf("err != nil")
	}
}

func TestDefaultProducerConfigs(t *testing.T) {
	config := &ProducerConfig{
		Stream: "mock-stream",
	}
	p, _ := NewRecordProducer(config)

	if p.started {
		t.Errorf("producer should be stopped initially")
	}
}

func TestProduceToStoppedProducer(t *testing.T) {
	config := &ProducerConfig{
		Stream: "mock-stream",
	}
	p, _ := NewRecordProducer(config)

	err := p.Produce("key", []byte("foo"))
	if err == nil {
		t.Errorf("error expected. attempting to produce to not stopped producer yet")
	}
	if err != ProducerNotStartedErr {
		t.Errorf("%v error expected while attempting to produce to stopped producer", InvalidProducerErr)
	}
}

func TestInvalidProduceMsg(t *testing.T) {
	for _, test := range invalidMsgCase {
		test.conf.Client = test.client
		p, _ := NewRecordProducer(test.conf)
		p.Start()

		var wg sync.WaitGroup
		wg.Add(len(test.record))

		for _, r := range test.record {
			go func(k string, v []byte) {
				err := p.Produce(k, v)
				if err == nil {
					t.Errorf("expected error while producing invalid message")
					return
				}
				if err != InvalidPublishRequest {
					t.Errorf("%v error expected while attempting to publish invalid msg", InvalidPublishRequest)
				}
				wg.Done()
			}(r.key, r.data)
		}
		wg.Wait()
		p.Stop()
	}
}

func TestRemotePutErr(t *testing.T) {
	c := putErrCase
	c.conf.Client = c.client

	p, _ := NewRecordProducer(c.conf)
	p.Start()

	err := p.Produce(c.record[0].key, c.record[0].data)
	if err == nil {
		t.Errorf("error expected while trying to produce msg. err == nil")
	}

	if err.Error() != kinesis.ErrCodeResourceNotFoundException {
		t.Errorf("%v != %v", err.Error(), kinesis.ErrCodeResourceNotFoundException)
	}
	p.Stop()

}

func TestProducer(t *testing.T) {
	for _, test := range cases {
		test.conf.Client = test.client
		p, _ := NewRecordProducer(test.conf)
		p.Start()

		var wg sync.WaitGroup
		wg.Add(len(test.record))

		for _, r := range test.record {
			go func(k string, v []byte) {
				err := p.Produce(k, v)
				if err != nil {
					t.Errorf("error not expected while producing valid message : %v", err)
				}
				wg.Done()
			}(r.key, r.data)
		}
		wg.Wait()
		p.Stop()

		reqLen := len(test.client.requests)
		expLen := len(test.record)
		reqMap := test.client.requests

		if len(test.client.requests) != len(test.record) {
			t.Errorf("all produce requests not received. Expected %v, got %v", reqLen, expLen)
		}

		for _, r := range test.record {
			if len(reqMap[r.key]) != len(r.data) {
				t.Errorf("message sizes dont match sent:%v, received:%v bytes", len(r.data), len(reqMap[r.key]))
			}
		}
	}
}

func TestStop(t *testing.T) {
	//Running test on a valid mock
	test := cases[0]
	test.conf.Client = test.client
	p, _ := NewRecordProducer(test.conf)
	p.Start()

	p.Stop()

	//check if started is false
	if p.started {
		t.Errorf("producer.started should be false after producer.Stop()")
	}

	_, ok := (<-p.requests)
	if ok {
		t.Errorf("producer request channel not closed")
	}

	_, ok = (<-p.done)
	if ok {
		t.Errorf("producer done channel not closed")
	}
}
