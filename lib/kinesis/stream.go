package kinesis

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/segmentio/events"
)

type KinesisStream struct {
	kinesis *kinesis.Kinesis
	Stream  string
	Shards  int64
}

func NewStream(conf *StreamConfig) (*KinesisStream, error) {
	err := conf.defaults()
	if err != nil {
		return nil, err
	}
	kconn, err := GetKinesisConnection()
	if err != nil {
		return nil, err
	}
	return &KinesisStream{
		kinesis: kconn,
		Stream:  conf.Stream,
		Shards:  conf.Shards,
	}, nil
}

//Create stream and wait for initialization
func (kin *KinesisStream) CreateStream() error {
	streamConfig := &kinesis.CreateStreamInput{
		StreamName: aws.String(kin.Stream),
		ShardCount: aws.Int64(int64(kin.Shards)),
	}

	_, err := kin.kinesis.CreateStream(streamConfig)
	if err != nil {
		events.Log("error creating kinesis stream %v %{error}v", kin.Stream, err)
		return err
	}
	events.Log("Stream initialization in progress : %v", kin.Stream)

	return kin.kinesis.WaitUntilStreamExists(&kinesis.DescribeStreamInput{
		StreamName: aws.String(kin.Stream),
	})
}

func (kin *KinesisStream) DescribeStream() (*kinesis.DescribeStreamOutput, error) {
	input := &kinesis.DescribeStreamInput{
		StreamName: aws.String(kin.Stream),
	}
	return kin.kinesis.DescribeStream(input)
}

//TODO
func (kin *KinesisStream) DeleteStream() error {
	return nil
}
