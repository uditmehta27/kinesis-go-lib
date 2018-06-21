package kinesis

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/segmentio/events"
)

func GetKinesisConnection() (*kinesis.Kinesis, error) {
	awsSession, err := session.NewSession()
	if err != nil {
		events.Log("error creating aws session %{error}v", err)
		return nil, err
	}
	kconn := kinesis.New(awsSession)
	return kconn, nil
}
