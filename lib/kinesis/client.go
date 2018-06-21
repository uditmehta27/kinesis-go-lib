package kinesis

import "github.com/aws/aws-sdk-go/service/kinesis"

//Help ease mocking
type Client interface {
	PutRecord(inp *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error)
}
