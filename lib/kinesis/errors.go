package kinesis

import "errors"

var (
	InvalidPublishRequest = errors.New("invalid publish request. empty msg or partition key")
	NoStreamNameErr       = errors.New("stream name not provided in configs")
	ProducerNotStartedErr = errors.New("producer not started yet. use p.Start() first")
)
