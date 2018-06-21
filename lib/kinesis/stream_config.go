package kinesis

type StreamConfig struct {
	Stream string
	Shards int64
}

const (
	DefaultShardCount = 1
)

func (config *StreamConfig) defaults() error {
	if config.Stream == "" {
		return NoStreamNameErr
	}
	if config.Shards == 0 {
		config.Shards = DefaultShardCount
	}
	return nil
}
