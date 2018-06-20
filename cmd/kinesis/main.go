package main

import (
	"github.com/segmentio/conf"
	"github.com/segmentio/events"
	_ "github.com/segmentio/events/ecslogs"
	_ "github.com/segmentio/events/text"
	"os"
	"github.com/segmentio/events/ecslogs"
	kin "github.com/segmentio/kinesis-go-lib/lib/kinesis"
)

type Config struct{
	Stream	string	`conf:"stream" help:"Kinesis stream name"`
	Shards	int		`conf:"shards" help:"Number of Kinesis shards"`
	Debug	bool	`conf:"debug" help:"enables debug logging"`
}

func main() {

	config:= Config{
		Debug:	true,
		Shards: 1,
	}
	conf.Load(&config)

	events.DefaultHandler = ecslogs.NewHandler(os.Stdout)

	// Log the starting
	events.Log("starting service", events.Args{
		{Name: "config", Value: config.Stream},
	})

	streamConfig := &kin.StreamConfig{
		Stream:	config.Stream,
		Shards:	int64(config.Shards),
	}

	stream , err := kin.NewStream(streamConfig)
	if err != nil {
		events.Log(" Error creating new stream")
	}

	err = stream.CreateStream()
	if err != nil {
		events.Log("Error creating new stream", events.Args {
			{Name: "error", Value: err},
		})
		return
	}

	//check if stream exists. Dont create new one if exists
	output, err := stream.DescribeStream()
	if err != nil {
		events.Log("Error retrieving stream metadata", events.Args {
			{Name: "error", Value: err},
		})
		return
	}
	events.Log("Stream status : %v",*output.StreamDescription.StreamStatus)
}

