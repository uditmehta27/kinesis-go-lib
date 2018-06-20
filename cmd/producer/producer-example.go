package main

import (
	"github.com/segmentio/conf"
	"github.com/segmentio/events"
	"github.com/segmentio/events/ecslogs"
	"os"
	"github.com/segmentio/kinesis-go-lib/lib/kinesis"
	"time"
	"fmt"
)

type Config struct{
	Stream	string	`conf:"stream" help:"Kinesis stream name"`
	Shards	int		`conf:"shards" help:"Number of Kinesis shards"`
	Debug	bool	`conf:"debug" help:"enables debug logging"`
	Type	string	`conf:"type" help:"producer type"`
}

func main() {
	config := Config{
		Debug:	true,
		Shards: 1,
		Type:	"records",
	}
	conf.Load(&config)

	events.DefaultHandler = ecslogs.NewHandler(os.Stdout)

	// Log the starting
	events.Log("starting service", events.Args{
		{Name: "config", Value: config.Stream},
	})

	kconn, err := kinesis.GetKinesisConnection()

	pconfig := kinesis.ProducerConfig{
		Stream:			config.Stream,
		Concurrency:	3,
		Type:			config.Type,
		Client:			kconn,
	}

	p, err := kinesis.CreateProducer(&pconfig)
	events.Log("Producer configs %v",pconfig)

	if err != nil {
		events.Log("error creating new producer %v", err)
		return
	}

	p.Start()

	for i := 0 ; i < 100; i ++ {
		err = p.Produce(fmt.Sprintf("key-%v",i), []byte(fmt.Sprintf("hello there - %v", i)))
		time.Sleep(5 * time.Second)
		if err != nil {
			events.Log("error publishing record %{error]v", err)
		}
	}

	time.Sleep(20 * time.Second)
	events.Log("Time expired. Stopping")

	p.Stop()
}
