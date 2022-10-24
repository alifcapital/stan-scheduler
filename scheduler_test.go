package scheduler_test

import (
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"

	scheduler "github.com/alifcapital/stan-scheduler"
)

func TestPublish(t *testing.T) {
	rand.Seed(time.Now().Unix())

	natsConn, err := nats.Connect("0.0.0.0:4222", nats.Timeout(time.Minute))
	if err != nil {
		log.Fatalln("can't connect nats", err)
	}

	stanConn, err := stan.Connect("test-cluster", "test-"+strconv.Itoa(rand.Int()), stan.NatsConn(natsConn))
	if err != nil {
		log.Fatalln("can't connect stan", err)
	}

	sch, err := scheduler.New(stanConn, 30*time.Second)
	if err != nil {
		log.Fatalln("can't init scheduler", err)
	}

	var (
		deliverAt = time.Now().Add(time.Minute)
		payload   = []byte(strconv.Itoa(rand.Int()))
	)

	if err := sch.Publish("kuksi", deliverAt, payload); err != nil {
		log.Fatalln("can't publish delayed message via scheduler", err)
	}

	done := make(chan struct{})

	if _, err := stanConn.Subscribe("kuksi", func(msg *stan.Msg) {
		if string(msg.Data) != string(payload) {
			t.Log("outdated data")
			return
		}

		defer func() { done <- struct{}{} }()

		if now := time.Now(); now.Before(deliverAt) {
			t.Fatal("it is too early!", now.Sub(deliverAt))
		}

		t.Log("EAT!", time.Now().Sub(deliverAt))
	}); err != nil {
		log.Fatalln("can't subscribe stan", err)
	}

	<-done
}
