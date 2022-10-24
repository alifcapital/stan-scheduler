package scheduler

import (
	"bytes"
	"encoding/gob"
	"log"
	"time"

	"github.com/nats-io/stan.go"
)

type Scheduler struct {
	stan         stan.Conn
	retryTimeout time.Duration
}

func New(stanConn stan.Conn, retryTimeout time.Duration) (Scheduler, error) {
	s := Scheduler{
		stan:         stanConn,
		retryTimeout: retryTimeout,
	}

	if _, err := stanConn.QueueSubscribe(
		"schedule_event",
		"shisui",
		s.cb,
		stan.DurableName("shisui"),
		stan.SetManualAckMode(),
		stan.AckWait(retryTimeout),
	); err != nil {
		return Scheduler{}, err
	}

	return s, nil
}

func (s Scheduler) Publish(subj string, t time.Time, payload []byte) error {
	evt := event{
		Subj:    subj,
		Payload: payload,
		Time:    t,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(evt); err != nil {
		return err
	}

	return s.stan.Publish("schedule_event", buf.Bytes())
}

func (s Scheduler) cb(msg *stan.Msg) {
	var evt event

	if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&evt); err != nil {
		log.Printf("can't decode message (got error %s)\n", err)
		_ = msg.Ack()
		return
	}

	if time.Now().Before(evt.Time) {
		return
	}

	if err := s.stan.Publish(evt.Subj, evt.Payload); err != nil {
		log.Printf("can't publish message (got error %s, subject=%s)\n", err, evt.Subj)
		return
	}

	_ = msg.Ack()
}

type event struct {
	Subj    string
	Payload []byte
	Time    time.Time
}
