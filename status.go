package main

import (
	"os"

	"gopkg.in/cyverse-de/messaging.v2"
	"gopkg.in/cyverse-de/model.v1"
)

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		log.Errorf("Couldn't get the hostname: %s", err.Error())
		return ""
	}
	return h
}

func fail(client JobUpdatePublisher, job *model.Job, msg string) error {
	log.Error(msg)
	return client.PublishJobUpdate(&messaging.UpdateMessage{
		Job:     job,
		State:   messaging.FailedState,
		Message: msg,
		Sender:  hostname(),
	})
}

func success(client JobUpdatePublisher, job *model.Job) error {
	log.Info("Job success")
	return client.PublishJobUpdate(&messaging.UpdateMessage{
		Job:    job,
		State:  messaging.SucceededState,
		Sender: hostname(),
	})
}

func running(client JobUpdatePublisher, job *model.Job, msg string) {
	err := client.PublishJobUpdate(&messaging.UpdateMessage{
		Job:     job,
		State:   messaging.RunningState,
		Message: msg,
		Sender:  hostname(),
	})
	if err != nil {
		log.Error(err)
	}
	log.Info(msg)
}
