package main

import (
	"os/exec"
	"strings"

	"github.com/cyverse-de/messaging"
	"github.com/spf13/viper"
)

func cleanup(cfg *viper.Viper) {
	var err error
	projName := strings.Replace(job.InvocationID, "-", "", -1) // dumb hack
	downCommand := exec.Command(cfg.GetString("docker-compose.path"), "-p", projName, "-f", "docker-compose.yml", "down", "--rmi", "all", "-v")
	downCommand.Stderr = log.Writer()
	downCommand.Stdout = log.Writer()
	if err = downCommand.Run(); err != nil {
		log.Errorf("%+v", err)
	}
}

// Exit handles clean up when road-runner is killed.
func Exit(cfg *viper.Viper, exit, finalExit chan messaging.StatusCode) {
	exitCode := <-exit
	log.Warnf("Received an exit code of %d, cleaning up", int(exitCode))
	cleanup(cfg)
	finalExit <- exitCode
}
