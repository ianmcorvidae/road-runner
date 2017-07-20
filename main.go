// road-runner
//
// Executes jobs based on a JSON blob serialized to a file.
// Each step of the job runs inside a Docker container. Job results are
// transferred back into iRODS with the porklock tool. Job status updates are
// posted to the **jobs.updates** topic in the **jobs** exchange.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	yaml "gopkg.in/yaml.v2"

	"github.com/Sirupsen/logrus"
	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/logcabin"
	"github.com/cyverse-de/messaging"
	"github.com/cyverse-de/model"
	"github.com/cyverse-de/road-runner/dcompose"
	"github.com/cyverse-de/road-runner/fs"
	"github.com/cyverse-de/version"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"github.com/spf13/viper"
)

var (
	job              *model.Job
	client           *messaging.Client
	amqpExchangeName string
	amqpExchangeType string
)

var log = logrus.WithFields(logrus.Fields{
	"service": "road-runner",
	"art-id":  "road-runner",
	"group":   "org.cyverse",
})

func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
}

// CleanableJob is a job definition that contains extra information that allows
// external tools to clean up after a job.
type CleanableJob struct {
	model.Job
	LocalWorkingDir string `json:"local_working_directory"`
}

func main() {
	var (
		showVersion = flag.Bool("version", false, "Print the version information")
		jobFile     = flag.String("job", "", "The path to the job description file")
		cfgPath     = flag.String("config", "", "The path to the config file")
		writeTo     = flag.String("write-to", "/opt/image-janitor", "The directory to copy job files to.")
		composePath = flag.String("docker-compose", "docker-compose.yml", "The filepath to use when writing the docker-compose file.")
		composeBin  = flag.String("docker-compose-path", "/usr/bin/docker-compose", "The path to the docker-compose binary.")
		dockerBin   = flag.String("docker-path", "/usr/bin/docker", "The path to the docker binary.")
		dockerCfg   = flag.String("docker-cfg", "/var/lib/condor/.docker", "The path to the .docker directory.")
		logdriver   = flag.String("log-driver", "de-logging", "The name of the Docker log driver to use in job steps.")
		pathprefix  = flag.String("path-prefix", "/var/lib/condor/", "The path prefix for the stderr/stdout logs.")
		err         error
		cfg         *viper.Viper
	)
	logcabin.Init("road-runner", "road-runner")
	sigquitter := make(chan bool)
	sighandler := InitSignalHandler()
	sighandler.Receive(
		sigquitter,
		func(sig os.Signal) {
			log.Info("Received signal:", sig)
			if job == nil {
				log.Warn("Info didn't get parsed from the job file, can't clean up. Probably don't need to.")
			}
			if job != nil {
				cleanup(cfg)
			}
			if client != nil && job != nil {
				fail(client, job, fmt.Sprintf("Received signal %s", sig))
			}
			os.Exit(-1)
		},
		func() {
			log.Info("Signal handler is quitting")
		},
	)
	signal.Notify(
		sighandler.Signals,
		os.Interrupt,
		os.Kill,
		syscall.SIGTERM,
		syscall.SIGSTOP,
		syscall.SIGQUIT,
	)

	flag.Parse()

	if *showVersion {
		version.AppVersion()
		os.Exit(0)
	}

	if *cfgPath == "" {
		log.Fatal("--config must be set.")
	}

	log.Infof("Reading config from %s\n", *cfgPath)
	if _, err = os.Open(*cfgPath); err != nil {
		log.Fatal(*cfgPath)
	}

	cfg, err = configurate.Init(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("Done reading config from %s\n", *cfgPath)

	if *jobFile == "" {
		log.Fatal("--job must be set.")
	}

	cfg.Set("docker-compose.path", *composeBin)
	cfg.Set("docker.path", *dockerBin)
	cfg.Set("docker.cfg", *dockerCfg)

	wd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	data, err := ioutil.ReadFile(*jobFile)
	if err != nil {
		log.Fatal(err)
	}

	job, err = model.NewFromData(cfg, data)
	if err != nil {
		log.Fatal(err)
	}

	//create a cleanable version of the job.
	cleanable := &CleanableJob{*job, wd}

	cleanablejson, err := json.Marshal(cleanable)
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to marshal json for job cleaning"))
	}

	// Check for the existence of the path at *writeTo
	if _, err = os.Open(*writeTo); err != nil {
		log.Fatal(err)
	}

	// Write out the cleanable job JSON to the *writeTo directory.
	if err = fs.WriteJob(fs.FS, job.InvocationID, *writeTo, cleanablejson); err != nil {
		log.Fatal(err)
	}

	uri := cfg.GetString("amqp.uri")
	amqpExchangeName = cfg.GetString("amqp.exchange.name")
	amqpExchangeType = cfg.GetString("amqp.exchange.type")
	client, err = messaging.NewClient(uri, true)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	client.SetupPublishing(amqpExchangeName)

	// Generate the docker-compose file used to execute the job.
	composer, err := dcompose.New(*logdriver, *pathprefix)
	if err != nil {
		log.Fatal(err)
	}
	composer.InitFromJob(job, cfg, wd)
	c, err := os.Create(*composePath)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	m, err := yaml.Marshal(composer)
	if err != nil {
		log.Fatal(err)
	}
	_, err = c.Write(m)
	if err != nil {
		log.Fatal(err)
	}
	c.Close()

	// The channel that the exit code will be passed along on.
	exit := make(chan messaging.StatusCode)
	// Could probably reuse the exit channel, but that's less explicit.
	finalExit := make(chan messaging.StatusCode)
	// Launch the go routine that will handle job exits by signal or timer.
	go Exit(cfg, exit, finalExit)
	go client.Listen()
	client.AddDeletableConsumer(
		amqpExchangeName,
		amqpExchangeType,
		messaging.StopQueueName(job.InvocationID),
		messaging.StopRequestKey(job.InvocationID),
		func(d amqp.Delivery) {
			d.Ack(false)
			running(client, job, "Received stop request")
			exit <- messaging.StatusKilled
		})
	go Run(client, job, cfg, exit)
	exitCode := <-finalExit
	if err = fs.DeleteJobFile(fs.FS, job.InvocationID, *writeTo); err != nil {
		log.Errorf("%+v", err)
	}
	os.Exit(int(exitCode))
}
