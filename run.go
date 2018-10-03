package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/cyverse-de/road-runner/dcompose"
	"github.com/cyverse-de/road-runner/fs"
	"github.com/kr/pty"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/cyverse-de/messaging.v6"
	"gopkg.in/cyverse-de/model.v4"
)

// logrusProxyWriter will prevent
// "Error while reading from Writer: bufio.Scanner: token too long" errors
// if a docker command generates a lot of output
// (from pulling many input containers at once, for example)
// and Logrus attempts to log all of that output in one log line.
type logrusProxyWriter struct {
	entry *logrus.Entry
}

func (w *logrusProxyWriter) Write(b []byte) (int, error) {
	return fmt.Fprintf(w.entry.Writer(), string(b))
}

var logWriter = &logrusProxyWriter{
	entry: log,
}

// JobRunner provides the functionality needed to run jobs.
type JobRunner struct {
	client      JobUpdatePublisher
	exit        chan messaging.StatusCode
	job         *model.Job
	status      messaging.StatusCode
	cfg         *viper.Viper
	logsDir     string
	volumeDir   string
	workingDir  string
	projectName string
	tmpDir      string
}

// NewJobRunner creates a new JobRunner
func NewJobRunner(client JobUpdatePublisher, job *model.Job, cfg *viper.Viper, exit chan messaging.StatusCode) (*JobRunner, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	runner := &JobRunner{
		client:     client,
		exit:       exit,
		job:        job,
		cfg:        cfg,
		status:     messaging.Success,
		workingDir: cwd,
		volumeDir:  path.Join(cwd, dcompose.VOLUMEDIR),
		logsDir:    path.Join(cwd, dcompose.VOLUMEDIR, "logs"),
		tmpDir:     path.Join(cwd, dcompose.TMPDIR),
	}
	return runner, nil
}

// Init will initialize the state for a JobRunner. The volumeDir and logsDir
// will get created.
func (r *JobRunner) Init() error {
	err := os.MkdirAll(r.logsDir, 0755)
	if err != nil {
		return err
	}

	err = os.MkdirAll(r.tmpDir, 0755)
	if err != nil {
		return err
	}

	// Set world-write perms on volumeDir, so non-root users can create job outputs.
	err = os.Chmod(r.volumeDir, 0777)
	if err != nil {
		// Log error and continue.
		log.Error(err)
	}

	// Set world-write perms on tmpDir, so non-root users can create temp outputs.
	err = os.Chmod(r.tmpDir, 0777)
	if err != nil {
		// Log error and continue.
		log.Error(err)
	}

	// Copy docker-compose file to the log dir for debugging purposes.
	err = fs.CopyFile(fs.FS, "docker-compose.yml", path.Join(r.logsDir, "docker-compose.yml"))
	if err != nil {
		// Log error and continue.
		log.Error(err)
	}

	// Copy upload exclude list to the log dir for debugging purposes.
	err = fs.CopyFile(fs.FS, dcompose.UploadExcludesFilename, path.Join(r.logsDir, dcompose.UploadExcludesFilename))
	if err != nil {
		// Log error and continue.
		log.Error(err)
	}

	// Copy input path list to the log dir for debugging purposes.
	if r.job.InputPathListFile != "" {
		err = fs.CopyFile(fs.FS, r.job.InputPathListFile, path.Join(r.logsDir, r.job.InputPathListFile))
		if err != nil {
			// Log error and continue.
			log.Error(err)
		}
	}

	transferTrigger, err := os.Create(path.Join(r.logsDir, "de-transfer-trigger.log"))
	if err != nil {
		return err
	}
	defer transferTrigger.Close()
	_, err = transferTrigger.WriteString("This is only used to force HTCondor to transfer files.")
	if err != nil {
		return err
	}

	if _, err = os.Stat("iplant.cmd"); err != nil {
		if err = os.Rename("iplant.cmd", path.Join(r.logsDir, "iplant.cmd")); err != nil {
			return err
		}
	}

	return nil
}

// DockerLogin will run "docker login" with credentials sent with the job.
func (r *JobRunner) DockerLogin() error {
	var err error
	dockerBin := r.cfg.GetString("docker.path")
	// Login so that images can be pulled.
	var authinfo *authInfo
	for _, img := range r.job.ContainerImages() {
		if img.Auth != "" {
			authinfo, err = parse(img.Auth)
			if err != nil {
				return err
			}
			authCommand := exec.Command(
				dockerBin,
				"login",
				"--username",
				authinfo.Username,
				"--password",
				authinfo.Password,
				parseRepo(img.Name),
			)
			f, err := pty.Start(authCommand)
			if err != nil {
				return err
			}
			go func() {
				io.Copy(logWriter, f)
			}()
			err = authCommand.Wait()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// JobUpdatePublisher is the interface for types that need to publish a job
// update.
type JobUpdatePublisher interface {
	PublishJobUpdate(m *messaging.UpdateMessage) error
}

func (r *JobRunner) createDataContainers(ctx context.Context) (messaging.StatusCode, error) {
	var (
		err error
	)
	composePath := r.cfg.GetString("docker-compose.path")
	for stepIndex, step := range r.job.Steps {
		for dcIndex := range step.Component.Container.VolumesFrom {
			svcname := fmt.Sprintf("data_%d_%d", stepIndex, dcIndex)
			running(r.client, r.job, fmt.Sprintf("creating data container %s", svcname))
			dataCommand := exec.CommandContext(
				ctx,
				composePath,
				"-p",
				r.projectName,
				"-f",
				"docker-compose.yml",
				"up",
				"--abort-on-container-exit",
				"--exit-code-from", svcname,
				"--no-color",
				svcname,
			)
			dataCommand.Env = os.Environ()
			dataCommand.Stderr = logWriter
			dataCommand.Stdout = logWriter
			if err = dataCommand.Run(); err != nil {
				running(r.client, r.job, fmt.Sprintf("error creating data container %s: %s", svcname, err.Error()))
				return messaging.StatusDockerCreateFailed, errors.Wrapf(err, "failed to create data container %s", svcname)
			}
			running(r.client, r.job, fmt.Sprintf("finished creating data container %s", svcname))
		}
	}
	return messaging.Success, nil
}

func (r *JobRunner) downloadInputs(ctx context.Context) (messaging.StatusCode, error) {
	env := os.Environ()
	env = append(env, fmt.Sprintf("VAULT_ADDR=%s", r.cfg.GetString("vault.url")))
	env = append(env, fmt.Sprintf("VAULT_TOKEN=%s", r.cfg.GetString("vault.token")))
	composePath := r.cfg.GetString("docker-compose.path")
	if job.InputPathListFile != "" {
		return r.downloadInputStep(ctx, "download_inputs", job.InputPathListFile, composePath, env)
	}
	for index, input := range r.job.Inputs() {
		svcname := fmt.Sprintf("input_%d", index)
		if status, err := r.downloadInputStep(ctx, svcname, input.IRODSPath(), composePath, env); err != nil {
			return status, err
		}
	}

	return messaging.Success, nil
}

func (r *JobRunner) downloadInputStep(ctx context.Context, svcname, inputPath, composePath string, env []string) (messaging.StatusCode, error) {
	var (
		exitCode int64
	)
	running(r.client, r.job, fmt.Sprintf("Downloading %s", inputPath))
	stderr, err := os.Create(path.Join(r.logsDir, fmt.Sprintf("logs-stderr-%s", svcname)))
	if err != nil {
		log.Error(err)
	}
	defer stderr.Close()
	stdout, err := os.Create(path.Join(r.logsDir, fmt.Sprintf("logs-stdout-%s", svcname)))
	if err != nil {
		log.Error(err)
	}
	defer stdout.Close()
	downloadCommand := exec.CommandContext(
		ctx,
		composePath,
		"-p", r.projectName,
		"-f", "docker-compose.yml",
		"up",
		"--no-color",
		"--abort-on-container-exit",
		"--exit-code-from", svcname,
		svcname,
	)
	downloadCommand.Env = env
	downloadCommand.Stderr = stderr
	downloadCommand.Stdout = stdout
	if err = downloadCommand.Run(); err != nil {
		running(r.client, r.job, fmt.Sprintf("error downloading %s: %s", inputPath, err.Error()))
		return messaging.StatusInputFailed, errors.Wrapf(err, "failed to download %s with an exit code of %d", inputPath, exitCode)
	}
	stdout.Close()
	stderr.Close()
	running(r.client, r.job, fmt.Sprintf("finished downloading %s", inputPath))

	return messaging.Success, nil
}

type authInfo struct {
	Username string
	Password string
}

func parse(b64 string) (*authInfo, error) {
	jsonstring, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}
	a := &authInfo{}
	err = json.Unmarshal(jsonstring, a)
	return a, err
}

func (r *JobRunner) runAllSteps(ctx context.Context) (messaging.StatusCode, error) {
	var err error

	for idx, step := range r.job.Steps {
		running(r.client, r.job,
			fmt.Sprintf(
				"Running tool container %s:%s with arguments: %s",
				step.Component.Container.Image.Name,
				step.Component.Container.Image.Tag,
				strings.Join(step.Arguments(), " "),
			),
		)

		stdout, err := os.Create(path.Join(r.logsDir, fmt.Sprintf("docker-compose-step-stdout-%d", idx)))
		if err != nil {
			log.Error(err)
		}
		defer stdout.Close()

		stderr, err := os.Create(path.Join(r.logsDir, fmt.Sprintf("docker-compose-step-stderr-%d", idx)))
		if err != nil {
			log.Error(err)
		}
		defer stderr.Close()

		composePath := r.cfg.GetString("docker-compose.path")
		svcname := fmt.Sprintf("step_%d", idx)
		runCommand := exec.CommandContext(
			ctx,
			composePath,
			"-p", r.projectName,
			"-f", "docker-compose.yml",
			"up",
			"--abort-on-container-exit",
			"--exit-code-from", svcname,
			"--no-color",
			svcname,
		)
		runCommand.Env = os.Environ()
		runCommand.Stdout = stdout
		runCommand.Stderr = stderr
		err = runCommand.Run()

		if err != nil {
			running(r.client, r.job,
				fmt.Sprintf(
					"Error running tool container %s:%s with arguments '%s': %s",
					step.Component.Container.Image.Name,
					step.Component.Container.Image.Tag,
					strings.Join(step.Arguments(), " "),
					err.Error(),
				),
			)

			return messaging.StatusStepFailed, err
		}

		running(r.client, r.job,
			fmt.Sprintf("Tool container %s:%s with arguments '%s' finished successfully",
				step.Component.Container.Image.Name,
				step.Component.Container.Image.Tag,
				strings.Join(step.Arguments(), " "),
			),
		)
		// stdout.Close()
		// stderr.Close()
	}
	return messaging.Success, err
}

func (r *JobRunner) uploadOutputs() (messaging.StatusCode, error) {
	var err error
	composePath := r.cfg.GetString("docker-compose.path")
	stdout, err := os.Create(path.Join(r.logsDir, fmt.Sprintf("logs-stdout-output")))
	if err != nil {
		log.Error(err)
	}
	defer stdout.Close()
	stderr, err := os.Create(path.Join(r.logsDir, fmt.Sprintf("logs-stderr-output")))
	if err != nil {
		log.Error(err)
	}
	defer stderr.Close()
	outputCommand := exec.Command(
		composePath,
		"-p", r.projectName,
		"-f", "docker-compose.yml",
		"up",
		"--no-color",
		"--abort-on-container-exit",
		"--exit-code-from", "upload_outputs",
		"upload_outputs",
	)
	outputCommand.Env = []string{
		fmt.Sprintf("VAULT_ADDR=%s", r.cfg.GetString("vault.url")),
		fmt.Sprintf("VAULT_TOKEN=%s", r.cfg.GetString("vault.token")),
	}
	outputCommand.Stdout = stdout
	outputCommand.Stderr = stderr
	err = outputCommand.Run()

	if err != nil {
		running(r.client, r.job, fmt.Sprintf("Error uploading outputs to %s: %s", r.job.OutputDirectory(), err.Error()))
		return messaging.StatusOutputFailed, errors.Wrapf(err, "failed to upload outputs to %s", r.job.OutputDirectory())
	}

	running(r.client, r.job, fmt.Sprintf("Done uploading outputs to %s", r.job.OutputDirectory()))
	return messaging.Success, nil
}

func parseRepo(imagename string) string {
	if strings.Contains(imagename, "/") {
		parts := strings.Split(imagename, "/")
		return parts[0]
	}
	return ""
}

// Run executes the job, and returns the exit code on the exit channel.
func Run(ctx context.Context, client JobUpdatePublisher, job *model.Job, cfg *viper.Viper, exit chan messaging.StatusCode) {
	host, err := os.Hostname()
	if err != nil {
		log.Error(err)
		host = "UNKNOWN"
	}

	runner, err := NewJobRunner(client, job, cfg, exit)
	if err != nil {
		log.Error(err)
	}

	err = runner.Init()
	if err != nil {
		log.Error(err)
	}

	runner.projectName = strings.Replace(runner.job.InvocationID, "-", "", -1)

	// let everyone know the job is running
	running(runner.client, runner.job, fmt.Sprintf("Job %s is running on host %s", runner.job.InvocationID, host))

	if err = runner.DockerLogin(); err != nil {
		log.Error(err)
	}

	networkName := fmt.Sprintf("%s_default", runner.projectName)
	dockerPath := cfg.GetString("docker.path")
	networkCreateCmd := exec.CommandContext(ctx, dockerPath, "network", "create", "--driver", "bridge", networkName)
	networkCreateCmd.Env = os.Environ()
	networkCreateCmd.Dir = runner.workingDir
	networkCreateCmd.Stdout = logWriter
	networkCreateCmd.Stderr = logWriter

	err = networkCreateCmd.Run()
	if err != nil {
		log.Error(err) // don't need to fail, since docker-compose is *supposed* to create the network
	}

	composePath := cfg.GetString("docker-compose.path")
	pullCommand := exec.CommandContext(ctx, composePath, "-p", runner.projectName, "-f", "docker-compose.yml", "pull", "--parallel")
	pullCommand.Env = os.Environ()
	pullCommand.Dir = runner.workingDir
	pullCommand.Stdout = logWriter
	pullCommand.Stderr = logWriter

	err = pullCommand.Run()
	if err != nil {
		log.Error(err)
		runner.status = messaging.StatusDockerPullFailed
	}

	if err = fs.WriteJobSummary(fs.FS, runner.logsDir, job); err != nil {
		log.Error(err)
	}

	if err = fs.WriteJobParameters(fs.FS, runner.logsDir, job); err != nil {
		log.Error(err)
	}

	if runner.status == messaging.Success {
		if runner.status, err = runner.createDataContainers(ctx); err != nil {
			log.Error(err)
		}
	}

	// If pulls didn't succeed then we can't guarantee that we've got the
	// correct versions of the tools. Don't bother pulling in data in that case,
	// things are already screwed up.
	if runner.status == messaging.Success {
		if runner.status, err = runner.downloadInputs(ctx); err != nil {
			log.Error(err)
		}
	}
	// Only attempt to run the steps if the input downloads succeeded. No reason
	// to run the steps if there's no/corrupted data to operate on.
	if runner.status == messaging.Success {
		if runner.status, err = runner.runAllSteps(ctx); err != nil {
			log.Error(err)
		}
	}
	// Always attempt to transfer outputs. There might be logs that can help
	// debug issues when the job fails.
	var outputStatus messaging.StatusCode
	running(runner.client, runner.job, fmt.Sprintf("Beginning to upload outputs to %s", runner.job.OutputDirectory()))
	if outputStatus, err = runner.uploadOutputs(); err != nil {
		log.Error(err)
	}
	if outputStatus != messaging.Success {
		runner.status = outputStatus
	}
	// Always inform upstream of the job status.
	if runner.status != messaging.Success {
		fail(runner.client, runner.job, fmt.Sprintf("Job exited with a status of %d", runner.status))
	} else {
		success(runner.client, runner.job)
	}
	exit <- runner.status
}
