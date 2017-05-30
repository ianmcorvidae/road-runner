package dcompose

import (
	"fmt"
	"path"
	"strconv"

	"github.com/cyverse-de/model"
	"github.com/spf13/viper"
)

// WORKDIR is the path to the working directory inside all of the containers
// that are run as part of a job.
const WORKDIR = "/de-app-work"

// CONFIGDIR is the path to the local configs inside the containers that are
// used to transfer files into and out of the job.
const CONFIGDIR = "/configs"

// VOLUMEDIR is the name of the directory that is used for the working directory
// volume.
const VOLUMEDIR = "workingvolume"

const (
	// TypeLabel is the label key applied to every container.
	TypeLabel = "org.iplantc.containertype"

	// InputContainer is the value used in the TypeLabel for input containers.
	InputContainer = iota

	// DataContainer is the value used in the TypeLabel for data containers.
	DataContainer

	// StepContainer is the value used in the TypeLabel for step containers.
	StepContainer

	// OutputContainer is the value used in the TypeLabel for output containers.
	OutputContainer
)

// Volume is a Docker volume definition in the Docker compose file.
type Volume struct {
	Driver  string
	Options map[string]string `yaml:"driver_opts"`
}

// Network is a Docker network definition in the docker-compose file.
type Network struct {
	Driver string
	// EnableIPv6 bool              `yaml:"enable_ipv6"`
	DriverOpts map[string]string `yaml:"driver_opts"`
}

// LoggingConfig configures the logging for a docker-compose service.
type LoggingConfig struct {
	Driver  string
	Options map[string]string `yaml:"driver_opts,omitempty"`
}

// ServiceNetworkConfig configures a docker-compose service to use a Docker
// Network.
type ServiceNetworkConfig struct {
	Aliases []string `yaml:",omitempty"`
}

// Service configures a docker-compose service.
type Service struct {
	CapAdd        []string          `yaml:"cap_add,flow"`
	CapDrop       []string          `yaml:"cap_drop,flow"`
	Command       []string          `yaml:",omitempty"`
	ContainerName string            `yaml:"container_name,omitempty"`
	CPUSet        string            `yaml:"cpuset,omitempty"`
	CPUShares     string            `yaml:"cpu_shares,omitempty"`
	CPUQuota      string            `yaml:"cpu_quota,omitempty"`
	DependsOn     []string          `yaml:"depends_on,omitempty"`
	Devices       []string          `yaml:",omitempty"`
	DNS           []string          `yaml:",omitempty"`
	DNSSearch     []string          `yaml:"dns_search,omitempty"`
	TMPFS         []string          `yaml:",omitempty"`
	EntryPoint    string            `yaml:",omitempty"`
	Environment   map[string]string `yaml:",omitempty"`
	Expose        []string          `yaml:",omitempty"`
	Image         string
	Labels        map[string]string                `yaml:",omitempty"`
	Logging       *LoggingConfig                   `yaml:",omitempty"`
	MemLimit      string                           `yaml:"mem_limit,omitempty"`
	MemSwapLimit  string                           `yaml:"memswap_limit,omitempty"`
	MemSwappiness string                           `yaml:"mem_swappiness,omitempty"`
	NetworkMode   string                           `yaml:"network_mode,omitempty"`
	Networks      map[string]*ServiceNetworkConfig `yaml:",omitempty"`
	Ports         []string                         `yaml:",omitempty"`
	Volumes       []string                         `yaml:",omitempty"`
	VolumesFrom   []string                         `yaml:"volumes_from,omitempty"`
	WorkingDir    string                           `yaml:"working_dir,omitempty"`
}

// JobCompose is the top-level type for what will become a job's docker-compose
// file.
type JobCompose struct {
	Version  string `yaml:"version"`
	Volumes  map[string]*Volume
	Networks map[string]*Network `yaml:",omitempty"`
	Services map[string]*Service
}

// New returns a newly instantiated *JobCompose instance.
func New() *JobCompose {
	return &JobCompose{
		Version:  "2",
		Volumes:  make(map[string]*Volume),
		Networks: make(map[string]*Network),
		Services: make(map[string]*Service),
	}
}

// InitFromJob fills out values as appropriate for running in the DE's Condor
// Cluster.
func (j *JobCompose) InitFromJob(job *model.Job, cfg *viper.Viper, workingdir string) {
	volpath := path.Join(workingdir, VOLUMEDIR)
	// The volume containing the local working directory
	j.Volumes[job.InvocationID] = &Volume{
		Driver: "local",
		Options: map[string]string{
			"type":   "none",
			"device": volpath,
			"o":      "bind",
		},
	}

	porklockImage := cfg.GetString("porklock.image")
	porklockTag := cfg.GetString("porklock.tag")
	porklockImageName := fmt.Sprintf("%s:%s", porklockImage, porklockTag)

	for index, dc := range job.DataContainers() {
		svcKey := fmt.Sprintf("data_%d", index)
		j.Services[svcKey] = &Service{
			Image:         fmt.Sprintf("%s:%s", dc.Name, dc.Tag),
			ContainerName: fmt.Sprintf("%s-%s", dc.NamePrefix, job.InvocationID),
			EntryPoint:    "/bin/true",
			Logging:       &LoggingConfig{Driver: "none"},
			Labels: map[string]string{
				model.DockerLabelKey: strconv.Itoa(DataContainer),
			},
		}

		svc := j.Services[svcKey]
		if dc.HostPath != "" || dc.ContainerPath != "" {
			var rw string
			if dc.ReadOnly {
				rw = "ro"
			} else {
				rw = "rw"
			}
			svc.Volumes = []string{
				fmt.Sprintf("%s:%s:%s", dc.HostPath, dc.ContainerPath, rw),
			}
		}
	}

	for index, input := range job.Inputs() {
		j.Services[fmt.Sprintf("input_%d", index)] = &Service{
			CapAdd:  []string{"IPC_LOCK"},
			Image:   porklockImageName,
			Command: input.Arguments(job.Submitter, job.FileMetadata),
			Environment: map[string]string{
				"VAULT_ADDR":  "${VAULT_ADDR}",
				"VAULT_TOKEN": "${VAULT_TOKEN}",
				"JOB_UUID":    job.InvocationID,
			},
			WorkingDir: WORKDIR,
			Volumes: []string{
				fmt.Sprintf("%s:%s:rw", job.InvocationID, WORKDIR),
			},
			Labels: map[string]string{
				model.DockerLabelKey: strconv.Itoa(InputContainer),
			},
		}
	}

	// Add the steps to the docker-compose file.
	for index, step := range job.Steps {
		j.ConvertStep(&step, index, job.Submitter, job.InvocationID)
	}

	// Add the final output job
	j.Services["upload_outputs"] = &Service{
		CapAdd:  []string{"IPC_LOCK"},
		Image:   porklockImageName,
		Command: job.FinalOutputArguments(),
		Environment: map[string]string{
			"VAULT_ADDR":  "${VAULT_ADDR}",
			"VAULT_TOKEN": "${VAULT_TOKEN}",
			"JOB_UUID":    job.InvocationID,
		},
		WorkingDir: WORKDIR,
		Volumes: []string{
			fmt.Sprintf("%s:%s:%s", job.InvocationID, WORKDIR, "rw"),
		},
		Networks: map[string]*ServiceNetworkConfig{
			job.InvocationID: &ServiceNetworkConfig{},
		},
		Labels: map[string]string{
			model.DockerLabelKey: job.InvocationID,
			TypeLabel:            strconv.Itoa(OutputContainer),
		},
	}
}

// ConvertStep will add the job step to the JobCompose services
func (j *JobCompose) ConvertStep(step *model.Step, index int, user, invID string) {
	// Construct the name of the image
	// Set the name of the image for the container.
	var imageName string
	if step.Component.Container.Image.Tag != "" {
		imageName = fmt.Sprintf(
			"%s:%s",
			step.Component.Container.Image.Name,
			step.Component.Container.Image.Tag,
		)
	} else {
		imageName = step.Component.Container.Image.Name
	}

	step.Environment["IPLANT_USER"] = user
	step.Environment["IPLANT_EXECUTION_ID"] = invID

	var containername string
	if step.Component.Container.Name != "" {
		containername = step.Component.Container.Name
	} else {
		containername = fmt.Sprintf("step_%d_%s", index, invID)
	}

	j.Services[fmt.Sprintf("step_%d", index)] = &Service{
		Image:      imageName,
		Command:    step.Arguments(),
		WorkingDir: step.Component.Container.WorkingDirectory(),
		Labels: map[string]string{
			model.DockerLabelKey: strconv.Itoa(StepContainer),
		},
		ContainerName: containername,
		Environment:   step.Environment,
		VolumesFrom:   []string{},
		Volumes:       []string{},
		Devices:       []string{},
	}

	svc := j.Services[fmt.Sprintf("step_%d", index)]
	stepContainer := step.Component.Container

	if stepContainer.EntryPoint != "" {
		svc.EntryPoint = stepContainer.EntryPoint
	}

	if stepContainer.MemoryLimit > 0 {
		svc.MemLimit = strconv.FormatInt(stepContainer.MemoryLimit, 10)
	}

	if stepContainer.CPUShares > 0 {
		svc.CPUShares = strconv.FormatInt(stepContainer.CPUShares, 10)
	}

	if stepContainer.NetworkMode != "" {
		svc.NetworkMode = stepContainer.NetworkMode
	}

	// Handles volumes created by other containers.
	for _, vf := range stepContainer.VolumesFrom {
		containerName := fmt.Sprintf("%s-%s", vf.NamePrefix, invID)
		var foundService string
		for svckey, svc := range j.Services { // svckey is the docker-compose service name.
			if svc.ContainerName == containerName {
				foundService = svckey
			}
		}
		svc.VolumesFrom = append(svc.VolumesFrom, foundService)
	}

	// The working directory needs to be mounted as a volume.
	svc.Volumes = append(svc.Volumes, fmt.Sprintf("%s:%s:rw", invID, stepContainer.WorkingDirectory()))

	for _, v := range stepContainer.Volumes {
		var rw string
		if v.ReadOnly {
			rw = "ro"
		} else {
			rw = "rw"
		}
		if v.HostPath == "" {
			svc.Volumes = append(svc.Volumes, fmt.Sprintf("%s:%s", v.ContainerPath, rw))
		} else {
			svc.Volumes = append(svc.Volumes, fmt.Sprintf("%s:%s:%s", v.HostPath, v.ContainerPath, rw))
		}
	}

	for _, device := range stepContainer.Devices {
		svc.Devices = append(svc.Devices,
			fmt.Sprintf("%s:%s:%s",
				device.HostPath,
				device.ContainerPath,
				device.CgroupPermissions,
			),
		)
	}
}
