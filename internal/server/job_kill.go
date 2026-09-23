package server

import (
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

const jobMarkerEnv = dockercli.MarkerEnv

func killMarkedProcesses(jobID string, grace time.Duration) int {
	return dockercli.KillMarked(jobID, grace)
}
