package server

import (
	"context"
	"strings"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

const buildCompressionOpts = "compression=zstd,compression-level=1"

func imageOutputArgs(ctx context.Context, imageRef string) []string {
	return imageOutputArgsFor(imageRef, dockercli.ImageExportOptsSupported(ctx))
}

func imageOutputArgsFor(imageRef string, fastExport bool) []string {
	if !fastExport || !safeImageRef(imageRef) {
		return []string{"-t", imageRef}
	}
	return []string{"--output", "type=image,name=" + imageRef + "," + buildCompressionOpts}
}

func safeImageRef(ref string) bool {
	if strings.TrimSpace(ref) == "" || ref != strings.TrimSpace(ref) {
		return false
	}
	return !strings.ContainsAny(ref, ",=\"' \t\r\n")
}
