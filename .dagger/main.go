// A generated module for ValkeyClusterOperator functions
//
// This module has been generated via dagger init and serves as a reference to
// basic module structure as you get started with Dagger.
//
// Two functions have been pre-created. You can modify, delete, or add to them,
// as needed. They demonstrate usage of arguments and return types using simple
// echo and grep commands. The functions can be called from the dagger CLI or
// from one of the SDKs.
//
// The first line in this comment block is a short description line and the
// rest is a long description with more detail on the module's purpose or usage,
// if appropriate. All modules should have a short description.

package main

import (
	"context"
	"dagger/valkey-cluster-operator/internal/dagger"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/blang/semver/v4"
)

type ValkeyClusterOperator struct{}

// Returns a container that echoes whatever string argument is provided
func (m *ValkeyClusterOperator) ContainerEcho(stringArg string) *dagger.Container {
	return dag.Container().From("alpine:latest").WithExec([]string{"echo", stringArg})
}

// Returns lines that match a pattern in the files of the provided Directory
func (m *ValkeyClusterOperator) GrepDir(ctx context.Context, directoryArg *dagger.Directory, pattern string) (string, error) {
	return dag.Container().
		From("alpine:latest").
		WithMountedDirectory("/mnt", directoryArg).
		WithWorkdir("/mnt").
		WithExec([]string{"grep", "-R", pattern, "."}).
		Stdout(ctx)
}

// Build the application binary
func (m *ValkeyClusterOperator) BuildManager(
	ctx context.Context,
	// +defaultPath="/"
	source *dagger.Directory,
	// +default="linux/amd64"
	platform dagger.Platform,
) *dagger.File {

	parts := strings.Split(string(platform), "/")
	goos, goarch := parts[0], parts[1]

	builder := dag.Container().
		From("golang:1.24").
		WithWorkdir("/workspace").
		WithFile("/workspace/go.mod", source.File("go.mod")).
		WithFile("/workspace/go.sum", source.File("go.sum")).
		WithFile("/workspace/cmd/main.go", source.File("cmd/main.go")).
		WithDirectory("/workspace/api", source.Directory("api")).
		WithDirectory("/workspace/internal", source.Directory("internal")).
		WithEnvVariable("CGO_ENABLED", "0").
		WithEnvVariable("GOOS", goos).
		WithEnvVariable("GOARCH", goarch).
		WithMountedCache("/go/pkg/mod", dag.CacheVolume("go-mod-124")).
		WithEnvVariable("GOMODCACHE", "/go/pkg/mod").
		WithMountedCache("/go/build-cache", dag.CacheVolume("go-build-124")).
		WithEnvVariable("GOCACHE", "/go/build-cache").
		WithExec([]string{"go", "mod", "download"}).
		WithExec([]string{"go", "build", "-o", "manager", "cmd/main.go"})

	return builder.File("/workspace/manager")
}

// Publish docker container
func (m *ValkeyClusterOperator) PublishDocker(
	ctx context.Context,
	// +defaultPath="/"
	source *dagger.Directory,
) (string, error) {
	// container registry for the multi-platform image
	tag, err := getNewImageTag()
	if err != nil {
		return "", err
	}
	imageRepo := "quay.io/kurtmcalpine/valkey-cluster-operator:" + tag

	platformVariants, err := m.Build(ctx, source)
	if err != nil {
		return "", err
	}

	// publish to registry
	_, err = dag.Container().
		Publish(ctx, imageRepo, dagger.ContainerPublishOpts{
			PlatformVariants: platformVariants,
		})

	if err != nil {
		return "", err
	}

	// return build directory
	return tag, nil
}

// Build the application container
func (m *ValkeyClusterOperator) Build(
	ctx context.Context,
	// +defaultPath="/"
	source *dagger.Directory,
) ([]*dagger.Container, error) {

	var platforms = []dagger.Platform{
		"linux/amd64", // a.k.a. x86_64
		"linux/arm64", // a.k.a. aarch64
	}

	platformVariants := make([]*dagger.Container, 0, len(platforms))
	for _, platform := range platforms {
		manager := m.BuildManager(ctx, source, platform)
		ctr := source.WithFile("manager", manager).DockerBuild(dagger.DirectoryDockerBuildOpts{Platform: platform})
		platformVariants = append(platformVariants, ctr)
	}

	return platformVariants, nil
}

func getNewImageTag() (string, error) {
	type TagList struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}
	resp, err := http.Get("https://quay.io/v2/kurtmcalpine/valkey-cluster-operator/tags/list")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	t := TagList{}
	err = json.Unmarshal(b, &t)
	if err != nil {
		return "", err
	}

	newest, err := semver.Make("0.0.0")
	if err != nil {
		return "", err
	}
	for _, tag := range t.Tags {
		if !strings.HasPrefix(tag, "v") {
			continue
		}
		v, err := semver.Make(strings.TrimPrefix(tag, "v"))
		if err != nil {
			continue
		}

		if v.GT(newest) {
			newest = v
		}
	}
	newest.IncrementPatch()
	return "v" + newest.String(), nil
}
