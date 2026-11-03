// redis-operator runs Redis and Valkey on Kubernetes from Redis
// resources: one primary and its replicas, failover, switchover before a
// node drains, and the Services and Secrets that clients connect
// through. plans/00-design.md is the design.
//
// The one binary has three commands. With no command it is the
// operator. "manager" is the instance manager that runs in front of the
// server in every pod, and "install DIR" copies the binary into the
// pod's volume, from the init container.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// version is the release this binary was built from. The Dockerfile
// sets it with -ldflags "-X main.version=...". A build without the
// flag, such as go test, reports dev.
var version = "dev"

func main() {
	// The kubelet stops a pod with SIGTERM, and the context ends on it.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, log io.Writer) error {
	command := ""
	if len(args) > 0 {
		command = args[0]
	}
	switch command {
	case "", "operator":
		return runOperator(ctx, getenv, log)
	case "manager":
		return runManager(ctx, getenv, log)
	case "install":
		if len(args) != 2 {
			return errors.New("usage: redis-operator install DIR")
		}
		return installManager(args[1])
	case "version":
		_, err := fmt.Fprintf(log, "redis-operator %s\n", version)
		return err
	}
	return fmt.Errorf("unknown command %q: the commands are operator, manager, install, and version", command)
}

func runOperator(ctx context.Context, getenv func(string) string, log io.Writer) error {
	namespace, podName := getenv("POD_NAMESPACE"), getenv("POD_NAME")
	if namespace == "" || podName == "" {
		return errors.New("POD_NAMESPACE and POD_NAME must be set; the operator reads its own pod to learn its image")
	}
	metricsPort := 9200
	if port := getenv("METRICS_PORT"); port != "" {
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return fmt.Errorf("METRICS_PORT is %q; it must be a port number from 1 to 65535", port)
		}
		metricsPort = number
	}
	stages := defaultTimings
	for name, field := range map[string]*time.Duration{"CANARY_SOAK": &stages.canarySoak, "CANARY_TIMEOUT": &stages.canaryTimeout} {
		if text := getenv(name); text != "" {
			d, err := time.ParseDuration(text)
			if err != nil || d <= 0 {
				return fmt.Errorf("%s is %q; it must be a duration such as 90s", name, text)
			}
			*field = d
		}
	}
	client, err := inClusterKubeClient()
	if err != nil {
		return err
	}
	image, err := ownImage(ctx, client, namespace, podName)
	if err != nil {
		return err
	}
	readings := newMetrics(version)
	address, err := readings.serve(ctx, fmt.Sprintf(":%d", metricsPort))
	if err != nil {
		return fmt.Errorf("serving metrics: %w", err)
	}
	fmt.Fprintf(log, "redis-operator %s started, metrics on %s, instance managers from %s\n", version, address, image)
	return newElection(client, namespace, candidateIdentity(os.Hostname), readings, log).serve(ctx, func(ctx context.Context) error {
		c := newController(client, readings, image, time.Now, log)
		c.timings = stages
		return c.run(ctx)
	})
}

// ownImage reads the image of this pod's operator container, which the
// init container of every Redis pod runs to install the instance
// manager. The two always match, so a pod never runs a manager from
// another release than the operator that made it.
func ownImage(ctx context.Context, client *kubeClient, namespace, name string) (string, error) {
	var pod struct {
		Spec struct {
			Containers []struct {
				Name  string `json:"name"`
				Image string `json:"image"`
			} `json:"containers"`
		} `json:"spec"`
	}
	if err := client.get(ctx, podsResource, namespace, name, &pod); err != nil {
		return "", fmt.Errorf("reading this pod, %s/%s: %w", namespace, name, err)
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == "operator" {
			return container.Image, nil
		}
	}
	return "", fmt.Errorf("pod %s/%s has no container named operator", namespace, name)
}

func runManager(ctx context.Context, getenv func(string) string, log io.Writer) error {
	config := managerConfig{
		namespace:       getenv("POD_NAMESPACE"),
		redisName:       getenv("REDIS_NAME"),
		podName:         getenv("POD_NAME"),
		podIP:           getenv("POD_IP"),
		password:        getenv("REDIS_PASSWORD"),
		tlsSecret:       getenv("TLS_SECRET"),
		tlsDir:          filepath.Join("/manager", "tls"),
		tlsPort:         tlsPort,
		port:            serverPort,
		statusPort:      statusPort,
		loopback:        true,
		dataDir:         "/data",
		configPath:      filepath.Join("/manager", "server.conf"),
		backupDir:       backupMount,
		restoreDir:      restoreMount,
		leaseDuration:   primaryLeaseDuration,
		renewDeadline:   primaryRenewDeadline,
		retryPeriod:     primaryRetryPeriod,
		stepPeriod:      managerStepPeriod,
		failoverTimeout: switchoverTimeout,
		handoverWait:    handoverWait,
		demotionHold:    demotionHold,
		routeSettle:     routeSettle,
		pauseGap:        pauseGap,
		fullSyncDelay:   fullSyncDelay,
	}
	for name, value := range map[string]string{
		"POD_NAMESPACE": config.namespace, "REDIS_NAME": config.redisName, "POD_NAME": config.podName,
		"POD_IP": config.podIP, "REDIS_PASSWORD": config.password,
	} {
		if value == "" {
			return fmt.Errorf("%s is unset; the operator's pod template sets it", name)
		}
	}
	client, err := inClusterKubeClient()
	if err != nil {
		return err
	}
	return newManager(config, client, log).run(ctx)
}
