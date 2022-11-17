package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"flag"
	proberlib "spanner_prober/prober"

	"cloud.google.com/go/spanner"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"google.golang.org/grpc/grpclog"

	log "github.com/golang/glog"
)

var (
	enableStackDriverIntegration = flag.Bool("enable_stackdriver_integration", true, "Export traces and metrics to Stackdriver")
	project                      = flag.String("project", "", "Target project")
	instance_name                = flag.String("instance", "test1", "Target instance")
	database_name                = flag.String("database", "test1", "Target database")
	instanceConfig               = flag.String("instance_config", "regional-us-central1", "Target instance config")
	nodeCount                    = flag.Int("node_count", 1, "Node count for the prober. If specified, processing_units must be 0.")
	processingUnits              = flag.Int("processing_units", 0, "Processing units for the prober. If specified, node_count must be 0.")
	qps                          = flag.Float64("qps", 1, "QPS to probe per prober. If specified, qps_per_instance_config must be 0.")
	qpsPerInstanceConfig         = flag.Float64("qps_per_instance_config", 0, "Total QPS to probe per instance config, which will be evenly distributed to all non-withness regions. If specified, qps must be 0.")
	numRows                      = flag.Int("num_rows", 1000, "Number of rows in database to be probed")
	probeType                    = flag.String("probe_type", "noop", "The probe type this prober will run")
	maxStaleness                 = flag.Duration("max_staleness", 15*time.Second, "Maximum staleness for stale queries")
	payloadSize                  = flag.Int("payload_size", 1024, "Size of payload to write to the probe database")
	probeDeadline                = flag.Duration("probe_deadline", 60*time.Second, "Deadline for probe request")
	endpoint                     = flag.String("endpoint", "", "Cloud Spanner Endpoint to send request to")
)

func main() {
	flag.Parse()
	ctx := context.Background()

	errs := validateFlags()
	if len(errs) > 0 {
		log.Errorf("Flag validation failed with %v errors", len(errs))
		for _, err := range errs {
			log.Errorf("%v", err)
		}
		log.Exit("Flag validation failed... exiting.")
	}

	prober, err := parseProbeType(*probeType)
	if err != nil {
		log.Exitf("Could not create prober due to %v.", err)
	}

	opts := proberlib.ProberOptions{
		Project:              *project,
		Instance:             *instance_name,
		Database:             *database_name,
		InstanceConfig:       *instanceConfig,
		QPS:                  *qps,
		QPSPerInstanceConfig: *qpsPerInstanceConfig,
		NumRows:              *numRows,
		Prober:               prober,
		MaxStaleness:         *maxStaleness,
		PayloadSize:          *payloadSize,
		ProbeDeadline:        *probeDeadline,
		Endpoint:             *endpoint,
		NodeCount:            *nodeCount,
		ProcessingUnits:      *processingUnits,
	}

	grpclog.SetLoggerV2(grpclog.NewLoggerV2(ioutil.Discard, /* Discard logs at INFO level */
		os.Stderr, os.Stderr))

	// Enable all default views for Cloud Spanner
	if err := spanner.EnableStatViews(); err != nil {
		log.Errorf("Failed to export stats view: %v", err)
	}

	p, err := proberlib.NewProber(ctx, opts)
	if err != nil {
		log.Exitf("Failed to initialize the cloud prober, %v", err)
	}
	p.Start(ctx)

	if *enableStackDriverIntegration {
		// Set up the stackdriver exporter for sending metrics, views and traces

		// Register gRPC views.
		if err := view.Register(ocgrpc.DefaultClientViews...); err != nil {
			log.Fatalf("Failed to register ocgrpc client views: %v", err)
		}

		getPrefix := func(name string) string {
			if strings.HasPrefix(name, proberlib.MetricPrefix) {
				return ""
			}
			return proberlib.MetricPrefix
		}
		sd, err := stackdriver.NewExporter(stackdriver.Options{
			ProjectID:            *project,
			BundleDelayThreshold: 60 * time.Second,
			BundleCountThreshold: 3000,
			GetMetricPrefix:      getPrefix,
			// GetMetricDisplayName: func(view *view.View) string {
			// 	return getPrefix(view.Name) + view.Name
			// },

			// MonitoredResource:    &MonitoredResource{delegate: monitoredresource.Autodetect()},
		})

		if err != nil {
			log.Fatalf("Failed to create the StackDriver exporter: %v", err)
		}
		defer sd.Flush()
		sd.StartMetricsExporter()
		defer sd.StopMetricsExporter()
	}

	cancelChan := make(chan os.Signal, 1)
	signal.Notify(cancelChan, syscall.SIGTERM, syscall.SIGINT)

	select {
	case <-cancelChan:
	}
}

// type MonitoredResource struct {
// 	monitoredresource.Interface

// 	delegate monitoredresource.Interface
// }

// func (mr *MonitoredResource) MonitoredResource() (resType string, labels map[string]string) {
// 	dType, dLabels := mr.delegate.MonitoredResource()
// 	resType = dType
// 	labels = make(map[string]string)
// 	for k, v := range dLabels {
// 		if k == "project_id" {
// 			labels[k] = *project
// 			continue
// 		}
// 		labels[k] = v
// 	}
// 	return
// }

func parseProbeType(t string) (proberlib.Probe, error) {
	switch t {
	case "noop":
		return proberlib.NoopProbe{}, nil
	case "stale_read":
		return proberlib.StaleReadProbe{}, nil
	case "strong_query":
		return proberlib.StrongQueryProbe{}, nil
	case "stale_query":
		return proberlib.StaleQueryProbe{}, nil
	case "dml":
		return proberlib.DMLProbe{}, nil
	case "read_write":
		return proberlib.ReadWriteProbe{}, nil
	default:
		return proberlib.NoopProbe{}, fmt.Errorf("probe_type was not a valid probe type, was %q", *probeType)
	}
}

func validateFlags() []error {
	var errs []error

	projectRegex, err := regexp.Compile(`^[-_:.a-zA-Z0-9]*$`)
	if err != nil {
		return []error{err}
	}
	instanceDBRegex, err := regexp.Compile(`^[-_.a-zA-Z0-9]*$`)
	if err != nil {
		return []error{err}
	}

	if *qps <= 0 && *qpsPerInstanceConfig <= 0 {
		errs = append(errs, fmt.Errorf("At least one of qps or qps_per_instance_config must be greater than 0, got qps: %v, qps_per_instance_config: %v", *qps, *qpsPerInstanceConfig))
	}

	if *qps > 0 && *qpsPerInstanceConfig > 0 {
		errs = append(errs, fmt.Errorf("At most one of qps or qps_per_instance_config may be specified, got qps: %v, qps_per_instance_config: %v", *qps, *qpsPerInstanceConfig))
	}

	// We limit qps to < 1000 to ensure we don't overload Spanner accidentally.
	if *qps > 1000 {
		errs = append(errs, fmt.Errorf("qps must be 1 < qps < 1000, was %v", *qps))
	}
	if *qpsPerInstanceConfig > 1000 {
		errs = append(errs, fmt.Errorf("qps_per_instance_config must be 1 < qps_per_instance_config < 1000, was %v", *qpsPerInstanceConfig))
	}

	if *numRows <= 0 {
		errs = append(errs, fmt.Errorf("num_rows must be > 0, was %v", *numRows))
	}

	if *payloadSize <= 0 {
		errs = append(errs, fmt.Errorf("payload_size must be > 0, was %v", *payloadSize))
	}

	if matched := projectRegex.MatchString(*project); !matched {
		errs = append(errs, fmt.Errorf("project did not match %v, was %v", projectRegex, *project))
	}

	if matched := instanceDBRegex.MatchString(*instance_name); !matched {
		errs = append(errs, fmt.Errorf("instance did not match %v, was %v", instanceDBRegex, *instance_name))
	}

	if matched := instanceDBRegex.MatchString(*database_name); !matched {
		errs = append(errs, fmt.Errorf("database did not match %v, was %v", instanceDBRegex, *database_name))
	}

	if matched := instanceDBRegex.MatchString(*instanceConfig); !matched {
		errs = append(errs, fmt.Errorf("instance_config did not match %v, was %v", instanceDBRegex, *instanceConfig))
	}

	if _, err := parseProbeType(*probeType); err != nil {
		errs = append(errs, err)
	}

	return errs
}
