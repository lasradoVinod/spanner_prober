// Package prober defines a Cloud Spanner prober.
package prober

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"

	cppb "github.com/cloudprober/cloudprober/probes/external/proto"

	"github.com/cloudprober/cloudprober/probes/external/serverutils"
	log "github.com/golang/glog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"google.golang.org/protobuf/proto"

	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"

	"google.golang.org/grpc"

	"go.opencensus.io/trace"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"

	"cloud.google.com/go/spanner"
	dbadminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
)

const (
	aggregationChannelSize = 100
	// Long running operations retry parameters.
	baseLRORetryDelay = 200 * time.Millisecond
	maxLRORetryDelay  = 5 * time.Second
)

var (
	generatePayload = func(size int) ([]byte, []byte, error) {
		payload := make([]byte, size)
		rand.Read(payload)
		h := sha256.New()
		if _, err := h.Write(payload); err != nil {
			return nil, nil, err
		}
		return payload, h.Sum(nil), nil
	}
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// Options holds the settings required for creating a prober.
type ProberOptions struct {

	// Cloud Spanner settings in name form, not URI (e.g. an_instance, not projects/a_project/instances/an_instance).
	Project        string
	Instance       string
	Database       string
	InstanceConfig string

	// QPS rate to probe at.
	QPS float64

	// QPS per instance config to probe at. The QPS will be evenly distributed across all non-witness regions.
	QPSPerInstanceConfig float64

	// NumRows is the number of rows in which which the prober randomly chooses to probe.
	NumRows int

	// Prober is type of probe which will be run.
	Prober Probe

	// MaxStaleness is the bound of stale reads.
	MaxStaleness time.Duration

	// PayloadSize is the number of bytes of random data used as a Payload.
	PayloadSize int

	// GCE zone where the prober will be running, borg cell equivalent
	GCEZone string

	// ProbeDeadline is the deadline for request for probes.
	ProbeDeadline time.Duration

	// Cloud Spanner Endpoint to send request.
	Endpoint string

	// Node count for Spanner instance. If specified, ProcessingUnits must be 0.
	NodeCount int

	// Processing units for the Spanner instance. If specified, NodeCount must be 0.
	ProcessingUnits int
}

// Prober holds the internal prober state.
type Prober struct {
	instanceAdminClient *instance.InstanceAdminClient
	spannerClient       *spanner.Client
	deadline            time.Duration
	mu                  *sync.Mutex
	opsChannel          chan op
	ops                 []op
	numRows             int
	qps                 float64
	prober              Probe
	maxStaleness        time.Duration
	payloadSize         int
	generatePayload     func(int) ([]byte, []byte, error)
	opt                 ProberOptions
	allowedPeerRanges   []string
	deniedPeerRanges    []string
	// add clientOptions here as it contains credentials.
	clientOpts []option.ClientOption
}

type op struct {
	Latency time.Duration
	Error   error
}

// Probe is the interface for a single type of prober.
type Probe interface {
	name() string
	probe(context.Context, *Prober) error
}

// instanceURI returns an instance URI of the form: projects/{project}/instances/{instance name}.
// E.g. projects/test-project/instances/test-instance.
func (opt *ProberOptions) instanceURI() string {
	return fmt.Sprintf("projects/%s/instances/%s", opt.Project, opt.Instance)
}

// instanceName returns an instance name of the form: {instance name}.
// E.g. test-instance.
func (opt *ProberOptions) instanceName() string {
	return opt.Instance
}

// gceZone returns the zone where prober will run
// E.g. us-central1-b
func (opt *ProberOptions) gceZone() string {
	return opt.GCEZone
}

// instanceConfigURI returns an instance config URI of the form: projects/{project}/instanceConfigs/{instance config name}.
// E.g. projects/test-project/instanceConfigss/regional-test.
func (opt *ProberOptions) instanceConfigURI() string {
	return fmt.Sprintf("projects/%s/instanceConfigs/%s", opt.Project, opt.InstanceConfig)
}

// projectURI returns a project URI of the form: projects/{project}.
// E.g. projects/test-project.
func (opt *ProberOptions) projectURI() string {
	return fmt.Sprintf("projects/%s", opt.Project)
}

// databaseURI returns a database URI of the form: projects/{project}/instances/{instance name}/databases/{database name}.
// E.g. projects/test-project/instances/test-instance/databases/test-database.
func (opt *ProberOptions) databaseURI() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", opt.Project, opt.Instance, opt.Database)
}

// databaseName returns a database name of the form: {database name}.
// E.g. test-database.
func (opt *ProberOptions) databaseName() string {
	return opt.Database
}

// New initializes Cloud Spanner clients, setup up the database, and return a new CSProber.
func NewProber(ctx context.Context, opt ProberOptions, clientOpts ...option.ClientOption) (*Prober, error) {
	// Override Cloud Spanner endpoint if specified.
	if opt.Endpoint != "" {
		clientOpts = append(clientOpts, option.WithEndpoint(opt.Endpoint))
	}
	// TODO(b/168615976): remove this when we can disable DirectPath using GOOGLE_CLOUD_DISABLE_DIRECT_PATH env var
	clientOpts = append(clientOpts, internaloption.EnableDirectPath(false))
	p, err := newCloudProber(ctx, opt, clientOpts...)
	if err != nil {
		return nil, err
	}

	// This goroutine is run indefinitely to match the lifetime of the prober framework.
	// It will also return when p.opsChannel is closed, which the user cannot do.
	go p.backgroundStatsAggregator()
	return p, nil
}

func newCloudProber(ctx context.Context, opt ProberOptions, clientOpts ...option.ClientOption) (*Prober, error) {
	if opt.NumRows <= 0 {
		return nil, fmt.Errorf("NumRows must be at least 1, got %v", opt.NumRows)
	}

	if opt.NodeCount > 0 && opt.ProcessingUnits > 0 {
		return nil, fmt.Errorf("At most one of NodeCount or ProcessingUnits may be specified. NodeCount: %v, ProcessingUnits: %v", opt.NodeCount, opt.ProcessingUnits)
	}

	if opt.Prober == nil {
		return nil, errors.New("Prober must not be nil")
	}

	instanceClient, err := instance.NewInstanceAdminClient(ctx, clientOpts...)
	if err != nil {
		return nil, err
	}

	instanceConfig, err := instanceClient.GetInstanceConfig(ctx, &instancepb.GetInstanceConfigRequest{
		Name: opt.instanceConfigURI(),
	})

	if err != nil {
		return nil, err
	}

	isProbeTarget := checkCandidateForProbe(opt.gceZone(), instanceConfig.GetReplicas())

	proberQPS := 0.0
	if !isProbeTarget {
		// add overrides
		opt.QPS = 0
		opt.QPSPerInstanceConfig = 0
		// Add minimal qps. We will exclude this in metric.
		proberQPS = 0.00001
		opt.Database = "non-relevant"
	} else {
		proberQPS = qpsPerProber(opt.QPS, opt.QPSPerInstanceConfig, instanceConfig.GetReplicas())
	}

	if err := createCloudSpannerInstanceIfMissing(ctx, instanceClient, opt); err != nil {
		return nil, err
	}

	databaseClient, err := database.NewDatabaseAdminClient(ctx, clientOpts...)
	if err != nil {
		return nil, err
	}
	defer databaseClient.Close()
	if err := createCloudSpannerDatabase(ctx, databaseClient, opt); err != nil {
		return nil, err
	}

	clientOpts = append(clientOpts, option.WithGRPCDialOption(grpc.WithUnaryInterceptor(AddUnaryPeerInterceptor())),
		option.WithGRPCDialOption(grpc.WithStreamInterceptor(AddStreamPeerInterceptor())))
	// TODO(b/180957755) Surface t4t7 latency as a metric
	clientOpts = append(clientOpts, option.WithGRPCDialOption(grpc.WithUnaryInterceptor(AddGFELatencyUnaryInterceptor)),
		option.WithGRPCDialOption(grpc.WithStreamInterceptor(AddGFELatencyStreamingInterceptor)))
	dataClient, err := spanner.NewClient(ctx, opt.databaseURI(), clientOpts...)
	if err != nil {
		return nil, err
	}

	p := &Prober{
		instanceAdminClient: instanceClient,
		spannerClient:       dataClient,
		deadline:            time.Duration(opt.ProbeDeadline),
		opsChannel:          make(chan op, aggregationChannelSize),
		numRows:             opt.NumRows,
		qps:                 proberQPS,
		prober:              opt.Prober,
		maxStaleness:        opt.MaxStaleness,
		payloadSize:         opt.PayloadSize,
		generatePayload:     generatePayload,
		mu:                  &sync.Mutex{},
		opt:                 opt,
		clientOpts:          clientOpts,
	}

	//p.deniedPeerRanges = directPathPeerRanges

	return p, nil
}

// checkCandidateForProbe checks whether the gceZone is candidate for probing for that instance config
func checkCandidateForProbe(gceZone string, instanceConfigReplicas []*instancepb.ReplicaInfo) bool {
	// prober colocation with replica and replica not in witness
	for _, replicaInfo := range instanceConfigReplicas {
		if replicaInfo.GetType() != instancepb.ReplicaInfo_WITNESS && strings.HasPrefix(gceZone, replicaInfo.GetLocation()) {
			return true
		}
	}
	return false

}

// qpsPerProber calculates the qps that each individual prober should be probing at.
func qpsPerProber(qps float64, qpsPerInstanceConfig float64, instanceConfigReplicas []*instancepb.ReplicaInfo) float64 {
	// for non existent probe
	if qps == 0 && qpsPerInstanceConfig == 0 {
		return 0
	}
	if qps > 0 {
		return qps
	}
	regions := make(map[string]struct{})
	for _, replicaInfo := range instanceConfigReplicas {
		if replicaInfo.GetType() != instancepb.ReplicaInfo_WITNESS {
			regions[replicaInfo.GetLocation()] = struct{}{}
		}
	}
	return qpsPerInstanceConfig / float64(len(regions))
}

func backoff(baseDelay, maxDelay time.Duration, retries int) time.Duration {
	backoff, max := float64(baseDelay), float64(maxDelay)
	for backoff < max && retries > 0 {
		backoff = backoff * 1.5
		retries--
	}
	if backoff > max {
		backoff = max
	}

	// Randomize backoff delays so that if a cluster of requests start at
	// the same time, they won't operate in lockstep.  We just subtract up
	// to 40% so that we obey maxDelay.
	backoff -= backoff * 0.4 * rng.Float64()
	if time.Duration(backoff) < baseDelay {
		return baseDelay
	}
	return time.Duration(backoff)
}

// createCloudSpannerInstanceIfMissing creates a one node "Instance" of Cloud Spanner in the specificed project if missing.
// Instances are shared across multiple prober tasks running in differenct GCE VMs.
// Instances do not get cleaned up in this prober, as we do not support turning down Cloud Spanner regions.
func createCloudSpannerInstanceIfMissing(ctx context.Context, instanceClient *instance.InstanceAdminClient, opt ProberOptions) error {
	// skip instance creation requests if the instance already exists
	if checkInstancePresence(ctx, instanceClient, opt) {
		return nil
	}
	ctx, span := trace.StartSpan(ctx, "CreateInstance")
	span.AddAttributes(trace.StringAttribute("Instance", opt.instanceName()))
	span.AddAttributes(trace.StringAttribute("InstanceConfig", opt.instanceConfigURI()))
	defer span.End()

	op, err := instanceClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     opt.projectURI(),
		InstanceId: opt.instanceName(),
		Instance: &instancepb.Instance{
			Name:            opt.instanceURI(),
			Config:          opt.instanceConfigURI(),
			DisplayName:     opt.instanceName(),
			NodeCount:       int32(opt.NodeCount),
			ProcessingUnits: int32(opt.ProcessingUnits),
		},
	})

	// If instance create operations fails, check if the instance was already created. If no, return error
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return err
		}
	} else if _, err := op.Wait(ctx); err != nil {
		return err
	}

	// Wait for instance to be ready
	var retries int
	for {
		var resp *instancepb.Instance
		resp, err = instanceClient.GetInstance(ctx, &instancepb.GetInstanceRequest{
			Name: opt.instanceURI(),
		})
		if err != nil || resp.State == instancepb.Instance_READY {
			return err
		}
		select {
		case <-time.After(backoff(baseLRORetryDelay, maxLRORetryDelay, retries)):
		case <-ctx.Done():
			return ctx.Err()
		}
		retries++
	}
}

// checkInstancePresence checks whether the instance is already created.
func checkInstancePresence(ctx context.Context, instanceClient *instance.InstanceAdminClient, opt ProberOptions) bool {
	ctx, span := trace.StartSpan(ctx, "cloudprober:CheckInstancePresence")
	span.AddAttributes(trace.StringAttribute("Instance", opt.instanceName()))
	span.AddAttributes(trace.StringAttribute("InstanceConfig", opt.instanceConfigURI()))
	defer span.End()

	resp, err := instanceClient.GetInstance(ctx, &instancepb.GetInstanceRequest{
		Name: opt.instanceURI(),
	})
	// if instance is not present or instance is not ready
	if err != nil || resp.State != instancepb.Instance_READY {
		return false
	}
	return true
}

// deleteCloudSpannerInstance deletes the instance present in ProberOptions.
func deleteCloudSpannerInstance(ctx context.Context, instanceClient *instance.InstanceAdminClient, opt ProberOptions) error {
	ctx, span := trace.StartSpan(ctx, "DeleteInstance")
	span.AddAttributes(trace.StringAttribute("Instance", opt.instanceName()))
	span.AddAttributes(trace.StringAttribute("InstanceConfig", opt.instanceConfigURI()))
	defer span.End()

	err := instanceClient.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
		Name: opt.instanceURI(),
	})

	return err
}

// createCloudSpannerDatabase creates a prober "Database" on an "Instance" of Cloud Spanner in the specificed project.
// Databases are shared across multiple prober tasks running in differenct GCE VMs.
// Databases do not get cleaned up in this prober, as we do not support turning down Cloud Spanner regions.
func createCloudSpannerDatabase(ctx context.Context, databaseClient *database.DatabaseAdminClient, opt ProberOptions) error {
	ctx, span := trace.StartSpan(ctx, "CreateDatabase")
	span.AddAttributes(trace.StringAttribute("Database", opt.databaseName()))
	span.AddAttributes(trace.StringAttribute("Instance", opt.instanceName()))
	defer span.End()

	op, err := databaseClient.CreateDatabase(ctx, &dbadminpb.CreateDatabaseRequest{
		Parent:          opt.instanceURI(),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%v`", opt.databaseName()),
		ExtraStatements: []string{
			`CREATE TABLE ProbeTarget (
					Id INT64 NOT NULL,
					Payload BYTES(MAX),
					PayloadHash BYTES(MAX),
				) PRIMARY KEY (Id)`,
		},
	})

	if err != nil {
		if code := status.Code(err); code == codes.AlreadyExists {
			return nil
		}
		return err
	}

	_, err = op.Wait(ctx)
	return err
}

// dropCloudSpannerDatabase deops the database mentioned in ProberOptions.
func dropCloudSpannerDatabase(ctx context.Context, databaseClient *database.DatabaseAdminClient, opt ProberOptions) error {
	ctx, span := trace.StartSpan(ctx, "DropDatabase")
	span.AddAttributes(trace.StringAttribute("Database", opt.databaseName()))
	span.AddAttributes(trace.StringAttribute("Instance", opt.instanceName()))
	defer span.End()

	err := databaseClient.DropDatabase(ctx, &dbadminpb.DropDatabaseRequest{Database: opt.databaseURI()})
	return err
}

// backgroundStatsAggregator pulls stats from the prober channel, and places them in a slice.
// This avoids the case in which probes are blocked due to the channel being full.
func (p *Prober) backgroundStatsAggregator() {
	for op := range p.opsChannel {
		p.mu.Lock()
		p.ops = append(p.ops, op)
		p.mu.Unlock()
	}
}

func (p *Prober) probeInterval() time.Duration {
	// qps must be > 0 as we validate this when constructing a CSProber
	// qps is converted to a duration for type reasons, however it does not represent a value duration.
	// Use granularity of nanosecond to support float division. Useful for supporting probes with QPS < 1.
	// convert the interval to nanosecond
	probeIntervalNanoSeconds := int(1e9 / p.qps)
	// convert to time.Duration
	return time.Duration(probeIntervalNanoSeconds) * time.Nanosecond
}

// Start starts the prober. This will run a goroutinue until ctx is canceled.
func (p *Prober) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(p.probeInterval())

		for {
			select {
			case <-ctx.Done():
				// Stop probing when the context is canceled.
				log.Info("Probing stopped as context is done.")
				return
			case <-ticker.C:
				go func() {
					probeCtx, cancel := context.WithTimeout(ctx, p.deadline)
					defer cancel()
					p.runProbe(probeCtx)
				}()
			}
		}
	}()
}

func (p *Prober) runProbe(ctx context.Context) {
	peer := &peer.Peer{}
	ctx = context.WithValue(ctx, PeerKey{}, peer)
	startTime := time.Now()
	ctx, span := trace.StartSpan(ctx, reflect.TypeOf(p.prober).Name())
	defer span.End()
	// TODO(b/176558334): Add additional probes (and corresponding abstractions).
	rpcErr := p.prober.probe(ctx, p)
	latency := time.Now().Sub(startTime)

	if rpcErr != nil {
		span.SetStatus(trace.Status{Code: trace.StatusCodeInternal, Message: rpcErr.Error()})
	} else {
		span.SetStatus(trace.Status{Code: trace.StatusCodeOK})
	}

	p.opsChannel <- op{
		Latency: latency,
		Error:   rpcErr,
	}
}

// probeStats return a cloudprober playload with the latencies, error count, and op count since the last call.
func (p *Prober) probeStats() string {
	p.mu.Lock()
	ops := p.ops
	p.ops = nil
	p.mu.Unlock()

	var latencyStrs []string
	errorCount := 0
	opCount := len(ops)
	for _, op := range ops {
		millis := float64(op.Latency.Nanoseconds()) / float64(time.Millisecond.Nanoseconds())
		latencyStrs = append(latencyStrs, fmt.Sprintf("%.3f", millis))

		if op.Error != nil {
			errorCount++
		}
	}

	var payload strings.Builder
	fmt.Fprintf(&payload, "latencies %s\n", strings.Join(latencyStrs, ","))
	fmt.Fprintf(&payload, "op_count %d\n", opCount)
	fmt.Fprintf(&payload, "error_count %d\n", errorCount)
	return payload.String()
}

// Serve runs serverutils.Server to expose the results of probes via Cloud Prober.
func (p *Prober) Serve() {
	serverutils.Serve(func(req *cppb.ProbeRequest, reply *cppb.ProbeReply) {
		reply.Payload = proto.String(p.probeStats())
	})
}

// validateRows checks that the read did not return an error, and if the row is
// present it validates the hash. If the row is not present it completes successfully.
// Returns the number of validated rows, and the first error encounted.
func validateRows(iter *spanner.RowIterator) (int, error) {
	rows := 0
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return rows, nil
		}
		if err != nil {
			return rows, err
		}

		var id int64
		var payload, payloadHash []byte
		if err := row.Columns(&id, &payload, &payloadHash); err != nil {
			return rows, err
		}

		h := sha256.New()
		_, err = h.Write(payload)
		if err != nil {
			return rows, err
		}
		if calculatedHash := h.Sum(nil); !bytes.Equal(calculatedHash, payloadHash) {
			return rows, fmt.Errorf("hash for row %v did not match, got %v, want %v", id, calculatedHash, payloadHash)
		}

		rows++
	}
}

// NoopProbe is a fake prober which always succeeds.
type NoopProbe struct{}

func (NoopProbe) name() string { return "noop" }

func (NoopProbe) probe(ctx context.Context, p *Prober) error {
	return nil
}

// StaleReadProbe performs a stale read of the database.
type StaleReadProbe struct{}

func (StaleReadProbe) name() string { return "stale_read" }

func (StaleReadProbe) probe(ctx context.Context, p *Prober) error {
	// Random row within the range.
	k := rand.Intn(p.numRows)

	txn := p.spannerClient.Single().WithTimestampBound(spanner.MaxStaleness(p.maxStaleness))
	iter := txn.Read(ctx, "ProbeTarget", spanner.Key{k}, []string{"Id", "Payload", "PayloadHash"})
	defer iter.Stop()
	_, err := validateRows(iter)
	return err
}

// StrongQueryProbe performs a strong query of the database.
type StrongQueryProbe struct{}

func (StrongQueryProbe) name() string { return "strong_query" }

func (StrongQueryProbe) probe(ctx context.Context, p *Prober) error {
	// Random row within the range.
	k := rand.Intn(p.numRows)

	stmt := spanner.Statement{
		SQL: `select t.Id, t.Payload, t.PayloadHash from ProbeTarget t where t.Id = @Id`,
		Params: map[string]interface{}{
			"Id": k,
		},
	}
	iter := p.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()
	_, err := validateRows(iter)
	return err
}

// StaleQueryProbe performs a stale query of the database.
type StaleQueryProbe struct{}

func (StaleQueryProbe) name() string { return "stale_query" }

func (StaleQueryProbe) probe(ctx context.Context, p *Prober) error {
	// Random row within the range.
	k := rand.Intn(p.numRows)

	stmt := spanner.Statement{
		SQL: `select t.Id, t.Payload, t.PayloadHash from ProbeTarget t where t.Id = @Id`,
		Params: map[string]interface{}{
			"Id": k,
		},
	}
	iter := p.spannerClient.Single().WithTimestampBound(spanner.MaxStaleness(p.maxStaleness)).Query(ctx, stmt)
	defer iter.Stop()
	_, err := validateRows(iter)
	return err
}

// DMLProbe performs a SQL based transaction in the database.
type DMLProbe struct{}

func (DMLProbe) name() string { return "dml" }

func (DMLProbe) probe(ctx context.Context, p *Prober) error {
	// Random row within the range.
	k := rand.Intn(p.numRows)

	payload, payloadHash, err := p.generatePayload(p.payloadSize)
	if err != nil {
		return err
	}

	_, err = p.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		readStmt := spanner.Statement{
			SQL: `select t.Id, t.Payload, t.PayloadHash from ProbeTarget t where t.Id = @Id`,
			Params: map[string]interface{}{
				"Id": k,
			},
		}
		iter := txn.Query(ctx, readStmt)
		defer iter.Stop()
		rows, err := validateRows(iter)
		if err != nil {
			return err
		}

		// Update the row with a new random value if the row already exisis, otherwise insert a new row.
		dmlSQL := `update ProbeTarget t set t.Payload = @payload, t.PayloadHash = @payloadHash where t.Id = @Id`
		if rows == 0 {
			dmlSQL = `insert ProbeTarget (Id, Payload, PayloadHash) VALUES(@Id, @payload, @payloadHash)`
		}
		dmlStmt := spanner.Statement{
			SQL: dmlSQL,
			Params: map[string]interface{}{
				"Id":          k,
				"payload":     payload,
				"payloadHash": payloadHash,
			},
		}
		_, err = txn.Update(ctx, dmlStmt)
		return err
	})

	return err
}

// ReadWriteProbe performs a mutation based transaction in the database.
type ReadWriteProbe struct{}

func (ReadWriteProbe) name() string { return "read_write" }

func (ReadWriteProbe) probe(ctx context.Context, p *Prober) error {
	// Random row within the range.
	k := rand.Intn(p.numRows)

	payload, payloadHash, err := p.generatePayload(p.payloadSize)
	if err != nil {
		return err
	}

	_, err = p.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		iter := txn.Read(ctx, "ProbeTarget", spanner.Key{k}, []string{"Id", "Payload", "PayloadHash"})
		defer iter.Stop()
		_, err := validateRows(iter)
		if err != nil {
			return err
		}

		return txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertOrUpdate("ProbeTarget", []string{"Id", "Payload", "PayloadHash"}, []interface{}{k, payload, payloadHash}),
		})
	})

	return err
}
