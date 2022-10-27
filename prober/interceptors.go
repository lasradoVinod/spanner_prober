// Package interceptors creates interceptors for requests to Cloud Spanner APIs.
package prober

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"

	"go.opencensus.io/trace"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"google.golang.org/grpc"
)

const gfeT4T7prefix = "gfet4t7; dur="
const serverTimingKey = "server-timing"

// parseT4T7Latency parse the headers and trailers for finding the gfet4t7 latency.
func parseT4T7Latency(headers, trailers metadata.MD) (time.Duration, error) {
	var serverTiming []string

	if len(headers[serverTimingKey]) > 0 {
		serverTiming = headers[serverTimingKey]
	} else if len(trailers[serverTimingKey]) > 0 {
		serverTiming = trailers[serverTimingKey]
	} else {
		return 0, fmt.Errorf("server-timing headers not found")
	}
	for _, entry := range serverTiming {
		if !strings.HasPrefix(entry, gfeT4T7prefix) {
			continue
		}
		durationText := strings.TrimPrefix(entry, gfeT4T7prefix)
		durationMillis, err := strconv.ParseInt(durationText, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse gfe latency: %v", err)
		}
		return time.Duration(durationMillis) * time.Millisecond, nil
	}
	return 0, fmt.Errorf("no gfe latency response available")
}

// AddGFELatencyUnaryInterceptor intercepts unary client requests (spanner.Commit, spanner.ExecuteSQL) and annotates GFE latency.
func AddGFELatencyUnaryInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx, span := trace.StartSpan(ctx, "UnaryT4T7Interceptor")
	defer span.End()

	var headers, trailers metadata.MD
	opts = append(opts, grpc.Header(&headers))
	opts = append(opts, grpc.Trailer(&trailers))
	if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
		return err
	}

	if span.SpanContext().IsSampled() {
		gfeLatency, err := parseT4T7Latency(headers, trailers)
		if err != nil {
			span.AddAttributes(trace.StringAttribute("Error", err.Error()))
		} else {
			span.AddAttributes(trace.StringAttribute("Latency", gfeLatency.String()))
		}
	}
	return nil
}

// AddGFELatencyStreamingInterceptor intercepts streaming requests StreamingSQL and annotates GFE latency.
func AddGFELatencyStreamingInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx, span := trace.StartSpan(ctx, "StreamingT4T7Interceptor")
	var headers, trailers metadata.MD
	opts = append(opts, grpc.Header(&headers))
	opts = append(opts, grpc.Trailer(&trailers))

	cs, err := streamer(ctx, desc, cc, method, opts...)

	if err != nil {
		return cs, err
	}

	if span.SpanContext().IsSampled() {
		// Since cs.Header() can block, read it in a go routine
		go func() {
			headers, headerErr := cs.Header()
			trailers := cs.Trailer()
			if headerErr != nil {
				span.Annotate(nil, "headers error: "+headerErr.Error())
				span.End()
			}
			gfeLatency, err := parseT4T7Latency(headers, trailers)
			if err != nil {
				span.AddAttributes(trace.StringAttribute("Error", err.Error()))
			} else {
				span.AddAttributes(trace.StringAttribute("Latency", gfeLatency.String()))
			}
			span.End()

		}()
	}
	return cs, nil

}

// PeerKey is context's attributes' key; must be non built-in type.
type PeerKey struct{}

// unaryPeerSetter makes the grpc connection include peer information in a context variable keyed by PeerKey{} if it exists.
func unaryPeerSetter() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		p, ok := ctx.Value(PeerKey{}).(*peer.Peer)
		if ok {
			opts = append(opts, grpc.Peer(p))
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// streamPeerSetter makes the grpc connection include peer information in a context variable keyed by PeerKey{} if it exists.
func streamPeerSetter() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
		streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		p, ok := ctx.Value(PeerKey{}).(*peer.Peer)
		if ok {
			opts = append(opts, grpc.Peer(p))
		}

		return streamer(ctx, desc, cc, method, opts...)
	}
}

// AddUnaryPeerInterceptor intercepts unary requests and add PeerKey.
func AddUnaryPeerInterceptor() grpc.UnaryClientInterceptor {
	unaryInterceptors := []grpc.UnaryClientInterceptor{}
	unaryInterceptors = append(unaryInterceptors, unaryPeerSetter())
	return grpc_middleware.ChainUnaryClient(unaryInterceptors...)
}

// AddStreamPeerInterceptor intercepts stream requests and add PeerKey.
func AddStreamPeerInterceptor() grpc.StreamClientInterceptor {
	streamInterceptors := []grpc.StreamClientInterceptor{}
	streamInterceptors = append(streamInterceptors, streamPeerSetter())
	return grpc_middleware.ChainStreamClient(streamInterceptors...)
}
