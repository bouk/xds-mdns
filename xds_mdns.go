package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"sort"
	"strings"

	"bou.ke/xds-mdns/dnssdbrowser"
	"go.uber.org/zap"

	"github.com/cespare/xxhash"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	router "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	stream "github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
	xds "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"github.com/envoyproxy/go-control-plane/pkg/wellknown"
)

var (
	address string
)

func init() {
	flag.StringVar(&address, "address", ":18000", "Listen address")
}

func (cb *callbacks) Report() {
}

func (cb *callbacks) OnStreamOpen(ctx context.Context, id int64, typ string) error {
	return nil
}

func (cb *callbacks) OnStreamClosed(id int64, node *core.Node) {}

func (cb *callbacks) OnStreamRequest(id int64, r *discovery.DiscoveryRequest) error {
	cb.requests++
	return nil
}

func (cb *callbacks) OnStreamResponse(ctx context.Context, id int64, req *discovery.DiscoveryRequest, resp *discovery.DiscoveryResponse) {
	cb.Report()
}

func (cb *callbacks) OnFetchRequest(ctx context.Context, req *discovery.DiscoveryRequest) error {
	cb.fetches++
	return nil
}

func (cb *callbacks) OnFetchResponse(req *discovery.DiscoveryRequest, resp *discovery.DiscoveryResponse) {
}
func (cb *callbacks) OnDeltaStreamClosed(id int64, node *core.Node) {}
func (cb *callbacks) OnDeltaStreamOpen(ctx context.Context, id int64, typ string) error {
	return nil
}

func (c *callbacks) OnStreamDeltaRequest(i int64, request *discovery.DeltaDiscoveryRequest) error {
	return nil
}

func (c *callbacks) OnStreamDeltaResponse(i int64, request *discovery.DeltaDiscoveryRequest, response *discovery.DeltaDiscoveryResponse) {
}

type callbacks struct {
	fetches  int
	requests int
}

const grpcMaxConcurrentStreams = 1000

// runServer starts an xDS server at the given port.
func runServer(ctx context.Context, server xds.Server) error {
	var grpcOptions []grpc.ServerOption
	grpcOptions = append(grpcOptions, grpc.MaxConcurrentStreams(grpcMaxConcurrentStreams))
	grpcServer := grpc.NewServer(grpcOptions...)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	// register services
	discovery.RegisterAggregatedDiscoveryServiceServer(grpcServer, server)

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()
	return grpcServer.Serve(lis)
}

type cach struct {
	browser *dnssdbrowser.Browser
	log     *zap.Logger
}

func (*cach) CreateDeltaWatch(req *discovery.DeltaDiscoveryRequest, state stream.StreamState, resp chan cache.DeltaResponse) (cancel func()) {
	// TODO
	return nil
}

func servicesToClusterLoadAssignment(name string, services []dnssdbrowser.Service) *endpoint.ClusterLoadAssignment {
	endpoints := make([]*endpoint.LbEndpoint, 0, len(services))
	for _, svc := range services {
		for _, address := range svc.Addresses {
			endpoints = append(endpoints, &endpoint.LbEndpoint{
				HostIdentifier: &endpoint.LbEndpoint_Endpoint{
					Endpoint: &endpoint.Endpoint{
						Address: &core.Address{
							Address: &core.Address_SocketAddress{
								SocketAddress: &core.SocketAddress{
									Address:  address.String(),
									Protocol: core.SocketAddress_TCP,
									PortSpecifier: &core.SocketAddress_PortValue{
										PortValue: uint32(svc.Port),
									},
								},
							},
						},
						Hostname: strings.TrimSuffix(svc.Hostname, "."),
					},
				},
				HealthStatus: core.HealthStatus_HEALTHY,
			})
		}
	}

	return &endpoint.ClusterLoadAssignment{
		ClusterName: name,
		Endpoints: []*endpoint.LocalityLbEndpoints{{
			Locality: &core.Locality{
				Region: "local",
			},
			LbEndpoints:         endpoints,
			LoadBalancingWeight: &wrapperspb.UInt32Value{Value: 1000},
		}},
	}
}

func resourcesHash(resources []types.Resource) uint64 {
	hasher := xxhash.New()
	var buf []byte

	for _, resource := range resources {
		buf, _ = proto.MarshalOptions{
			Deterministic: true,
		}.MarshalAppend(buf, resource)
		hasher.Write(buf)
		hasher.Write([]byte{0})
		buf = buf[:0]
	}

	return hasher.Sum64()
}

func generateResponse(req *discovery.DiscoveryRequest, resources []types.Resource) *cache.RawResponse {
	sort.Slice(resources, func(i, j int) bool {
		a := cache.GetResourceName(resources[i])
		b := cache.GetResourceName(resources[j])
		return a < b
	})

	hash := resourcesHash(resources)
	version := fmt.Sprintf("%x", hash)

	resourcesWithTTL := make([]types.ResourceWithTTL, 0, len(resources))
	for _, resource := range resources {
		resourcesWithTTL = append(resourcesWithTTL, types.ResourceWithTTL{Resource: resource, TTL: nil})
	}
	return &cache.RawResponse{
		Request:   req,
		Version:   version,
		Resources: resourcesWithTTL,
		Heartbeat: false,
		Ctx:       context.TODO(),
	}
}

func (c *cach) CreateWatch(req *discovery.DiscoveryRequest, state stream.StreamState, resp chan cache.Response) (cancel func()) {
	c.log.Debug("CreateWatch", zap.Any("req", req))

	ctx := context.Background()

	switch req.TypeUrl {
	case resource.ListenerType:
		var resources []types.ResourceWithTTL
		for _, name := range req.ResourceNames {
			hcRds := &hcm.HttpConnectionManager_Rds{
				Rds: &hcm.Rds{
					RouteConfigName: name,
					ConfigSource: &core.ConfigSource{
						ResourceApiVersion: core.ApiVersion_V3,
						ConfigSourceSpecifier: &core.ConfigSource_Ads{
							Ads: &core.AggregatedConfigSource{},
						},
					},
				},
			}

			hff := &router.Router{}
			tctx, err := anypb.New(hff)
			if err != nil {
				continue
			}

			manager := &hcm.HttpConnectionManager{
				CodecType:      hcm.HttpConnectionManager_AUTO,
				RouteSpecifier: hcRds,
				HttpFilters: []*hcm.HttpFilter{{
					Name: wellknown.Router,
					ConfigType: &hcm.HttpFilter_TypedConfig{
						TypedConfig: tctx,
					},
				}},
			}

			pbst, err := anypb.New(manager)
			if err != nil {
				continue
			}

			l := &listener.Listener{
				Name: name,
				ApiListener: &listener.ApiListener{
					ApiListener: pbst,
				},
			}
			resources = append(resources, types.ResourceWithTTL{Resource: l})
		}

		if req.VersionInfo == "" {
			resp <- &cache.RawResponse{
				Request:   req,
				Version:   "1",
				Resources: resources,
				Heartbeat: false,
				Ctx:       context.TODO(),
			}
		}
		return func() {}
	case resource.RouteType:
		var resources []types.ResourceWithTTL
		for _, routeName := range req.ResourceNames {
			route := &route.RouteConfiguration{
				Name:             routeName,
				ValidateClusters: &wrapperspb.BoolValue{Value: true},
				VirtualHosts: []*route.VirtualHost{{
					Name:    routeName,
					Domains: []string{routeName},
					Routes: []*route.Route{{
						Match: &route.RouteMatch{
							PathSpecifier: &route.RouteMatch_Prefix{
								Prefix: "",
							},
						},
						Action: &route.Route_Route{
							Route: &route.RouteAction{
								ClusterSpecifier: &route.RouteAction_Cluster{
									Cluster: routeName,
								},
							},
						},
					},
					},
				}},
			}
			resources = append(resources, types.ResourceWithTTL{Resource: route})
		}
		if req.VersionInfo == "" {
			resp <- &cache.RawResponse{
				Request:   req,
				Version:   "1",
				Resources: resources,
				Heartbeat: false,
				Ctx:       context.TODO(),
			}
		}
		return func() {}
	case resource.ClusterType:
		var resources []types.ResourceWithTTL
		for _, clusterName := range req.ResourceNames {
			cluster := &cluster.Cluster{
				Name:                 clusterName,
				LbPolicy:             cluster.Cluster_ROUND_ROBIN,
				ClusterDiscoveryType: &cluster.Cluster_Type{Type: cluster.Cluster_EDS},
				EdsClusterConfig: &cluster.Cluster_EdsClusterConfig{
					EdsConfig: &core.ConfigSource{
						ConfigSourceSpecifier: &core.ConfigSource_Ads{},
					},
				},
			}
			resources = append(resources, types.ResourceWithTTL{Resource: cluster})
		}
		if req.VersionInfo == "" {
			resp <- &cache.RawResponse{
				Request:   req,
				Version:   "1",
				Resources: resources,
				Heartbeat: false,
				Ctx:       context.TODO(),
			}
		}
		return func() {}
	case resource.EndpointType:
		services := req.ResourceNames
		var resources []types.Resource

		updates := make(chan *endpoint.ClusterLoadAssignment)
		current := make(map[string]*endpoint.ClusterLoadAssignment)
		ctx, cancel := context.WithCancel(ctx)
		g, ctx := errgroup.WithContext(ctx)
		for _, svc := range services {
			svc := svc
			mdnsService := "_" + svc + "._tcp.local."
			subscription := c.browser.FullSubscribe(mdnsService)
			instances := <-subscription.Stream
			current[svc] = servicesToClusterLoadAssignment(svc, instances)
			g.Go(func() error {
				defer subscription.Close()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case instances := <-subscription.Stream:
						updates <- servicesToClusterLoadAssignment(svc, instances)
					}
				}
			})
		}

		for _, cluster := range current {
			resources = append(resources, cluster)
		}

		response := generateResponse(req, resources)
		if req.VersionInfo != response.Version {
			resp <- response
		}

		go func() {
			for update := range updates {
				current[update.ClusterName] = update
				var resources []types.Resource
				for _, cluster := range current {
					resources = append(resources, cluster)
				}
				resp <- generateResponse(req, resources)
			}
		}()

		return func() {
			cancel()
			g.Wait()
			close(updates)
		}
	}
	return nil
}

func (*cach) Fetch(ctx context.Context, req *discovery.DiscoveryRequest) (cache.Response, error) {
	return nil, fmt.Errorf("unimplemented")
}

func main() {
	flag.Parse()

	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	log, err := config.Build()
	if err != nil {
		panic(err)
	}

	browser := dnssdbrowser.NewBrowser().SetLogger(log)
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() {
		for {
			socket, err := net.ListenMulticastUDP("udp", nil, &net.UDPAddr{IP: net.IPv4(224, 0, 0, 251), Port: 5353})
			if err != nil {
				cancel(err)
				log.Error("Failed to listen", zap.Error(err))
				return
			}

			browser.Listen(socket)
		}
	}()

	log.Info("Starting xDS-mDNS server", zap.String("address", address))

	cb := &callbacks{}
	cache := &cach{
		browser: browser,
		log:     log,
	}
	srv := xds.NewServer(ctx, cache, cb)

	if err := runServer(ctx, srv); err != nil {
		log.Fatal("Failed to start server", zap.Error(err))
	}
}
