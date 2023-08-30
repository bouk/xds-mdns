package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"sync"
	"time"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	ep "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	lv2 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	router "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	xds "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"github.com/envoyproxy/go-control-plane/pkg/wellknown"
	"github.com/hashicorp/mdns"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	debug       bool
	port        uint
	gatewayPort uint
	mode        string
	version     int32
	serviceName string
)

func init() {
	flag.BoolVar(&debug, "debug", true, "Use debug logging")
	flag.UintVar(&port, "port", 18000, "Management server port")
	flag.UintVar(&gatewayPort, "gateway", 18001, "Management server port for HTTP gateway")
	flag.StringVar(&serviceName, "service", "", "Service name to listen to")
}

func (cb *callbacks) Report() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	log.WithFields(log.Fields{"fetches": cb.fetches, "requests": cb.requests}).Info("Report")
}

func (cb *callbacks) OnStreamOpen(ctx context.Context, id int64, typ string) error {
	return nil
}

func (cb *callbacks) OnStreamClosed(id int64, node *corev3.Node) {}

func (cb *callbacks) OnStreamRequest(id int64, r *discovery.DiscoveryRequest) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.requests++
	if cb.signal != nil {
		close(cb.signal)
		cb.signal = nil
	}
	return nil
}

func (cb *callbacks) OnStreamResponse(ctx context.Context, id int64, req *discovery.DiscoveryRequest, resp *discovery.DiscoveryResponse) {
	cb.Report()
}

func (cb *callbacks) OnFetchRequest(ctx context.Context, req *discovery.DiscoveryRequest) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.fetches++
	if cb.signal != nil {
		close(cb.signal)
		cb.signal = nil
	}
	return nil
}

func (cb *callbacks) OnFetchResponse(req *discovery.DiscoveryRequest, resp *discovery.DiscoveryResponse) {
}
func (cb *callbacks) OnDeltaStreamClosed(id int64, node *corev3.Node) {}
func (cb *callbacks) OnDeltaStreamOpen(ctx context.Context, id int64, typ string) error {
	return nil
}

func (c *callbacks) OnStreamDeltaRequest(i int64, request *discovery.DeltaDiscoveryRequest) error {
	return nil
}

func (c *callbacks) OnStreamDeltaResponse(i int64, request *discovery.DeltaDiscoveryRequest, response *discovery.DeltaDiscoveryResponse) {
}

type callbacks struct {
	signal   chan struct{}
	fetches  int
	requests int
	mu       sync.Mutex
}

const grpcMaxConcurrentStreams = 1000

// RunManagementServer starts an xDS server at the given port.
func RunManagementServer(ctx context.Context, server xds.Server, port uint) {
	var grpcOptions []grpc.ServerOption
	grpcOptions = append(grpcOptions, grpc.MaxConcurrentStreams(grpcMaxConcurrentStreams))
	grpcServer := grpc.NewServer(grpcOptions...)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.WithError(err).Fatal("failed to listen")
	}

	// register services
	discovery.RegisterAggregatedDiscoveryServiceServer(grpcServer, server)

	log.WithFields(log.Fields{"port": port}).Info("management server listening")
	go func() {
		if err = grpcServer.Serve(lis); err != nil {
			log.Error(err)
		}
	}()
	<-ctx.Done()

	grpcServer.GracefulStop()
}

type OneHash struct{}

func (OneHash) ID(node *core.Node) string {
	return ""
}

func main() {
	flag.Parse()
	if serviceName == "" {
		log.Fatal("service name is required")
	}

	if debug {
		log.SetLevel(log.DebugLevel)
	}

	listenerName := serviceName
	routeConfigName := serviceName + "-route"
	clusterName := serviceName + "-cluster"
	virtualHostName := serviceName + "-vs"

	ctx := context.Background()

	log.Printf("Starting control plane")

	signal := make(chan struct{})
	cb := &callbacks{
		signal:   signal,
		fetches:  0,
		requests: 0,
	}
	cache := cachev3.NewSnapshotCache(true, OneHash{}, nil)
	srv := xds.NewServer(ctx, cache, cb)

	go RunManagementServer(ctx, srv, port)

	mdnsService := "_" + serviceName + "._tcp"
	for {
		entries := make(chan *mdns.ServiceEntry, 10)

		done := make(chan bool)
		var lbendpoints []*ep.LbEndpoint
		go func() {
			for entry := range entries {
				lbendpoints = append(lbendpoints, &ep.LbEndpoint{
					HostIdentifier: &ep.LbEndpoint_Endpoint{
						Endpoint: &ep.Endpoint{
							Address: &core.Address{
								Address: &core.Address_SocketAddress{
									SocketAddress: &core.SocketAddress{
										Address:  entry.AddrV4.String(),
										Protocol: core.SocketAddress_TCP,
										PortSpecifier: &core.SocketAddress_PortValue{
											PortValue: uint32(entry.Port),
										},
									},
								},
							},
						},
					},
					HealthStatus: core.HealthStatus_HEALTHY,
				})
			}
			done <- true
		}()

		err := mdns.Query(&mdns.QueryParam{
			Service:             mdnsService,
			Domain:              "local",
			Timeout:             2 * time.Second,
			Entries:             entries,
			WantUnicastResponse: false,
			DisableIPv4:         false,
			DisableIPv6:         true,
		})
		close(entries)

		if err != nil {
			log.Errorf("could not lookup service: %v", err)
			continue
		}
		<-done

		eds := []types.Resource{
			&endpoint.ClusterLoadAssignment{
				ClusterName: clusterName,
				Endpoints: []*ep.LocalityLbEndpoints{{
					Locality: &core.Locality{
						Region: "local",
					},
					LbEndpoints:         lbendpoints,
					LoadBalancingWeight: &wrapperspb.UInt32Value{Value: 1000},
				}},
			},
		}

		cls := []types.Resource{
			&cluster.Cluster{
				Name:                 clusterName,
				LbPolicy:             cluster.Cluster_ROUND_ROBIN,
				ClusterDiscoveryType: &cluster.Cluster_Type{Type: cluster.Cluster_EDS},
				EdsClusterConfig: &cluster.Cluster_EdsClusterConfig{
					EdsConfig: &core.ConfigSource{
						ConfigSourceSpecifier: &core.ConfigSource_Ads{},
					},
				},
			},
		}

		rds := []types.Resource{
			&route.RouteConfiguration{
				Name:             routeConfigName,
				ValidateClusters: &wrapperspb.BoolValue{Value: true},
				VirtualHosts: []*route.VirtualHost{{
					Name:    virtualHostName,
					Domains: []string{listenerName},
					Routes: []*route.Route{{
						Match: &route.RouteMatch{
							PathSpecifier: &route.RouteMatch_Prefix{
								Prefix: "",
							},
						},
						Action: &route.Route_Route{
							Route: &route.RouteAction{
								ClusterSpecifier: &route.RouteAction_Cluster{
									Cluster: clusterName,
								},
							},
						},
					},
					},
				}},
			},
		}

		hcRds := &hcm.HttpConnectionManager_Rds{
			Rds: &hcm.Rds{
				RouteConfigName: routeConfigName,
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
			log.Errorf("could not unmarshall router: %v", err)
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
			log.Errorf("could not unmarshall manager: %v", err)
			continue
		}

		l := []types.Resource{
			&listener.Listener{
				Name: listenerName,
				ApiListener: &lv2.ApiListener{
					ApiListener: pbst,
				},
			},
		}

		// TODO: only increment version if we actually changed something
		version++
		resources := make(map[resource.Type][]types.Resource, 4)
		resources[resource.ClusterType] = cls
		resources[resource.ListenerType] = l
		resources[resource.RouteType] = rds
		resources[resource.EndpointType] = eds

		log.Printf("%+v", resources)

		snap, err := cachev3.NewSnapshot(fmt.Sprint(version), resources)
		if err != nil {
			log.Errorf("could not create snapshot: %v", err)
			continue
		}

		// cant get the consistent snapshot thing working anymore...
		// https://github.com/envoyproxy/go-control-plane/issues/556
		// https://github.com/envoyproxy/go-control-plane/blob/main/pkg/cache/v3/snapshot.go#L110
		// if err := snap.Consistent(); err != nil {
		// 	log.Errorf("snapshot inconsistency: %+v\n%+v", snap, err)
		// 	os.Exit(1)
		// }

		err = cache.SetSnapshot(ctx, "", snap)
		if err != nil {
			log.Errorf("could not create snapshot: %v", err)
			continue
		}

		time.Sleep(5 * time.Second)
	}
}
