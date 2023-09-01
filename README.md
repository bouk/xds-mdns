# gRPC xDS loadbalancing using mDNS

This project uses mDNS to make LAN services available to gRPC clients.

## Usage

1. `xds-mdns --service service-name`
2. `GRPC_XDS_BOOTSTRAP=xds_bootstrap.json grpc-client -plaintext xds:///service-name`

## TODO

Make all mDNS services available instead of just the one passed-in by looking at the service name request we get.
