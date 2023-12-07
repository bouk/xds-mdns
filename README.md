# gRPC xDS loadbalancing using mDNS

This project uses mDNS to make LAN services available to gRPC clients.

## Usage

1. `xds-mdns --address :18000`
2. `GRPC_XDS_BOOTSTRAP=xds_bootstrap.json grpc-client -plaintext xds:///service-name`
