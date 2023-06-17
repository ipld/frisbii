# Frisbii

**An experimental minimal IPLD data provider for the IPFS network**

**Frisbii** is currently a simple HTTP server that conforms minimally to the [IPFS Trustless Gateway](https://specs.ipfs.tech/http-gateways/trustless-gateway/) specification, serving IPLD data in CAR format to HTTP clients.

Content routing, when enabled, is delegated to the [Interpletary Network Indexer (IPNI)](https://cid.contact/) service. On startup, Frisbii can announce its content to IPNI, allowing it to be discovered by clients that query the indexer for that content.

Frisbii does not directly interact with the IPFS DHT, which limits discovery potential for clients which primarily use the DHT. However, with increasing reliance on IPNI for content discovery, this will change over time. In particular, **[Lassie](https://github.com/filecoin-project/lassie)**, a universal IPFS and Filecoin retrieval client, relies on IPNI for content discovery.

## CLI Usage

Basic usage:

```
frisbii --announce=roots --car=/path/to/file.car
```

Full argument list:

* `--car` - path to one or more CAR files to serve, this can be a plain path, a glob path to match multiple files, and `--car` can be supplied multiple times.
* `--announce` - announce the given roots to IPNI on startup. Can be `roots` or `none`. Defaults to `none`.
* `--listen` - hostname and port to listen on. Defaults to `:3747`.
* `--public-addr` - multiaddr or URL of this server as seen by the indexer and other peers if it is different to the listen address. Defaults address of the server once started (typically the value of `--listen`).
* `--log-file` - path to file to append HTTP request and error logs to. Defaults to `stdout`.
* `--max-response-duration` - maximum duration to spend responding to a request. Defaults to `5m`.
* `--max-response-bytes` - maximum size of a response from IPNI. Defaults to `100MiB`.
* `--help` - show help.

### CAR files

* [go-car](https://github.com/ipld/go-car) can be used to author, manipulate and inspect CAR files.
* [Kubo](https://github.com/ipfs/kubo)'s `dag export` command can be used to export a CAR file from an IPFS node, although this is limited to complete DAGs.
* [Lassie](https://github.com/filecoin-project/lassie) can be used to export a CAR file from the network.

Both CARv1 and CARv2 formats are usable by Frisbii. However, on startup, Frisbii will need to generate an index in memory for a CARv1. So for faster start-up times it is recommended that you start Frisbii with CARv2 files (using go-car this can be done with `car index input.car > output.car`).

Using `--anounce=roots` will announce the roots of all CARs loaded by Frisbii to the indexer. Other blocks are not announced, and will not be discoverable by clients that query the indexer for that content, however they are served by Frisbii when requested directly or as part of a DAG whose root has been advertised.

## Library usage

See https://pkg.go.dev/github.com/ipld/frisbii for full documentation.

`NewFrisbiiServer()` can be used to create a new server given a `LinkSystem` as a source of IPLD data.

## Further Development

The goal of Frisbii is to be maximally minimal according to user need. It is not intended to be a full IPFS node, but rather a simple server that can be used to serve IPLD data. However, the limitations of HTTP as the only transport, the restrictions within the current minimal Trustless Gateway implementation, and reliance on IPNI for content announcement present some challenges for data provision to a wide audience.

Some areas for possible further development (perhaps by you!) include:

* Support for additional transports, including Graphsync over a minimal DataTransfer implementation to mirror the default transport functionality of Filecoin storage providers, and Bitswap for the widest possible compatibility with existing IPFS clients. Unfortunately both transports introduce considerable complexity.
* Support for P2P message sending for IPNI, so announcements can be made to multiple indexers via pubsub.
* DHT support.
* Support for alternative data storage backends.
* More advanced announcement options, including `all` CIDs, or a user-supplied list of CIDs.

## License

Apache-2.0/MIT © Protocol Labs
