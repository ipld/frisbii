# go-frisbii

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

## Library usage

See https://pkg.go.dev/github.com/ipld/go-frisbii for full documentation.

`NewFrisbiiServer()` can be used to create a new server given a `LinkSystem` as a source of IPLD data.

## License

Apache-2.0/MIT © Protocol Labs