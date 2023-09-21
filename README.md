# pBFT-rust

This repository contains a simple implementation of [Practical Byzantine Fault Tolerance Algorithm](https://pmg.csail.mit.edu/papers/osdi99.pdf) in Rust.

The project is an in-memory key-value store backed by pBFT.

While the implementation aims to be fairly complete (with view change protocol, message forwarding, etc.) this is a learning/fun project, it is not intended nor prepared for production usage. Not all features are implemented, all data lives in memory, and there are likely a lot of edge cases that are not handled.

## Running locally

Running multiple replicas locally can easily be done with [Overmind](https://github.com/DarthSim/overmind) (see [the Procfile](./kv-node/Procfile)). To do that use:
```
cd kv-node
overmind start
```

This will start 5 replicas with built-in dev configuration. The default ports will be `[10000-10004]`.

You can also simply start replicas separately one by one in different sessions:
```
PBFT_DEV=0 cargo r --bin kv-node
PBFT_DEV=1 cargo r --bin kv-node
...
```
`PBFT_DEV` environment variable instructs the program to use built-in configuration for easy development and testing, the value denotes replica id and the correct values are [0-4].

Customization is possible with config files or environment variables.

## Usage

After starting nodes, you can access the service with `curl`.

Set key:
```bash
curl localhost:10000/api/v1/kv -H "Content-Type: application/json" -d '{"key":"test_key", "value":"test_value"}'
```

Get key with consensus based operation:
```bash
curl "localhost:10000/api/v1/kv?key=test_key"
```

Get key from local node state:
```bash
curl "localhost:10000/api/v1/kv/local?key=test_key"
```

For testing/debugging purposes some properties of the node state can be inspected with:
```bash
curl localhost:10000/api/v1/pbft/state
```

Requests can be send to any replica, and will be automatically forwarded to the leader.

### Logging

Use `RUST_LOG` environment variable to customize logging verbosity:
```bash
export RUST_LOG=kv_node=debug,pbft_core=debug
```

### Triggering View Change

In order to trigger the view change stop the leader replica (`replica0` when starting from scratch):
```bash
overmind stop replica0
```
> **NOTE**: Make sure the replica actually stops (the graceful shutdown may wait for open connections to close). If you are not sure, simply execute the stop command twice, which will issue a `SIGKILL`.

And send the request to any other node:
```bash
curl "localhost:10001/api/v1/kv?key=test_key"
```
The request will fail, but the view change protocol should kick in in few seconds.

Repeating the request couple of seconds later, should yield correct result.

You can query the state endpoint to ensure the view has changed, and `replica1` is the leader now:
```bash
curl "localhost:10001/api/v1/pbft/state"
```
```json
{"replica_state":{"Leader":{"sequence":4}},"view":2,"last_applied":4,"last_stable_checkpoint_sequence":null}
```

> **NOTE**: There are likely quite a bit of bugs / not handled edge cases.
