# Building and Testing DAG Pirate Ship
Basic workflow for local tests with different builds.

## Build

To build code, run `make` commands:
- `make` Builds the default release version of PirateShip with the logger app
- `make clean` - Cleans up log files, good to run between tests
- `make pirate_ship_logger` - same as default
- `make dag_pirate_ship_logger` - build the PirateShip logger with `dag` feature enabled
- `make signed_raft_logger`
- `make dag_signed_raft_logger`
- `make signed_raft_kvs`
- `make dag_signed_raft_kvs`

After a successful build, you should have `server` and `client` binaries in `target/release`.

## Run

*Make sure you have downloaded and unpacked the `.zip` of config files into a `configs` dir in the repo source.*

To run the local 4-node, 1-client config:
1. Open 5 different terminals.
2. In the first 4 terminals, run `./target/release/server configs/node1_config.json`, replacing 1 with 2,3,4 so we have 4 different nodes.
3. In the 5th terminal, run `./target/release/client configs/client1_config.json`. The client will send requests for 60s, wait for responses, and then exit.
4. When you are done, kill the 4 server nodes.

The default logging level is `info`, if you want to get debug or trace messages, preface the server/client command with `LOG_LEVEL=debug|trace`, e.g. `LOG_LEVEL=debug ./target/release/server configs/node1_config.json`. 

If you ever want to sanity check what's running, check the first line of the server logs:
```
[INFO][server][2025-11-19T20:16:06.081048-08:00] Protocol: signed_raft, App: app_logger, DAG: false
```