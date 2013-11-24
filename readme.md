# redisync
A Redis slave which persists to disk without loading the data set into memory.

## state
Development

## why
A problem with offloading persistence to a slave is that it requires the entire data set to be held in memory. This results in persistence being memory-bound. You cannot, for example, have one or few machines dedicated to persisting a cluster of Redis masters without having sufficient memory.

## how
On startup, a `sync` command is sent to the configured master. The master first replied with a full dump (`.rdb`), which redisync saves in a standalone file. The master continues to send a replication stream, which redisync saves in an append only file (`.aof`).

To limit the size of the append only file, a new worker is start as a configurable interval (the old worker doesn't stop until the new worker is properly established).

## restoration
Restoration is accomplished in two steps.

First, the desired `.rdb` file is loaded into the new master server (the same way you restore any redis rdb file).

Next, the corresponding `.aof` file is piped into the master:

    cat 20131124_135146.aof | redis-cli --pipe

## configuration
Configuration is done via the `config.json` file (or, optionally by specifying a path via the `-config=PATH` parameter).

* **ttl** the time, in seconds, that a worker will live for
* **network** tcp/unix
* **address** the address:port for the master
* **storage** the path to store files

