# Simply Kaspa Indexer
A high performance Kaspa PostgreSQL indexer implemented in Rust.  

## About
The indexer has been implemented from scratch by deriving the functional spec of [kaspa-db-filler](https://github.com/lAmeR1/kaspa-db-filler).  
As part of this process the database schema was reworked to better support concurrency.  
This means that databases populated by the lAmeR1/kaspa-db-filler must be migrated to be compatible.  
A schema migration script has been developed and is available [here](https://github.com/supertypo/kaspa-db-filler-migration).  
A compatible version of the kaspa-rest-server is available [here](https://github.com/kaspa-ng/kaspa-rest-server).

## Important notes

### Optional tables
To maximize the performance for your specific needs you should take care to disable any optional tables you don't need through command line flags.  
Currently --skip_resolving_addresses and --skip_block_relations are supported, more will be added in the future.

### Optional fields
In addition to optional tables, many fields can be left empty if they are not required for your use case.  
Use include_fields or exclude_fields arguments to fine tune. See --help for a list of optional fields.  
**Be aware that the required fields can change at any time, so take special care when upgrading the indexer!**

### Postgres tuning
Make sure to tune Postgres to your specific hardware, here is an example for a server with 12GB RAM and SSD storage:
```
shared_buffers = 2GB
work_mem = 128MB
effective_io_concurrency = 32
checkpoint_timeout = 5min
max_wal_size = 4GB
min_wal_size = 80MB
effective_cache_size = 8GB
```
In addition, I highly recommend running Postgres on ZFS with compression=lz4 for space savings as well as for improving performance. Make sure to also set recordsize=8k.

### Tn11 (10bps) note
The indexer is able to keep up with the 10bps testnet (TN11) under full load (2000+tps) as long as Postgres is running on a sufficiently high-end NVMe.  
You can consider disabling optional tables and fields to bring the requirements down if running on lesser hardware.

### Historical data
The indexer will begin collecting data from the point in time when it's started.  
If you have an archival node, you can specify the start-block using the --ignore_checkpoint argument and specify an older start block.  
Please make contact with us on the [Kaspa Discord](https://kaspa.org) if you need a pg_dump-file of historical records.

# License
ISC. See LICENSE.

### Donations
kaspa:qrjtsnnpjyvlmkffdqyayrny3qyen9yjkpuw7xvhsz36n69wmrfdyf3nwv67t

# Getting started

## Run using precompiled Docker image
Please consult the [Docker Hub page](https://hub.docker.com/r/supertypo/simply-kaspa-indexer).

## Build and run from source
These instructions are for Ubuntu 24.04, adjustments might be needed for other distributions (or versions). 

### 1. Install dependencies
```shell
sudo apt update && sudo apt install -y git curl build-essential pkg-config libssl-dev
```

### 2. Install Rust
```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
```

### 3. Update path
```shell
source ~/.bashrc
```

### 4. Clone this repository
```shell
git clone <repository-url>
```

### 5. Build workspace
```shell
cargo build
```

### 6. Run indexer
```shell
cargo run -- -s ws://<kaspad_host>:17110 -d postgres://postgres:postgres@<postgres_host>:5432
```
