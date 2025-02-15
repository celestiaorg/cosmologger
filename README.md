# Cosmologger

This is a logger tool that collects blocks and transactions as they are happening in a cosmos based network.

## Install

The best way to install it is to do it through a bundle called [testnet-evaluator](https://github.com/celestiaorg/testnet-evaluator/). 

## Development

To have a development environment just run the following commands:

```bash
git clone git@github.com:celestiaorg/cosmologger.git
cd cosmologger
docker-compose up -d --build
```

Then enter the shell of the running dev-container:

```bash
docker exec -it cosmologger sh
```

To build and try:

```bash
clear && go build . && ./cosmologger
```

## Configuration

### ENV Variables

* **RPC_ADDRESS**: indicates the address to the RPC server
* **GRPC_ADDRESS**: indicates the address to the GRPC server
* **GRPC_TLS**: if you use TLS (ssl) security layer, this must be `true`

* **POSTGRES_DB**: The name of `postgres` database
* **POSTGRES_USER**: Database username
* **POSTGRES_PASSWORD**: Password for the database user
* **POSTGRES_PORT**: Port number of the database
* **POSTGRES_HOST**: Host address of the server running postgres

* **DATA_COLLECTION_MODE**: Determines the mode of data collection. Available values: {'pull' | 'event'}. _event mode is recommended_

**Note**: `cosmologger` creates all the database schema on its first run, so make sure the database user has enough privilege to create tables and indexes.

### Config file

There is a `config.json` file, which has to be mapped into the app directory of the container. i.e. be in the same path of the executable.

Here is what is inside the conf file:

```json
{
    "grpc":{
        "api_call_retry": 20,
        "call_timeout": 30
    },

    "tendermint_client": {
        "subscriber_name":"cosmologger",
        "connect_retry": 60
    },

    "bech32_prefix" : {
        "account" :{
            "address": "celestia",
            "pubkey": "celestiapub"
        },
        "validator" :{
            "address": "celestiavaloper",
            "pubkey": "celestiavaloperpub"
        },
        "consensus" :{
            "address": "celestiavalcons",
            "pubkey": "celestiavalconspub"
        }
    }
}
```

`grpc` keeps the GRPC configs:

* `api_call_retry`: Since API calls sometimes fails due to network load or some other reasons, `cosmologger` tries multiple times to get the results. This parameter tells how many times it can try.
* `call_timeout`: GRPC API timeout

```json
"grpc":{
        "api_call_retry": 20,
        "call_timeout": 30
    }
```

`tendermint_client` keeps the RPC configs

* `subscriber_name` is the arbitrary RPC subscriber
* `connect_retry` indicates how many times `cosmologger` should try to connect to the RPC server if it is not available. This feature comes handy when the RPC server is not ready yet. `cosmologger` waits a second before every trial.

```json
"tendermint_client": {
        "subscriber_name":"cosmologger",
        "connect_retry": 60
    }
```

`bech32_prefix` Keeps the prefixes of the addresses and keys, it has to be configured according to the network configs that is being used.

```json
"bech32_prefix" : {
        "account" :{
            "address": "celestia",
            "pubkey": "celestiapub"
        },
        "validator" :{
            "address": "celestiavaloper",
            "pubkey": "celestiavaloperpub"
        },
        "consensus" :{
            "address": "celestiavalcons",
            "pubkey": "celestiavalconspub"
        }
}
```
