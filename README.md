# MPC Uniqueness Check

The MPC Uniqueness Check is a secure multi-party computation (SMPC) protocol that powers the matching engine behind Worldcoin's [iris recognition inference system](https://worldcoin.org/blog/engineering/iris-recognition-inference-system). SMPC is used to calculate a fractional hamming distance to determine uniqueness of a given iris code. A detailed specification of the iris code SMPC can be found [here](docs/specification.ipynb).



```mermaid
sequenceDiagram
    autonumber
    participant RequestQueue as Uniqueness Check Queue
    participant Coord as Coordinator
    participant Participants as Participants
    participant ResultsQueue as Results Queue

    RequestQueue ->> Coord: New uniquness check request
    Coord ->> Participants: Send code to all participants
    Coord ->> Coord: Calculate mask portion of FHD
    Participants ->> Participants: Calculate iris code portion of FHD
    Participants ->> Coord: Send partial results
    Coord ->> Coord: Calculate final result
    Coord ->> ResultsQueue: Enqueue final result
```

The MPC setup consists of a coordinator and `n` participants, where each participant stores an encrypted portion of each iris code and the coordinator stores the corresponding masks. When a new uniqueness check request is enqueued, the coordinator will send the code to each of the participant, which will compute a fractional hamming distance against the encrypted partial iris codes. The participant results will be sent back to the coordinator, which will then be combined to arrive at the final result.



## Installing
To install mpc uniqueness check and all utilities, you can run the following command. 
```
cargo install --path .
```

## Usage

### Coordinator

To start the coordinator, you can run `mpc-node --config <path_to_config>` or specify the following configuration via environment variables.

```toml
[coordinator]
# Socket addresses for each participant
participants = '["127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"]'
# Hamming distance threshold to determine if a given iris code is unique 
hamming_distance_threshold = 0.375
# Max duration allotted when connecting to participants
participant_connection_timeout = "1s"

# Database where the masks will be stored
[coordinator.db]
url = "postgres://localhost:5432/mpc"
migrate = true
create = false

# AWS configuration for the coordinator
[coordinator.aws]
endpoint = ""
region = ""

[coordinator.queues]
# Uniqueness check requests queue
queries_queue_url = "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-query-queue"
# Uniqueness check results queue
distances_queue_url = "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-distance-results-queue"
# Queue specifying masks to add to the database
db_sync_queue_url = "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-query-queue"
```

### Participant


```toml
[participant]
# Socket address for the participant
socket_addr = "127.0.0.1:8000"
# Batch size when calculating fractional hamming distance
batch_size = 20000

# Database configuration for storing data related to the participant
[participant.db]
url = "postgres://localhost:5432/participant_db"
migrate = true
create = false

# AWS configuration for the participant
[participant.aws]
endpoint = ""
region = ""

[participant.queues]
# Queue specifying shares to add to the database
db_sync_queue_url = "https://sqs.us-east-1.amazonaws.com/1234567890/participant-db-sync-queue"
```


### Running Locally

To run a local version of the MPC uniqueness check with two participants, you can execute the following command. Note that you do not need to specify the configuration above as this is already included in the compose file. 
```
docker compose up -d
```

Once all of the services are running, you can seed the coordinator and participant databases with random iris codes/masks.
```
utils seed-db -c postgres://postgres:postgres@127.0.0.1:5432/db -p postgres://postgres:postgres@127.0.0.1:5433/db --num 10000 --batch-size 1000
```

After seeding the databases, you can enqueue a random template to the coordinator queue which will orchestrate the MPC uniqueness check.

```
utils sqs-query -e http://localhost:4566 -q http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/coordinator-uniqueness-check
```
