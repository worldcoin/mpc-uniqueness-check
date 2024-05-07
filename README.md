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

### Participant


### Running Locally

To run a local version of the MPC uniqueness check with two participants, you can execute the following command.
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
