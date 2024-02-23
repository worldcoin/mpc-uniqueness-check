# MPC Utils


## Seed MPC Databases
Seed the coordinator and participant databases with random shares and masks. When seeding multiple participant databases, specify a `--participant-db-url` for each participant db.

```bash
cargo run --bin utils seed-mpc-db  --coordinator-db-url <COORDINATOR_DB_URL> --participant-db-url <PARTICIPANT_0_DB_URL> --participant-db-url <PARTICIPANT_1_DB_URL> --num-templates <NUM_TEMPLATES> 
```

## Seed Iris Database
Seed the iris code database with random iris code entries. Each entry includes a `signup_id`, `serial_id`, `iris_code_left`, `mask_code_left`, `iris_code_right` and `mask_code_right`.

```bash
cargo run --bin utils seed-iris-db  --iris-code-db <IRIS_CODE_DB> --num-templates <NUM_TEMPLATES> 
```


## Send a Random Query via SQS
Send a random iris code template to the MPC setup via SQS. If running with Localstack, make sure to pass `--endpoint-url <ENDPOINT_URL>` and `--region <AWS_REGION>`.

```bash
cargo run --bin utils sqs-query  --queue-url <SQS_QUEUE_URL> 
```


## Receive SQS Results
Receive messages from a queue. This is useful when inspecting the results queue after sending a query to the MPC setup. If the queue is empty, the program will continually check for new message every second. Once messages are in the queue, the program will receive all messages, print them to the terminal and then delete the message from the queue. If running with Localstack, make sure to pass `--endpoint-url <ENDPOINT_URL>` and `--region <AWS_REGION>`.

```bash
cargo run --bin utils sqs-receive  --queue-url <SQS_QUEUE_URL> 
```

## Generate Mock Templates

Generate random templates and write the resulting items to an output file.

```bash
cargo run --bin utils generate-mock-templates --num-templates <NUM_TEMPLATES> --output <OUTPUT_FILE>
```
