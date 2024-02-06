# MPC Service

## Running locally
You can easily setup the services and databases using

```
docker compose up -d
```

To seed the db with random templates, run the following:
```
cargo run --release --bin utils -- seed-db -c postgres://postgres:postgres@127.0.0.1:5432/db -p postgres://postgres:postgres@127.0.0.1:5433/db --num 10000 --batch-size 1000
```

To test run a random query run the following command
```
cargo run --bin utils -- sqs-query -e http://localhost:4566 -q http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/coordinator-uniqueness-check
```
