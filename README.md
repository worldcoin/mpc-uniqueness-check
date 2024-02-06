# MPC Service

## Running locally
You can easily setup the services and databases using

```
docker compose up -d
```

To test run a random query run the following command
```
cargo run --bin utils -- sqs-query -e http://localhost:4566 -q http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/coordinator-uniqueness-check
```
