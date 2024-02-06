#!/bin/bash

# Create your SQS queues
echo "Creating SQS queues..."
awslocal sqs create-queue --queue-name coordinator-uniqueness-check
awslocal sqs create-queue --queue-name coordinator-results-queue
awslocal sqs create-queue --queue-name coordinator-db-sync-queue
