#!/bin/bash

# Create your SQS queues
echo "Creating SQS queues..."
awslocal sqs create-queue --queue-name coordinator-uniqueness-check.fifo --attributes FifoQueue=true,ContentBasedDeduplication=true
awslocal sqs create-queue --queue-name coordinator-results-queue.fifo --attributes FifoQueue=true,ContentBasedDeduplication=true

awslocal sqs create-queue --queue-name coordinator-db-sync-queue
awslocal sqs create-queue --queue-name participant-db-sync-queue
