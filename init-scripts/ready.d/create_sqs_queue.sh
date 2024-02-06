#!/bin/bash

# Create your SQS queues
echo "Creating SQS queues..."
awslocal sqs create-queue --queue-name coordinator-inbound
awslocal sqs create-queue --queue-name coordinator-outbound
