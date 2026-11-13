#!/bin/sh
set -eu

create_queue() {
  awslocal sqs create-queue --queue-name "$1" \
    --attributes FifoQueue=true,ContentBasedDeduplication=false >/dev/null
}

create_queue wager-transactions-dlq.fifo
create_queue wager-transactions.fifo
create_queue wager-events-dlq.fifo
create_queue wager-events.fifo

configure_redrive() {
  source_url=$(awslocal sqs get-queue-url --queue-name "$1" --query QueueUrl --output text)
  dead_url=$(awslocal sqs get-queue-url --queue-name "$2" --query QueueUrl --output text)
  dead_arn=$(awslocal sqs get-queue-attributes --queue-url "$dead_url" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text)
  printf '{"RedrivePolicy":"{\"deadLetterTargetArn\":\"%s\",\"maxReceiveCount\":\"5\"}"}' \
    "$dead_arn" > /tmp/sqs-redrive.json
  awslocal sqs set-queue-attributes --queue-url "$source_url" \
    --attributes file:///tmp/sqs-redrive.json
}

configure_redrive wager-transactions.fifo wager-transactions-dlq.fifo
configure_redrive wager-events.fifo wager-events-dlq.fifo
