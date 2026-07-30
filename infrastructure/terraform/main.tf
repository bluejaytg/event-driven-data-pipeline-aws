provider "aws" {
  region = "us-east-1"
}

resource "aws_sqs_queue" "dlq" {
  name                      = "clinical-events-dlq.fifo"
  fifo_queue                = true
  content_based_deduplication = true
}

resource "aws_sqs_queue" "main_queue" {
  name                        = "clinical-events-queue.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_ecs_cluster" "pipeline_cluster" {
  name = "production-data-pipeline"
}