import json
import os
import boto3
from typing import Dict, Any

ecs_client = boto3.client("ecs")

CLUSTER_NAME = os.getenv("ECS_CLUSTER_NAME", "production-data-pipeline")
TASK_DEF = os.getenv("ECS_TASK_DEFINITION", "pyspark-batch-job:latest")
SUBNETS = os.getenv("SUBNET_IDS", "").split(",")
SECURITY_GROUPS = os.getenv("SECURITY_GROUP_IDS", "").split(",")

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Consumes batch events from SQS FIFO and triggers an ECS Task 
    running the PySpark transformation process.
    """
    records = event.get("Records", [])
    if not records:
        return {"statusCode": 200, "body": json.dumps("No records to process.")}

    s3_paths = []
    for record in records:
        body = json.loads(record.get("body", "{}"))
        if "s3_path" in body:
            s3_paths.append(body["s3_path"])

    if not s3_paths:
        return {"statusCode": 400, "body": json.dumps("No valid S3 paths found in batch.")}

    response = ecs_client.run_task(
        cluster=CLUSTER_NAME,
        taskDefinition=TASK_DEF,
        launchType="FARGATE",
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": SUBNETS,
                "securityGroups": SECURITY_GROUPS,
                "assignPublicIp": "DISABLED"
            }
        },
        overrides={
            "containerOverrides": [
                {
                    "name": "pyspark-transformer",
                    "environment": [
                        {"name": "BATCH_S3_PATHS", "value": ",".join(s3_paths)}
                    ]
                }
            ]
        }
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"status": "ECS Task Triggered", "taskArn": response["tasks"][0]["taskArn"]})
    }