# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
import json
import logging
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def log_structured(level, message, **kwargs):
    """Helper function for structured JSON logging"""
    log_entry = {
        "level": level,
        "message": message,
        **kwargs
    }
    getattr(logger, level)(json.dumps(log_entry))


def handler(event, context):
    request_id = context.request_id
    table = os.environ.get("TABLE_NAME")
    
    log_structured(
        "info",
        "Processing request",
        request_id=request_id,
        table_name=table,
        http_method=event.get("httpMethod"),
        source_ip=event.get("requestContext", {}).get("identity", {}).get("sourceIp"),
    )
    
    if event["body"]:
        item = json.loads(event["body"])
        log_structured("info", "Received payload", request_id=request_id, item=item)
        year = str(item["year"])
        title = str(item["title"])
        id = str(item["id"])
        dynamodb_client.put_item(
            TableName=table,
            Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
        )
        message = "Successfully inserted data!"
        log_structured("info", "Data inserted successfully", request_id=request_id, item_id=id)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
    else:
        log_structured("info", "Received request without payload", request_id=request_id)
        item_id = str(uuid.uuid4())
        dynamodb_client.put_item(
            TableName=table,
            Item={
                "year": {"N": "2012"},
                "title": {"S": "The Amazing Spider-Man 2"},
                "id": {"S": item_id},
            },
        )
        message = "Successfully inserted data!"
        log_structured("info", "Default data inserted successfully", request_id=request_id, item_id=item_id)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
