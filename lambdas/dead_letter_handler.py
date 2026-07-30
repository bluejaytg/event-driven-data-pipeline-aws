import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Captures malformed payloads routed to the Dead Letter Queue (DLQ)
    for compliance logging and audit inspection.
    """
    failed_records = event.get("Records", [])
    for record in failed_records:
        message_id = record.get("messageId")
        body = record.get("body")
        attributes = record.get("attributes", {})
        
        logger.error(
            json.dumps({
                "event": "DLQ_RECORD_CAPTURED",
                "message_id": message_id,
                "receive_count": attributes.get("ApproximateReceiveCount"),
                "raw_payload": body
            })
        )

    return {"statusCode": 200, "body": json.dumps(f"Processed {len(failed_records)} DLQ records.")}