"""
CDC Pipeline: DynamoDB → Kinesis Data Streams → EC2 → Postgres (Warehouse)

Tables handled:
  - merchant_balance               UPSERT key: (dbId=5, merchantId)
  - merchant_balance_logs          UPSERT key: (dbId=5, DynamoId, merchantId)
  - merchant_operator_balance      UPSERT key: (dbId=5, merchantId, operatorId)
  - merchant_operator_balance_logs UPSERT key: (dbId=5, id, operatorId)

Column mapping:
  - merchant_balance.DynamoId      ← DynamoDB field "id"
  - merchant_balance_logs.DynamoId ← DynamoDB field "id"

Behaviour:
  - REMOVE events are ignored completely — no deletes propagated to Postgres
  - Records are processed and committed one at a time (Limit=1 per get_records call)

Run on EC2:
  pip install boto3 psycopg2-binary python-dotenv
  python cdc_processor.py
"""

import boto3
import psycopg2
import psycopg2.extras
import json
import base64
import logging
import logging.handlers
import os
import time
import sys
from decimal import Decimal
from datetime import datetime
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

load_dotenv()

PKT = ZoneInfo("Asia/Karachi")

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING — file only for DEBUG+INFO, console for ERROR+ only
# ═══════════════════════════════════════════════════════════════════════════════

LOG_DIR  = os.getenv("LOG_DIR", "logs")
LOG_FILE = os.path.join(LOG_DIR, "cdc_processor.log")
os.makedirs(LOG_DIR, exist_ok=True)

_fmt = logging.Formatter(
    fmt="%(asctime)s [%(levelname)-8s] %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_handler.setFormatter(_fmt)
_file_handler.setLevel(logging.INFO)

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_fmt)
_console_handler.setLevel(logging.INFO)

logging.basicConfig(level=logging.DEBUG, handlers=[_file_handler, _console_handler])
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
log = logging.getLogger("cdc")

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

DB_ID = 5

STREAMS = {
    "merchant_balance":               os.getenv("KINESIS_MERCHANT_BALANCE_STREAM",               "merchant-balance-cdc"),
    "merchant_balance_logs":          os.getenv("KINESIS_MERCHANT_BALANCE_LOGS_STREAM",           "merchant-balance-logs-cdc"),
    "merchant_operator_balance":      os.getenv("KINESIS_MERCHANT_OPERATOR_BALANCE_STREAM",       "merchant-operator-balance-cdc"),
    "merchant_operator_balance_logs": os.getenv("KINESIS_MERCHANT_OPERATOR_BALANCE_LOGS_STREAM",  "merchant-operator-balance-log-cdc"),
}

AWS_REGION        = os.getenv("AWS_REGION", "ap-southeast-1")
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", 1))

PG_DSN = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "dbname":   os.getenv("PG_DB",       "postgres"),
    "user":     os.getenv("PG_USER",     "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
}

# ═══════════════════════════════════════════════════════════════════════════════
# TYPE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

from boto3.dynamodb.types import TypeDeserializer
_deserializer = TypeDeserializer()

def deserialize(dynamo_item: dict) -> dict:
    return {k: _deserializer.deserialize(v) for k, v in dynamo_item.items()}

def to_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def to_int(val):
    if val is None:
        return None
    try:
        return int(Decimal(str(val))) if isinstance(val, (Decimal, float, str)) else int(val)
    except (TypeError, ValueError):
        return None

def to_str(val):
    return str(val) if val is not None else None

def to_ts(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val))
    except (ValueError, TypeError):
        pass
    try:
        return datetime.utcfromtimestamp(float(val))
    except (TypeError, ValueError):
        return None

# ═══════════════════════════════════════════════════════════════════════════════
# POSTGRES CONNECTION
# ═══════════════════════════════════════════════════════════════════════════════

def get_pg_connection():
    conn = psycopg2.connect(**PG_DSN)
    conn.autocommit = False
    return conn

# ═══════════════════════════════════════════════════════════════════════════════
# TABLE 1 — merchant_balance
# UPSERT key: (dbId=5, merchantId)
# ═══════════════════════════════════════════════════════════════════════════════

def transform_merchant_balance(item: dict) -> dict:
    return {
        "dbId":            DB_ID,
        "DynamoId":        to_str(item.get("id")),
        "merchantId":      to_int(item.get("merchantId")),
        "in_hand":         to_float(item.get("in_hand")),
        "total":           to_float(item.get("total")),
        "available":       to_float(item.get("available")),
        "on_hold":         to_float(item.get("on_hold")),
        "status":          to_int(item.get("status")),
        "createdDate":     to_ts(item.get("createdDate")),
        "amount":          to_float(item.get("amount")),
        "reference":       item.get("reference"),
        "transactionType": to_int(item.get("transactionType")),
        "comments":        item.get("comments"),
        "currency":        item.get("currency", "PKR"),
        "fxrate":          to_float(item.get("fxrate")) or 1.0,
        "inHand":          to_float(item.get("inHand", 0)),
        "onHold":          to_float(item.get("onHold", 0)),
        "updatedDate":     to_ts(item.get("updatedDate")) or datetime.now(PKT),
    }

UPSERT_MERCHANT_BALANCE = """
INSERT INTO warehouse.merchant_balance
    ("dbId","DynamoId","merchantId","in_hand","total","available","on_hold",
     "status","createdDate","amount","reference","transactionType",
     "comments","currency","fxrate","inHand","onHold","updatedDate")
VALUES
    (%(dbId)s,%(DynamoId)s,%(merchantId)s,%(in_hand)s,%(total)s,%(available)s,
     %(on_hold)s,%(status)s,%(createdDate)s,%(amount)s,%(reference)s,
     %(transactionType)s,%(comments)s,%(currency)s,%(fxrate)s,%(inHand)s,
     %(onHold)s,%(updatedDate)s)
ON CONFLICT ("dbId","merchantId") WHERE "dbId" = 5
    DO UPDATE SET
        "DynamoId"        = EXCLUDED."DynamoId",
        "in_hand"         = EXCLUDED."in_hand",
        "total"           = EXCLUDED."total",
        "available"       = EXCLUDED."available",
        "on_hold"         = EXCLUDED."onHold",
        "status"          = EXCLUDED."status",
        "createdDate"     = EXCLUDED."createdDate",
        "amount"          = EXCLUDED."amount",
        "reference"       = EXCLUDED."reference",
        "transactionType" = EXCLUDED."transactionType",
        "comments"        = EXCLUDED."comments",
        "currency"        = EXCLUDED."currency",
        "fxrate"          = EXCLUDED."fxrate",
        "inHand"          = EXCLUDED."inHand",
        "updatedDate"     = EXCLUDED."updatedDate"
"""

def upsert_merchant_balance(cur, item: dict, dynamo_id: str = None):
    row = transform_merchant_balance(item)
    log.info(
        "[merchant_balance] UPSERT | merchantId=%s DynamoId=%s available=%s total=%s updatedDate=%s",
        row["merchantId"], row["DynamoId"], row["available"], row["total"], row["updatedDate"]
    )
    cur.execute(UPSERT_MERCHANT_BALANCE, row)

# ═══════════════════════════════════════════════════════════════════════════════
# TABLE 2 — merchant_balance_logs
# UPSERT key: (dbId=5, DynamoId, merchantId)
# ═══════════════════════════════════════════════════════════════════════════════

def transform_merchant_balance_logs(item: dict, dynamo_id: str = None) -> dict:
    return {
        "dbId":            DB_ID,
        "DynamoId":        dynamo_id or to_str(item.get("Id")),
        "merchantId":      to_int(item.get("merchantId")),
        "in_hand":         to_float(item.get("in_hand", 0)),
        "total":           to_float(item.get("total", 0)),
        "available":       to_float(item.get("available", 0)),
        "on_hold":         to_float(item.get("onHold", 0)),
        "status":          to_int(item.get("status", 1)),
        "createdDate":     to_ts(item.get("createdDate")) or datetime.now(PKT),
        "amount":          to_str(item.get("amount")),
        "reference":       item.get("reference"),
        "transactionType": to_int(item.get("transactionType", 0)),
        "currency":        item.get("currency"),
        "fxrate":          to_float(item.get("fxrate")),
        "comment":         item.get("comment") or item.get("comments"),
        "referenceStatus": item.get("referenceStatus"),
    }

UPSERT_MERCHANT_BALANCE_LOGS = """
INSERT INTO warehouse.merchant_balance_logs
    ("dbId","DynamoId","merchantId","in_hand","total","available","on_hold",
     "status","createdDate","amount","reference","transactionType",
     "currency","fxrate","comment","referenceStatus")
VALUES
    (%(dbId)s,%(DynamoId)s,%(merchantId)s,%(in_hand)s,%(total)s,%(available)s,
     %(on_hold)s,%(status)s,%(createdDate)s,%(amount)s,%(reference)s,
     %(transactionType)s,%(currency)s,%(fxrate)s,%(comment)s,%(referenceStatus)s)
ON CONFLICT ("dbId","DynamoId","merchantId") WHERE "dbId" = 5
    DO UPDATE SET
        "in_hand"         = EXCLUDED."in_hand",
        "total"           = EXCLUDED."total",
        "available"       = EXCLUDED."available",
        "on_hold"         = EXCLUDED."on_hold",
        "status"          = EXCLUDED."status",
        "createdDate"     = EXCLUDED."createdDate",
        "amount"          = EXCLUDED."amount",
        "reference"       = EXCLUDED."reference",
        "transactionType" = EXCLUDED."transactionType",
        "currency"        = EXCLUDED."currency",
        "fxrate"          = EXCLUDED."fxrate",
        "comment"         = EXCLUDED."comment",
        "referenceStatus" = EXCLUDED."referenceStatus"
"""

def upsert_merchant_balance_logs(cur, item: dict, dynamo_id: str = None):
    row = transform_merchant_balance_logs(item, dynamo_id)
    log.info(
        "[merchant_balance_logs] UPSERT | merchantId=%s DynamoId=%s amount=%s transactionType=%s",
        row["merchantId"], row["DynamoId"], row["amount"], row["transactionType"]
    )
    cur.execute(UPSERT_MERCHANT_BALANCE_LOGS, row)

# ═══════════════════════════════════════════════════════════════════════════════
# TABLE 3 — merchant_operator_balance
# UPSERT key: (dbId=5, merchantId, operatorId)
# ═══════════════════════════════════════════════════════════════════════════════

def transform_merchant_operator_balance(item: dict) -> dict:
    return {
        "dbId":        DB_ID,
        "merchantId":  to_int(item.get("merchantId")),
        "operatorId":  to_int(item.get("operatorId")),
        "dealId":      to_float(item.get("dealId")),
        "available":   to_float(item.get("available", 0)),
        "onHold":      to_float(item.get("onHold", 0)),
        "status":      to_int(item.get("status")),
        "createdDate": to_ts(item.get("createdDate")) or datetime.now(PKT),
        "updatedDate": to_ts(item.get("updatedDate")) or datetime.now(PKT),
    }

UPSERT_MERCHANT_OPERATOR_BALANCE = """
INSERT INTO warehouse.merchant_operator_balance
    ("dbId","merchantId","operatorId","dealId","available","onHold",
     "status","createdDate","updatedDate")
VALUES
    (%(dbId)s,%(merchantId)s,%(operatorId)s,%(dealId)s,%(available)s,%(onHold)s,
     %(status)s,%(createdDate)s,%(updatedDate)s)
ON CONFLICT ("dbId","merchantId","operatorId")
    DO UPDATE SET
        "dealId"      = EXCLUDED."dealId",
        "available"   = EXCLUDED."available",
        "onHold"      = EXCLUDED."onHold",
        "status"      = EXCLUDED."status",
        "createdDate" = EXCLUDED."createdDate",
        "updatedDate" = EXCLUDED."updatedDate"
"""

def upsert_merchant_operator_balance(cur, item: dict, dynamo_id: str = None):
    row = transform_merchant_operator_balance(item)
    log.info(
        "[merchant_operator_balance] UPSERT | merchantId=%s operatorId=%s available=%s onHold=%s",
        row["merchantId"], row["operatorId"], row["available"], row["onHold"]
    )
    cur.execute(UPSERT_MERCHANT_OPERATOR_BALANCE, row)

# ═══════════════════════════════════════════════════════════════════════════════
# TABLE 4 — merchant_operator_balance_logs
# UPSERT key: (dbId=5, id, operatorId)
# ═══════════════════════════════════════════════════════════════════════════════

def transform_merchant_operator_balance_logs(item: dict) -> dict:
    return {
        "dbId":            DB_ID,
        "id":              to_str(item.get("id")),
        "merchantId":      to_int(item.get("merchantId")),
        "operatorId":      to_int(item.get("operatorId")),
        "amount":          to_str(item.get("amount")),
        "onHold":          to_float(item.get("onHold", 0)),
        "reference":       item.get("reference"),
        "currency":        item.get("currency"),
        "transactionType": to_int(item.get("transactionType")),
        "referenceStatus": item.get("referenceStatus"),
        "status":          to_int(item.get("status")),
        "comment":         item.get("comment") or item.get("comments"),
        "createdDate":     to_ts(item.get("createdDate")) or datetime.now(PKT),
        "fxrate":          to_float(item.get("fxrate")),
    }

UPSERT_MERCHANT_OPERATOR_BALANCE_LOGS = """
INSERT INTO warehouse.merchant_operator_balance_logs
    ("dbId","id","merchantId","operatorId","amount","onHold","reference",
     "currency","transactionType","referenceStatus","status","comment",
     "createdDate","fxrate")
VALUES
    (%(dbId)s,%(id)s,%(merchantId)s,%(operatorId)s,%(amount)s,%(onHold)s,%(reference)s,
     %(currency)s,%(transactionType)s,%(referenceStatus)s,%(status)s,%(comment)s,
     %(createdDate)s,%(fxrate)s)
ON CONFLICT ("dbId","id","operatorId")
    DO UPDATE SET
        "merchantId"      = EXCLUDED."merchantId",
        "amount"          = EXCLUDED."amount",
        "onHold"          = EXCLUDED."onHold",
        "reference"       = EXCLUDED."reference",
        "currency"        = EXCLUDED."currency",
        "transactionType" = EXCLUDED."transactionType",
        "referenceStatus" = EXCLUDED."referenceStatus",
        "status"          = EXCLUDED."status",
        "comment"         = EXCLUDED."comment",
        "createdDate"     = EXCLUDED."createdDate",
        "fxrate"          = EXCLUDED."fxrate"
"""

def upsert_merchant_operator_balance_logs(cur, item: dict, dynamo_id: str = None):
    row = transform_merchant_operator_balance_logs(item)
    log.info(
        "[merchant_operator_balance_logs] UPSERT | id=%s merchantId=%s operatorId=%s amount=%s transactionType=%s",
        row["id"], row["merchantId"], row["operatorId"], row["amount"], row["transactionType"]
    )
    cur.execute(UPSERT_MERCHANT_OPERATOR_BALANCE_LOGS, row)

# ═══════════════════════════════════════════════════════════════════════════════
# EVENT ROUTER
# ═══════════════════════════════════════════════════════════════════════════════

TABLE_HANDLERS = {
    "merchant_balance":               upsert_merchant_balance,
    "merchant_balance_logs":          upsert_merchant_balance_logs,
    "merchant_operator_balance":      upsert_merchant_operator_balance,
    "merchant_operator_balance_logs": upsert_merchant_operator_balance_logs,
}

def process_record(cur, record: dict, table_type: str):
    event_name    = record.get("eventName")
    dynamo        = record.get("dynamodb", {})
    new_image_raw = dynamo.get("NewImage")
    old_image_raw = dynamo.get("OldImage")
    keys_raw      = dynamo.get("Keys", {})

    keys_deserialized = deserialize(keys_raw) if keys_raw else {}
    dynamo_id = to_str(
        keys_deserialized.get("Id")
        or keys_deserialized.get("id")   
        # or keys_deserialized.get("merchantId")
        or dynamo.get("SequenceNumber")
    )

    new_item = deserialize(new_image_raw) if new_image_raw else None
    old_item = deserialize(old_image_raw) if old_image_raw else None
    item     = new_item or old_item

    if not item:
        log.error(
            "[%s] Empty record — no NewImage or OldImage | eventName=%s seq=%s",
            table_type, event_name, dynamo.get("SequenceNumber")
        )
        return

    upsert_fn = TABLE_HANDLERS.get(table_type)
    if upsert_fn is None:
        log.error("No handler registered for table_type=%s — skipping", table_type)
        return

    if event_name in ("INSERT", "MODIFY"):
        upsert_fn(cur, item, dynamo_id)
    elif event_name == "REMOVE":
        pass  # deletes intentionally ignored
    else:
        log.error("[%s] Unknown eventName=%s seq=%s — skipping", table_type, event_name, dynamo.get("SequenceNumber"))

# ═══════════════════════════════════════════════════════════════════════════════
# KINESIS POLLING
# ═══════════════════════════════════════════════════════════════════════════════

def get_all_shard_iterators(kinesis_client, stream_name: str) -> list:
    try:
        resp = kinesis_client.describe_stream(StreamName=stream_name)
    except NoCredentialsError as exc:
        raise RuntimeError(
            "AWS credentials missing. Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY "
            "or attach an IAM role to the EC2 instance."
        ) from exc
    except ClientError as exc:
        raise RuntimeError(
            f"Could not describe Kinesis stream '{stream_name}': {exc}"
        ) from exc

    iterators = []
    for shard in resp["StreamDescription"]["Shards"]:
        shard_id = shard["ShardId"]
        it = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType="LATEST",
        )["ShardIterator"]
        iterators.append(it)

    return iterators


def poll_stream(kinesis_client, pg_conn, stream_name: str,
                table_type: str, shard_iterators: list) -> list:
    new_iterators = []

    for it in shard_iterators:
        try:
            resp = kinesis_client.get_records(ShardIterator=it, Limit=1)

        except kinesis_client.exceptions.ExpiredIteratorException:
            log.error(
                "Shard iterator expired for stream=%s — rebuilding from LATEST",
                stream_name
            )
            return get_all_shard_iterators(kinesis_client, stream_name)

        except ClientError as exc:
            log.error(
                "Kinesis get_records failed | stream=%s error=%s",
                stream_name, exc, exc_info=True
            )
            new_iterators.append(it)
            continue

        records = resp.get("Records", [])

        if records:
            cur = pg_conn.cursor()
            success_count = 0
            try:
                for kinesis_record in records:
                    seq = kinesis_record.get("SequenceNumber", "?")
                    try:
                        raw = kinesis_record["Data"]
                        payload = json.loads(
                            raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
                        )
                        process_record(cur, payload, table_type)
                        pg_conn.commit()
                        success_count += 1

                    except json.JSONDecodeError as exc:
                        log.error(
                            "[%s] JSON decode failed | seq=%s error=%s",
                            table_type, seq, exc
                        )
                    except KeyError as exc:
                        log.error(
                            "[%s] Missing expected field | seq=%s missing_key=%s",
                            table_type, seq, exc
                        )
                    except psycopg2.Error as exc:
                        log.error(
                            "[%s] Postgres error | seq=%s pgcode=%s error=%s",
                            table_type, seq, exc.pgcode, exc, exc_info=True
                        )
                        pg_conn.rollback()
                        cur.close()
                        cur = pg_conn.cursor()
                    except Exception as exc:
                        log.error(
                            "[%s] Unexpected error | seq=%s error=%s",
                            table_type, seq, exc, exc_info=True
                        )

            except Exception as exc:
                pg_conn.rollback()
                log.error(
                    "Fatal error polling stream=%s — rolled back | error=%s",
                    stream_name, exc, exc_info=True
                )
            finally:
                cur.close()

        next_it = resp.get("NextShardIterator")
        if next_it:
            new_iterators.append(next_it)

    return new_iterators

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    kinesis = boto3.client("kinesis", region_name=AWS_REGION)

    shard_iterators = {}
    for table_type, stream_name in STREAMS.items():
        try:
            shard_iterators[table_type] = get_all_shard_iterators(kinesis, stream_name)
        except RuntimeError as exc:
            log.error("Failed to init stream '%s': %s", stream_name, exc)
            return

    try:
        pg_conn = get_pg_connection()
    except psycopg2.OperationalError as exc:
        log.error("Cannot connect to Postgres | host=%s error=%s", PG_DSN["host"], exc, exc_info=True)
        return

    while True:
        if pg_conn.closed:
            log.error("Postgres connection lost — reconnecting...")
            try:
                pg_conn = get_pg_connection()
            except psycopg2.OperationalError as exc:
                log.error("Postgres reconnect failed — retrying in 10s | error=%s", exc)
                time.sleep(10)
                continue

        for table_type, stream_name in STREAMS.items():
            try:
                shard_iterators[table_type] = poll_stream(
                    kinesis, pg_conn, stream_name,
                    table_type, shard_iterators[table_type]
                )
            except Exception as exc:
                log.error(
                    "Unhandled error | stream=%s table=%s error=%s",
                    stream_name, table_type, exc, exc_info=True
                )

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
