# DynamoDB CDC Processor

A Python-based Change Data Capture (CDC) consumer that reads DynamoDB
change events from Amazon Kinesis Data Streams, transforms the records,
and upserts them into a PostgreSQL warehouse.

## Architecture

<img width="827" height="554" alt="CDC on DynamoDB to Warehouse using Kinesis" src="https://github.com/user-attachments/assets/bcc16caf-d728-4160-820f-b7825c526563" />

``` text
DynamoDB
   │
   │ DynamoDB change events (forwarded by an upstream integration)
   ▼
Amazon Kinesis Data Streams
   │
   │ boto3 polling (GetRecords)
   ▼
EC2 instance running cdc_processor.py
   │
   │ Transform + PostgreSQL UPSERT
   ▼
PostgreSQL
   └── warehouse schema
```

> This script consumes records from Kinesis. It does not create DynamoDB
> streams, configure the upstream DynamoDB-to-Kinesis delivery, or
> provision AWS/PostgreSQL resources.

## Tables Supported

The processor consumes four Kinesis streams and writes to the
corresponding PostgreSQL tables.

  -------------------------------------------------------------------------------------------------------
  Source / stream purpose            PostgreSQL target                            Conflict key
  ---------------------------------- -------------------------------------------- -----------------------
  `merchant_balance`                 `warehouse.merchant_balance`                 (`dbId`, `merchantId`)
                                                                                  where `dbId = 5`

  `merchant_balance_logs`            `warehouse.merchant_balance_logs`            (`dbId`, `DynamoId`,
                                                                                  `merchantId`) where
                                                                                  `dbId = 5`

  `merchant_operator_balance`        `warehouse.merchant_operator_balance`        (`dbId`, `merchantId`,
                                                                                  `operatorId`)

  `merchant_operator_balance_logs`   `warehouse.merchant_operator_balance_logs`   (`dbId`, `id`,
                                                                                  `operatorId`)
  -------------------------------------------------------------------------------------------------------

The processor uses `DB_ID = 5` for every row.

### Column mapping

-   `merchant_balance.DynamoId` is populated from the DynamoDB item
    field `id`.
-   `merchant_balance_logs.DynamoId` is populated from the DynamoDB
    event key (`Id` or `id`) when available, with the item field `Id` as
    the transform fallback.
-   The other two tables use their respective `merchantId`,
    `operatorId`, and `id` fields as defined in the transformation
    functions.

## Event Processing

-   `INSERT` and `MODIFY` events are transformed and processed using
    PostgreSQL `INSERT ... ON CONFLICT DO UPDATE`.
-   `REMOVE` events are intentionally ignored. Deletes are not
    propagated to PostgreSQL.
-   The consumer requests at most one Kinesis record per shard per
    `GetRecords` call (`Limit=1`).
-   Each successfully processed record is committed individually.
-   DynamoDB AttributeValue objects are deserialized using boto3's
    `TypeDeserializer`.
-   Numeric and timestamp fields are converted by helper functions
    before insertion.
-   Missing timestamps may be replaced with the current Asia/Karachi
    time in the table-specific transformation functions.

## Requirements

-   Python 3.9 or later is recommended.
-   An EC2 instance with network access to the Kinesis streams and
    PostgreSQL.
-   AWS credentials with permission to describe the configured streams,
    obtain shard iterators, and read records.
-   PostgreSQL access to the target database and `warehouse` schema.
-   Matching target-table columns and unique constraints/indexes for the
    `ON CONFLICT` clauses.

## Installation

SSH into the EC2 instance and install the dependencies:

``` bash
python3 -m pip install boto3 psycopg2-binary python-dotenv
```

If your Python version does not provide `zoneinfo`, install the
backport:

``` bash
python3 -m pip install backports.zoneinfo
```

Place the script on the EC2 instance as:

``` text
cdc_processor.py
```

## Configuration

The application loads environment variables from a `.env` file in its
working directory. Environment variables supplied by the EC2 environment
also work.

Create a `.env` file alongside the script:

``` dotenv
# AWS
AWS_REGION=ap-southeast-1

# Kinesis streams
KINESIS_MERCHANT_BALANCE_STREAM=merchant-balance-cdc
KINESIS_MERCHANT_BALANCE_LOGS_STREAM=merchant-balance-logs-cdc
KINESIS_MERCHANT_OPERATOR_BALANCE_STREAM=merchant-operator-balance-cdc
KINESIS_MERCHANT_OPERATOR_BALANCE_LOGS_STREAM=merchant-operator-balance-log-cdc

# Consumer polling interval
POLL_INTERVAL_SEC=1

# PostgreSQL
PG_HOST=127.0.0.1
PG_PORT=5432
PG_DB=postgres
PG_USER=postgres
PG_PASSWORD=replace_with_password

# Logging
LOG_DIR=logs
```

All values above have defaults in the script except `PG_PASSWORD`, which
defaults to an empty string. Replace the sample PostgreSQL values with
the actual connection details. Do not commit credentials or the `.env`
file to source control.

### Configuration reference

  ---------------------------------------------------------------------------------------------------------------
  Variable                                          Default                               Description
  ------------------------------------------------- ------------------------------------- -----------------------
  `AWS_REGION`                                      `ap-southeast-1`                      AWS region containing
                                                                                          the Kinesis streams

  `KINESIS_MERCHANT_BALANCE_STREAM`                 `merchant-balance-cdc`                Stream for merchant
                                                                                          balance records

  `KINESIS_MERCHANT_BALANCE_LOGS_STREAM`            `merchant-balance-logs-cdc`           Stream for merchant
                                                                                          balance log records

  `KINESIS_MERCHANT_OPERATOR_BALANCE_STREAM`        `merchant-operator-balance-cdc`       Stream for
                                                                                          merchant/operator
                                                                                          balance records

  `KINESIS_MERCHANT_OPERATOR_BALANCE_LOGS_STREAM`   `merchant-operator-balance-log-cdc`   Stream for
                                                                                          merchant/operator
                                                                                          balance log records

  `POLL_INTERVAL_SEC`                               `1`                                   Delay between polling
                                                                                          cycles, in seconds

  `PG_HOST`                                         `localhost`                           PostgreSQL hostname

  `PG_PORT`                                         `5432`                                PostgreSQL port

  `PG_DB`                                           `postgres`                            PostgreSQL database
                                                                                          name

  `PG_USER`                                         `postgres`                            PostgreSQL username

  `PG_PASSWORD`                                     Empty                                 PostgreSQL password

  `LOG_DIR`                                         `logs`                                Directory where the log
                                                                                          file is written
  ---------------------------------------------------------------------------------------------------------------

## AWS Permissions

The EC2 instance needs AWS credentials. An attached EC2 IAM role is
preferable to long-lived access keys.

At minimum, grant read access to the configured Kinesis streams,
including these actions:

-   `kinesis:DescribeStream`
-   `kinesis:GetShardIterator`
-   `kinesis:GetRecords`

Scope the policy to the four stream ARNs where practical. The script
does not require DynamoDB permissions because it reads from Kinesis.

## PostgreSQL Preparation

Before starting the consumer:

1.  Create the `warehouse` schema if it does not already exist.
2.  Ensure all four target tables exist.
3.  Ensure target columns and data types match the insert statements in
    the script.
4.  Create unique constraints or unique indexes that match each
    `ON CONFLICT` target.
5.  Ensure the configured PostgreSQL user has the required schema/table
    privileges.

For the first two tables, the SQL specifies partial conflict targets
with the predicate `WHERE "dbId" = 5`. PostgreSQL therefore requires
matching partial unique indexes for those conflict targets. The other
two statements use non-partial conflict targets.

## Running the Processor

Run from the directory containing `cdc_processor.py` and `.env`:

``` bash
python3 cdc_processor.py
```

The process runs continuously and polls all four streams in sequence.
Stop it with `Ctrl+C` when running interactively.

## Logging

The script creates the configured log directory if it does not exist and
writes logs to:

``` text
logs/cdc_processor.log
```

The log format includes the timestamp, severity, logger name, and
message. The file handler and console handler are both configured at
`INFO` level. boto3, botocore, and urllib3 loggers are reduced to
`WARNING`.

Logs include successful UPSERT details and errors such as:

-   Kinesis stream initialization or polling failures
-   Expired shard iterators
-   JSON decoding errors
-   Missing expected fields
-   PostgreSQL errors
-   PostgreSQL connection failures and reconnection attempts

## Operational Behaviour and Limitations

### Starting position and restart behaviour

The script requests shard iterators with `ShardIteratorType="LATEST"`
when it initializes a stream or rebuilds expired iterators. This starts
reading from the latest position rather than replaying earlier records.

The script does not persist Kinesis sequence numbers or checkpoints.
After a process restart, it initializes from `LATEST`; records that
arrived while the consumer was stopped may therefore not be processed by
this script. If replay/recovery is required, implement durable
checkpointing and an appropriate iterator strategy before relying on
this process for lossless CDC.

### Error handling

-   PostgreSQL errors trigger a rollback for the failed record and the
    consumer continues.
-   JSON decoding and missing-key errors are logged and processing
    continues.
-   A failed or malformed record is not placed in a durable retry queue
    by this script.
-   The Kinesis iterator is advanced using the returned
    `NextShardIterator`; consequently, logging an error does not itself
    guarantee that the record will be retried.

### Deletes

`REMOVE` events are ignored by design. A row deleted in DynamoDB can
remain in PostgreSQL unless it is removed through a separate process.

### Throughput

The `Limit=1` setting limits each `GetRecords` request to one record per
shard. The streams are polled sequentially, and the configured polling
interval is applied after a complete polling cycle. Throughput and
processing latency depend on the number of streams/shards, database
response time, and polling interval.

## Troubleshooting

  -----------------------------------------------------------------------
  Symptom                             Checks
  ----------------------------------- -----------------------------------
  AWS credentials error               Confirm the EC2 IAM role or AWS
                                      credentials are available and have
                                      the required Kinesis permissions.

  Stream cannot be described          Verify the stream name, AWS region,
                                      stream status, and IAM policy.

  PostgreSQL connection failure       Check `PG_HOST`, `PG_PORT`,
                                      database credentials, security
                                      groups, routing, and PostgreSQL
                                      availability.

  `ON CONFLICT` constraint error      Confirm the target unique
                                      constraint/index exactly matches
                                      the conflict columns and any
                                      partial-index predicate.

  Missing-column or datatype error    Compare the target table definition
                                      with the insert statements and
                                      transformation functions.

  No records are processed            Confirm the upstream integration is
                                      publishing to the configured
                                      streams and that the consumer is
                                      reading the correct region and
                                      stream names.

  Records are missing after a restart The current implementation starts
                                      from `LATEST` and has no persisted
                                      checkpoints; implement
                                      checkpointing/replay if recovery is
                                      required.
  -----------------------------------------------------------------------

## Important Code Review Note

Review the `merchant_balance` UPSERT before production use. The insert
statement writes the PostgreSQL column `"on_hold"`, but the `DO UPDATE`
clause currently assigns it from `EXCLUDED."onHold"`:

``` sql
"on_hold" = EXCLUDED."onHold"
```

`EXCLUDED` exposes the inserted target-table column names, so this
reference should be checked against the actual target schema; based on
the insert column list, it appears the intended reference is:

``` sql
"on_hold" = EXCLUDED."on_hold"
```

Validate this correction against the real table definition before
deploying.

## Project Files

``` text
.
├── cdc_processor.py
├── .env
└── logs/
    └── cdc_processor.log
```

Keep `.env` and log files out of version control if they contain
credentials or operational information.
