# DynamoDB Source Connector

Replicates Amazon DynamoDB tables into OLake destinations (Apache Iceberg, Parquet).

## Sync modes

| Mode | Support | How |
|------|---------|-----|
| Full Refresh (default) | ✅ | Parallel table scan with one chunk per segment |
| Incremental | ✅ | Strongly consistent scans of a bounded cursor range |
| CDC | ❌ | DynamoDB Streams integration is outside the scope of this connector |

### Incremental sync

Set the stream's sync mode to `incremental` and choose a top-level Number or
String attribute as `cursor_field`. Cursor values must increase across the
table in the order writes become visible, including writes to different items. DynamoDB orders
strings by their UTF-8 bytes. For timestamps, use UTC with a consistent format
and fixed fractional precision. Set `cursor_field` to
`primary_cursor:secondary_cursor` to track two independent watermarks. An item
is included when either cursor advances.

Each incremental sync first projects the cursor attributes to find the upper
bound. If any cursor advances, a second scan reads the rows in that range. Both
scans use strongly consistent reads and consume capacity for the whole table.
The upper bound leaves newer writes for the following sync. Items without a
qualifying cursor are skipped after backfill. Incremental sync does not
propagate source deletes or provide a point-in-time snapshot of the table.

Catalog filters run before column selection. A filter may reference a source
attribute even when that attribute is excluded from the destination.

## Schema discovery

DynamoDB is schemaless, so the connector samples items (1 → 100 → 10000 record
buckets, same mechanism as MongoDB) and merges the observed attribute types.
Key attributes are always present with their declared types, even for empty
tables. Attributes unseen during discovery arrive as new columns on later
syncs when the destination supports schema evolution.

Type mapping:

| DynamoDB | OLake |
|----------|-------|
| S | string (ISO-8601 strings are detected as timestamps) |
| N | int64 or exact decimal when representable. Numbers outside those ranges use floating-point destination columns, with exact numeric checkpoints retained in state |
| B | binary (base64 string when the destination has no binary support) |
| BOOL | boolean |
| NULL | null |
| M / L | object / array |
| SS / NS / BS | array |

## Configuration

| Field | Required | Description |
|-------|----------|-------------|
| `region` | yes* | AWS region (*optional when `endpoint` is set) |
| `access_key_id` / `secret_access_key` | no | Static credentials. With neither field set, AWS endpoints use the default credential chain and custom endpoints use local test credentials |
| `session_token` | no | For temporary credentials |
| `endpoint` | no | Custom endpoint, e.g. `http://localhost:8000` for DynamoDB Local |
| `table_pattern` | no | Regex filter over table names |
| `consistent_read` | no | Use strongly consistent item reads for discovery and full refresh (default false). Incremental sync, including its initial backfill, always uses strongly consistent reads |
| `max_threads` | no | Parallel scan segments per table (default 3) |
| `retry_count` | no | Retries for failed operations (default 3) |

Example (`config.json`):

```json
{
  "region": "us-east-1",
  "access_key_id": "AKIA...",
  "secret_access_key": "...",
  "table_pattern": "^prod-.*",
  "max_threads": 8
}
```

### IAM permissions

```
dynamodb:ListTables
dynamodb:DescribeTable
dynamodb:Scan
```

## Local development

```bash
make olake.dynamodb.start      # DynamoDB Local on localhost:8000
make dev.dynamodb.build        # host binary at drivers/dynamodb/olake
cd drivers/dynamodb
./olake spec
./olake check --config config.json
./olake discover --config config.json
./olake sync --config config.json --destination writer.json \
  --available-streams available_streams.json --selected-streams selected_streams.json
```

Against DynamoDB Local, credentials can be omitted (the driver signs with dummy
credentials) and `region` defaults to `us-east-1`.
