# Flexible Database Extractor

A flexible database extraction tool that extracts data from various RDBMS sources (PostgreSQL, MySQL, Oracle, SQL Server) and exports it to cloud storage or local files in Parquet format. The tool supports time-based partitioning, custom queries, and configurable cloud storage output destinations. It's designed to be put in place for on-going batch extracts. As such, when executed, it will extract data from the number of days value in "last_n_days" through yesterday. 

You can then either import the Parquet files into your Data Warehouse of choice, or simply add them as external tables and go!

## Features

- **Multiple Database Support**: PostgreSQL, MySQL, Oracle, and SQL Server
- **Time-based Extraction**: Extract data within configurable time windows using timestamp columns
- **Flexible Output**: Export to Google Cloud Storage, AWS S3, Azure Blob Storage, or local
- **Parquet Format**: Optimized columnar storage with configurable compression
- **Custom Queries**: Support for both table extracts and custom SQL queries
- **Batch Processing**: Configurable chunk sizes for memory-efficient processing
- **Templated Naming**: Dynamic file and object naming with template variables

## Installation

```bash
pip install -r requirements.txt
```
## Tips

Create and use a "read-only" user on your source database(s). 

## Configuration

The tool is configured using a YAML file. Below is a complete breakdown of all configuration options:

### Database Connection (`source_database`)

Configure your source database connection:

```yaml
source_database:
  type: postgresql          # Required: postgresql | mysql | oracle | mssql
  host: 127.0.0.1          # Database host
  port: 5432               # Database port
  database: postgres       # Database name
  user: user               # Username
  password: secret         # Password
```

**Alternative Connection Method:**
```yaml
source_database:
  url: postgresql+psycopg2://myuser:mypassword@127.0.0.1:5432/mydb
```

**Database-Specific Options:**

For Oracle databases:
```yaml
source_database:
  type: oracle
  # Use either service_name OR sid 
  service_name: ORCLPDB1   # Oracle service name
  # sid: ORCL              # Oracle SID (alternative)
  # For Oracle Autonomous DB (type: oracle) Supply a single URL that includes your DSN (URL-encoded)
  # url: "oracle+oracledb://TESTUSER:aGreatPassword1@/?dsn=tcps%3A%2F%2Fadb.us-ashburn-1.oraclecloud.com%3A1522%2Fg65e2f200d6df76_testdb_medium.adb.oraclecloud.com%3Fretry_count%3D20%26retry_delay%3D3%26ssl_server_dn_match%3Dtrue"

```

For SQL Server databases:
```yaml
source_database:
  type: mssql
  odbc_driver: "ODBC Driver 18 for SQL Server"  # Optional ODBC driver
```

### Data Sources (`sources`)

Define one or more data extraction sources. Each source can be either a table extract or a custom query.

#### Table Extract Example:
```yaml
sources:
  - name: parenttable                    # Unique identifier for this source
    schema: bigclient                    # Database schema
    table: parenttable                   # Table name
    timestamp_column: createdon          # Column used for time-based filtering
    last_n_days: 3                      # Extract data from last N days
    upper_bound_inclusive: false         # Use < (false) or <= (true) for upper bound
    output_filename: "{{schema}}_{{table}}_{{yesterday}}.parquet"
    object_name: "exports/{{schema}}/{{table}}/dt={{yesterday}}/extract_{{run_ts}}.parquet"
```

#### Custom Query Example:
```yaml
sources:
  - name: simplequery
    query: |                           # Custom SQL query
      SELECT *
      FROM bigclient.childtable
      WHERE status = 'ACTIVE'
    timestamp_column: modifiedon       # Column for time filtering
    last_n_days: 1
```

#### Source Configuration Options:

| Option | Description | Default |
|--------|-------------|---------|
| `name` | Unique identifier for the source | Required |
| `schema` | Database schema (for table extracts) | Required for tables |
| `table` | Table name (for table extracts) | Required for tables |
| `query` | Custom SQL query (alternative to schema/table) | - |
| `timestamp_column` | Column used for time-based filtering | Required |
| `timestamp_columns` | List of columns to ensure are proper datetime types | - |
| `timestamp_utc` | Convert timestamps to UTC | `true` |
| `timestamp_naive` | Remove timezone info for Parquet compatibility | `true` |
| `last_n_days` | Number of days to look back from yesterday | Required |
| `upper_bound_inclusive` | Include records at exact upper bound time | `false` |
| `output_filename` | Local filename template | Uses global template |
| `object_name` | Cloud storage object name template | Uses global template |
| `where_extra` | Additional WHERE clause conditions | - |

#### Template Variables

The following variables are available for `output_filename` and `object_name` templates:

- `{{schema}}` - Database schema name
- `{{table}}` - Table name
- `{{yesterday}}` - Partition date (YYYY-MM-DD format)
- `{{run_ts}}` - Execution timestamp

### Target Storage (`target`)

Configure where to store the extracted Parquet files:

#### Google Cloud Storage:
```yaml
target:
  type: gcs
  object_name: "exports/{{schema}}/{{table}}/{{yesterday}}/run={{run_ts}}.parquet"
  gcs:
    bucket: your_GCP_bucket
    service_account_key_file: /path/to/service-account-key.json
```

#### AWS S3:
```yaml
target:
  type: s3
  s3:
    bucket: my-s3-bucket
    region: us-east-1
    aws_access_key_id: YOUR_ACCESS_KEY
    aws_secret_access_key: YOUR_SECRET_KEY
    aws_session_token: OPTIONAL_SESSION_TOKEN  # Optional
```

#### Azure Blob Storage:
```yaml
target:
  type: azure        # gcs | s3 | azure | local
  object_name: "exports/{{schema}}/{{table}}/{{yesterday}}/run={{run_ts}}.parquet"
  azure:
    container: my-container
    connection_string: "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=***;EndpointSuffix=core.windows.net"
```

#### Local Filesystem:
```yaml
target:
  type: local
  # Files will be saved to local_temp_dir specified in io section
```

### I/O and Performance (`io`)

Configure processing and performance parameters:

```yaml
io:
  chunksize: 100000                    # Number of rows to process at once
  parquet_compression: snappy          # Compression: snappy | gzip | zstd | none
  local_temp_dir: /tmp                 # Temporary directory for local files
  pandas_read_sql_kwargs:              # Additional pandas.read_sql_query options
    parse_dates: ["modifiedon"]        # Example: parse specific columns as dates
  default_use_text_select: false   # avoid reflection, use quoted text SELECT instead - set to truefor Oracle
```

## Usage

1. **Create your configuration file** (e.g., `config.yaml`) using the examples above
2. **Run the extractor**:
   ```bash
   python extractor.py --config config.yaml
   ```

## Examples

### Extract Recent Orders
```yaml
sources:
  - name: recent_orders
    schema: sales
    table: orders
    timestamp_column: order_date
    last_n_days: 7
    where_extra: "status IN ('PENDING', 'PROCESSING')"
```

### Custom Analytics Query
```yaml
sources:
  - name: daily_summary
    query: |
      SELECT 
        DATE(created_at) as order_date,
        COUNT(*) as order_count,
        SUM(total_amount) as total_revenue
      FROM sales.orders
      GROUP BY DATE(created_at)
    timestamp_column: order_date
    last_n_days: 30
```

### Multiple Time Windows
```yaml
sources:
  - name: recent_data
    table: transactions
    timestamp_column: created_at
    last_n_days: 1
    
  - name: historical_data
    table: transactions
    timestamp_column: created_at
    last_n_days: 30
    output_filename: "historical_{{table}}_{{yesterday}}.parquet"
```

## Troubleshooting

**Connection Issues:**
- Verify database credentials and network connectivity
- Check that the specified database/schema exists
- Ensure proper firewall rules for database access

**Memory Issues:**
- Reduce `chunksize` in the `io` section
- Consider using `gzip` or `zstd` compression for better memory efficiency

**Timestamp Issues:**
- Ensure `timestamp_column` exists and contains valid datetime data
- Check timezone handling with `timestamp_utc` and `timestamp_naive` settings
- Verify the time window with `last_n_days` captures the expected data range

**Storage Issues:**
- Verify cloud storage credentials and permissions
- Check that the specified bucket/container exists
- Ensure sufficient local disk space in `local_temp_dir`

**Other Issues:**
- During testing on a table in PostgreSQL with a uuid column, I needed to cast the column as a varchar.  This can be done using a query or creating a view which does it on the source database and configuring it as a source table.