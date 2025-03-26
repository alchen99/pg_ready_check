# pg_ready_check
Check if PostgreSQL connection is ready and optionally check for the existence of tables.

## Features
* Connection Check: Verifies if a connection to the PostgreSQL server can be established.
* Table Existence Check: Optionally checks if one or more specified tables exist in the target database.
* Retry Mechanism: Waits and retries the checks until success or a timeout is reached.
* Configurable: Uses command-line flags and standard PostgreSQL environment variables (PGHOST, PGPORT, PGUSER, PGDATABASE, PGPASSWORD).
* Exit Codes: Uses exit codes similar to pg_isready (0 for success, 1 for connection failure, 2 for check failure like missing tables, 3 for bad arguments).

## Usage

### Basic connection check (like pg_isready) - uses defaults/env vars
`./pg_ready_check`

### Specify connection parameters
`./pg_ready_check -host=my_db_host -port=5433 -username=app_user -dbname=my_app`

### Wait up to 2 minutes, checking connection and existence of 'users' table
`./pg_ready_check -timeout=2m -tables=users`

### Check for multiple tables, including one in a specific schema
`./pg_ready_check -tables=public.users,orders,audit.logs`

### Run quietly (only exit code matters) - useful in scripts
`./pg_ready_check -quiet -tables=migrations`

## Output

### Success
```
If it succeeds:
(No output if -quiet is used)
Otherwise:
INFO: Attempting to connect to database: host=...
INFO: Will also check for tables: [users,orders]
INFO: Waiting up to 1m0s for database to be ready...
INFO: Connection successful.
INFO: All required tables [users,orders] found.
INFO: Database ready after 1.5s.
(Exit Code 0)
```

### Connection fails within timeout
```
(No output if -quiet is used)
Otherwise:
INFO: Attempting to connect...
INFO: Waiting up to 1m0s...
DEBUG: connection attempt failed: failed to connect... (repeated)
ERROR: Overall timeout (1m0s) exceeded. Last error: connection attempt failed: ...
(Exit Code 1)
```

### If connection succeeds but tables are missing:
```
(No output if -quiet is used)
Otherwise:
INFO: Attempting to connect...
INFO: Waiting up to 1m0s...
INFO: Connection successful.
DEBUG: required tables missing: orders (repeated)
ERROR: Overall timeout (1m0s) exceeded. Last error: required tables missing: orders
(Exit Code 1 - Timeout eventually occurs if tables never appear)
Note: If the timeout happens *during* the table check, it might exit 1. If the *reason* it timed out was missing tables, arguably exit code 2 might be better, but timeout usually implies connection issues. Let's stick to 1 for timeout, 2 only if connection works but tables *definitively* don't exist *when checked*. We could refine this to return 2 if the *last known error* before timeout was missing tables.
```
