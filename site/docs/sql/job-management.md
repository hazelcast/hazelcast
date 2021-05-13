---
title: Job Management
description: Commands to manage SQL jobs
---

## CREATE/DROP/ALTER JOB

These statements create a potentially long-running Jet job that is not
tied to the client session.

When you submit a standard INSERT query, its lifecycle is tied to the
client session: if the client disconnects, the statement is cancelled.

If you want to submit a statement that is independent from the client
session, use the `CREATE JOB` command. This command will complete
quickly and the job will be running in the cluster. You can also
configure such a job to be fault-tolerant.

### CREATE JOB Synopsis

```sql
CREATE JOB [IF NOT EXISTS] job_name
[ OPTIONS ( 'option_name' = 'option_value' [, ...] ) ]
AS query_spec
```

- `job_name`: a unique name identifying the job.

- `query_spec`: the query the job will run. Currently we support `INSERT
  INTO` and `SINK INTO` queries. `SELECT` is not supported by design
  because it returns the results to the client.

- `option_name`, `option_value`: the job configuration options. The list
  of options matches the methods in the `JobConfig` class.

#### List of Available Job Options

|Option|Allowed value|
|--|--|
|`processingGuarantee`|`exactlyOnce`<br>`atLeastOnce`<br>`none`|
|`snapshotIntervalMillis`|a positive number|
|`autoScaling`|`true`, `false`|
|`splitBrainProtectionEnabled`|`true`, `false`|
|`metricsEnabled`|`true`, `false`|
|`storeMetricsAfterJobCompletion`|`true`, `false`|
|`initialSnapshotName`|any string|
|`maxProcessorAccumulatedRecords`|a positive number|

The methods to add resources and classes to the job classpath (e.g.
`addClass`, `addJar`) aren't supported. If you need some library or
resource, it has to be available on the members' classpath.

#### Example

```sql
CREATE JOB myJob
OPTIONS (
    'processingGuarantee' = 'exactlyOnce',
    'snapshotIntervalMillis' = '5000',
    'metricsEnabled' = 'true'
) AS
INSERT INTO my_sink_topic
SELECT * FROM my_source_topic
```

### DROP JOB Synopsis

```sql
DROP JOB [IF EXISTS] job_name [WITH SNAPSHOT snapshot_name]
```

- `IF EXISTS`: return silently if the job doesn't exist or already
terminated

- `WITH SNAPSHOT`: export a named snapshot before cancelling the job
  (Enterprise feature)

### ALTER JOB Synopsis

```sql
ALTER JOB job_name { SUSPEND | RESUME | RESTART }
```

Get more details on Jet job management in the [Operations
Guide](/docs/operations/job-management).

### SHOW JOBS

The command shows the names of all existing jobs, sorted by name:

```sql
SHOW JOBS
```

The list doesn't include jobs that finished (successfully or with an
error). On the other hand, it includes jobs submitted through the Java
API, if they have a name assigned.

## CREATE/DROP SNAPSHOT

To export a named snapshot for a running job use:

```sql
CREATE OR REPLACE SNAPSHOT snapshot_name FOR JOB job_name
```

- `OR REPLACE`: Jet replaces the snapshot if it already exists, this
  option is mandatory

- `snapshot_name`: the name of the saved snapshot

- `job_name`: the job for which to create the snapshot

The job will continue running after the snapshot is exported. To cancel
the job after the snapshot is exported, use the [DROP JOB .. WITH
SNAPSHOT](#drop-job-synopsis) command described above.

To delete a previously exported snapshot use:

```sql
DROP SNAPSHOT [IF EXISTS] snapshot_name
```

- `IF EXISTS`: don't throw an error if the snapshot doesn't exist

- `job_name`: the job for which to create the snapshot

To start a new job using the exported snapshot as the starting point,
use the [CREATE JOB](#create-job-synopsis) command with the
`initialSnapshotName` option set to the snapshot name.

*Note:* Exported snapshots are an Enterprise feature.
