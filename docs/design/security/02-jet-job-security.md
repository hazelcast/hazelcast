---
title: 021 - Job Security
description: Authorize Jobs in a secure environment
---

## Summary

Hazelcast provides fine-grained authorization mechanisms for
distributed data-structures. With merge to Hazelcast, we want to provide
this mechanism for Jet jobs too.

## Open-Source side Security

- Disable Jet: One can disable Jet engine completely using
`JetConfig#enable`. We don't create the internal Jet service and don't
start the threads. If a user tries to obtain `JetService`, we throw an
exception. 
- Disable Jet class/resource upload: Jet uploads the job resources and
classes along with the job. This is disabled by default and can be
enabled using `JetConfig:resourceUploadEnabled`. Since this config is
not available on the client side, when submitting the job we still
upload the resources/classes, but the job fails on the server side.

## Client Permissions

Hazelcast creates a security context on server-side and gate-guards each
client operation using configured permissions. These permissions are per
data-structure, MapPermission, ListPermission... There are also feature
related permissions, UserCodeDeploymentPermission, ConfigPermission,
ManagementPermission and TransactionPermission.

These permissions can have a name and various actions configured. For
example a MapPermission with the name `foo` and actions `create` and
`read`, permits the client to create a map with the name `foo` and make
read operations (like get, getAll, keySet etc.) on it.

### Job Permission

For Jet jobs, we created the type `JobPermission` to authorize the job
specific operations. Contrary to data-structure specific permissions,
JobPermission does not have a name. It can be configured with below
actions:

- create: submit a new job, without uploading resources.
- destroy: cancel a running job
- read: get the job (with the id or the name), get job-config, get
status, get submission time... The other actions imply `read` action,
for example if permission has `create` action, it means the permission
has `read` action as well.
- list: list the jobs
- suspend: suspend a running job
- resume: resume a suspended job
- export: export the snapshot
- upload: upload the resources/classes along with the jobs. This means
running custom code on the server, basically user can do anything with
this permission.
- all: all the above

### Connector Permission

Jet offers some OOTB connectors: file, socket, jms and jdbc connectors.
We want to guard these and possible others (extension module) hence the
connector permission. Instead of creating a separate permission for
each connector we created a single generic one which has the type of
the connector as a prefix in its name. The permission has following
actions:

- read: for sources
- write: for sinks
- all: all the above

Configuration options for different connectors:

**File Connector**:

```xml
<connector-permission name="file:directory_name">
    <actions>
        <action>all</action>
    </actions>
</connector-permission>
```

**Socket Connector**:

```xml
<connector-permission name="socket:host:port">
    <actions>
        <action>all</action>
    </actions>
</connector-permission>
```

**JMS Connector**: the jms connector which is not configured with an
explicit destination name but with a consumer function requires a
permission with the name `jms:`

```xml
<connector-permission name="jms:destination_name">
    <actions>
        <action>all</action>
    </actions>
</connector-permission>
```

**JDBC Connector** the jdbc connector which is not configured with an
explicit connection url but with a data source supplier requires a
permission with the name `jdbc:`

```xml
<connector-permission name="jdbc:connection_url">
    <actions>
        <action>all</action>
    </actions>
</connector-permission>
```

### Hazelcast Connector Permissions

A job can bypass the configured permissions for Hazelcast 
data-structures like map, cache, list and ringbuffer using the OOTB
Hazelcast connectors. User should add the necessary permissions to be
able to include these connectors to the pipeline.

```java
Pipeline p = Pipeline.create();

p.readFrom(Sources.map("source_map"))
 .writeTo(Sinks.map("sink_map"));
```

For the above pipeline, user should configure below permissions for
Hazelcast connectors:

```xml
<map-permission name="source_map">
    <actions>
        <action>create</action>
        <action>read</action>
    </actions>
</map-permission>
<map-permission name="source_map">
    <actions>
        <action>create</action>
        <action>write</action>
    </actions>
</map-permission>
```

In addition to `read`/`write` actions, user should also configure
`create` action because these connectors may create data structures if
not created already.

## Implementation

To apply the job permissions, we've implemented the
`SecureRequest#getRequiredPermission` for each job related message task.
This secured the DAG/Pipeline jobs, but the jobs originated from SQL
are submitted from server.

SQL has already a mechanism to apply some security checks, these checks
are limited to map/cache read/write permissions. We've extended them to
apply the necessary job permissions as well as other connector
permissions.

We've added a `#getRequiredPermission` method to `Processor`, 
`ProcessorSupplier` and `ProcessorMetaSupplier` interfaces. We get the
required permissions from PMS, but some connectors don't have a
concrete PMS implementations but use PS or `Supplier<Processor>` to
create it, for example `MetaSupplierFromProcessorSupplier` and 
`ProcessorSupplierFromSimpleSupplier`. For those cases, the
implementation uses the supplied PS or the Processor to obtain the
required permission.

Before submitting a job, from the vertices of the DAG we obtain the
PMSs and from them the required permissions. For each permission we ask
the security context if the endpoint has the required permissions.

### Upload Permission

Jet jobs, by design, uploads the custom code written by the user to the
server and runs it on the server side. User can do whatever he wants in
this code. For example, user can get the member instance using
`Hazelcast#getAllHazelcastInstances` and update a map bypassing the
permission mechanism.
