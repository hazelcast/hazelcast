---
title: 021 - Job Security
description: Authorize jobs in a secure environment
---

## Summary

Hazelcast provides fine-grained authorization mechanisms for distributed
data structures. After merge with Hazelcast IMDG, we want to provide
this mechanism for Jet jobs too.

## Security in the Open Source Part

- Disable Jet: One can disable Jet engine completely using
`JetConfig#enable`. We don't create the internal Jet service and don't
start the threads. If a user tries to obtain `JetService`, we throw an
exception. Jet is disabled by default.
- Disable Jet class/resource upload: Jet uploads the job resources and
classes along with the job. This is disabled by default and can be
enabled using `JetConfig:resourceUploadEnabled`. Since this config is
not available on the client side, when submitting the job we still
upload the resources/classes, but the job fails on the server side.

## Client Permissions

Hazelcast creates a security context on server side and gate-guards each
client operation using configured permissions. These permissions are per
data structure, MapPermission, ListPermission... There are also
feature-related permissions, UserCodeDeploymentPermission,
ConfigPermission, ManagementPermission and TransactionPermission.

These permissions can have a name and various actions configured. For
example a MapPermission with the name `foo` and actions `create` and
`read`, permits the client to create a map with the name `foo` and make
read operations (like get, getAll, keySet etc.) on it.

### Job Permission

For Jet jobs, we created the type `JobPermission` to authorize the
job-specific operations. Contrary to data structure-specific
permissions, JobPermission does not have a name. It can be configured
with actions below:

- submit: submit a new job, without uploading resources
- cancel: cancel a running job
- read: all the readonly actions related to job like list the jobs, get
the job (by id or name), get job config, get status, submission time...
Every other action implies `read` action, for example if permission has
`create` action, it means the permission has `read` action as well.
- restart: restart/suspend/resume a job
- export-snapshot: export the snapshot
- add-resources: upload the resources/classes along with jobs. This
means running custom code on the server, basically user can do anything
with this permission.
- all: all of the above

### Connector Permission

Jet offers some OOTB connectors: file, socket, jms and jdbc connectors.
We want to guard these and possibly others (extension module), hence the
connector permission. Instead of creating a separate permission for
each connector we created a single generic one which has the type of
the connector as a prefix in its name. The permission has the following
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

A job can bypass the configured permissions for Hazelcast data
structures like IMap, ICache, IList and RingBuffer using the OOTB
Hazelcast connectors. User should add the necessary permissions to be
able to include these connectors in the pipeline.

```java
Pipeline p = Pipeline.create();

p.readFrom(Sources.map("source_map"))
 .writeTo(Sinks.map("sink_map"));
```

For the above pipeline, user must configure the permissions below for
Hazelcast connectors:

```xml
<map-permission name="source_map">
    <actions>
        <action>create</action>
        <action>read</action>
    </actions>
</map-permission>
<map-permission name="sink_map">
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

To enforce the job permissions, we've implemented the
`SecureRequest#getRequiredPermission` for each job-related message task.
This secured the DAG/Pipeline jobs, but jobs originated from SQL are
submitted from server.

SQL already has a mechanism to apply security checks, these checks
are limited to map/cache read/write permissions. We've extended them to
apply the necessary job permissions as well as other connector
permissions.

We've added a `#getRequiredPermission` method to `ProcessorMetaSupplier`
interface. Before submitting a job, from the vertices of the DAG we
obtain the PMSs and from them the required permissions. For each
permission we ask the security context if the endpoint has the required
permissions.

### SecuredFunction

There are some lambdas on the classpath which can be used to gain
access to data-structures. For example:

```java
context -> context.hazelcastInstance().getMap(name);
```

We've introduced below interface which returns a list of permissions
required to execute the function.
```java
@PrivateApi
public interface SecuredFunction {

    /**
     * @return the list of permissions required to run this function
     */
    @Nullable
    default List<Permission> permissions() {
        return null;
    }
}
```

These permissions are checked at the initialization phase of the job,
the permissions of the `ProcessorMetaSupplier` are checked at the
submit phase of the job.

You can find these functions in `SecuredFunctions` utility class.
Any lambda we provide in the distribution which accesses a restricted
data-structure should implement this interface. Permissions returned
from this function should not be a field of an object which serialized
and sent to the server. Instead, it should be hardcoded so that it
cannot be manipulated using reflections.

### Internal Jet Data Structures

Jet uses some internal data structures, `FlakeIdGenerator` for creating
job ids and `IMap` for resources and snapshots. We implicitly configure
permissions for these data structures if user has relevant permissions.

- If user has `submit` action, we add `FlakeIdGenerator` permission with
the name `__jet.ids`. 
 
- If user has `add-resources` action, we add `IMap` permission with the
name `__jet.resources*`.

- If user has `export-snapshot` action, we add `IMap` permission with
the name `__jet.exportedSnapshot*`.

### Observable

`Observable` uses ring-buffer under the hood by adding a prefix to the
defined name, `__jet.observables.`. We strip this prefix when checking
for the permission so that user can define a ring-buffer permission
with the same name as observable. For example, in order to user an
observable with the name `foo`, user should have below permission:

```xml
<ring-buffer-permission name="foo">
    <actions>
        <action>all</action>
    </actions>
</ring-buffer-permission>
```

There is also an option to create an observable without a name, which
in fact creates a random name for the observable. If user wants to use
this API in a secured environment, the user should configure the
ring-buffer permission with a wildcard.

```xml
<ring-buffer-permission name="*">
    <actions>
        <action>all</action>
    </actions>
</ring-buffer-permission>
```

### Add Resources Permission

Jet jobs, by design, upload the custom code written by the user to the
server and run it on the server side. The user can do whatever he wants
in this code. For example, he can get the member instance using
`Hazelcast#getAllHazelcastInstances` and update a map bypassing the
permission mechanism.
