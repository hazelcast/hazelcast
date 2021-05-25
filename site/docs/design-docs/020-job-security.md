---
title: 020 - Job Security
description: Authorize Jobs in a secure environment
---

## Summary

Hazelcast provides fine-grained authorization mechanisms for
distributed data-structures. With merge to platform, we want to provide
this mechanism for Jet jobs too.

## Open-Source side Security

- Disable Jet: One can disable Jet engine completely using
`JetConfig#enable`. We don't create the internal Jet service and don't
start the threads. If user tries to obtain `JetInstance` an exception
is thrown. 
- Disable Jet class/resource upload: Jet uploads the job resources and
classes along with the job. This is disabled by default and can be
enabled using `JetConfig:uploadResources`. Since this config is not
available on the client side, when submitting the job we still upload
the resources/classes but don't use them on the server side.

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
        status, get submission time... The other actions imply `read`
        action, for example if permission has `create` action, it means
        the permission has `read` action as well.
- list: list the jobs
- suspend: suspend a running job
- resume: resume a suspended job
- export: export the snapshot
- upload: upload the resources/classes along with the jobs
- all: all the above