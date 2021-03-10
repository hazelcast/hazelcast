---
title: Blue-Green Client
description: Configure Blue-Green Client with Hazelcast Jet
id: version-4.3.1-blue-green
original_id: blue-green
---

Blue/Green deployment refers to a software technique which can be used
to reduce system downtime by deploying two mirrored production
environments, referred to as _blue_ (active) and _green_ (idle). One of
the environments is running in production while the other is in active
standby.

The Jet client can be configured to switch to the other cluster in case
when a connection to the production environment fails.

## Configuration

To configure the client to use blue/green feature, you'll need to have a
file called `hazelcast-client-failover.yaml` which is a root config file
that references a client configuration file for each cluster. It can be
configured as follows:

```yaml
hazelcast-client-failover:
  try-count: 10
  clients:
    - hazelcast-client-clusterA.yaml
    - hazelcast-client-clusterB.yaml
```

When acquiring the Jet client instance, `Jet.newJetFailoverClient()`
should be used to create the client. Submitting jobs using the failover
client with `jet submit` command is not supported at this time.

## Switching Clusters

Once the client detects that one of the clusters is down, it will
automatically switch to the other one. After switching to cluster B, the
state of any running job on cluster A will be undefined. The jobs may
continue running on cluster A if the cluster is still alive and the
switching happened due to a network problem. If you try to query the
state of the job using the Job interface, you’ll get a
`JobNotFoundException`.

While you can use the Jet client for any IMDG operation, in Jet context
a client is mostly used for submitting jobs to the cluster. After the
submission the job lives in the cluster, client is used to query the
state of the job. Therefore Blue/Green deployment is not applicable for
long-lived jobs, but can be used with short-lived batch jobs which will
not cause any problem if run on different clusters at the same time,
like queries for example.

## Additional Configuration Options

Hazelcast Jet Client offers many configuration options for detecting
failures in the cluster You can also force a client to specifically
disconnect from one cluster and connect to the other one through
blacklisting. For a list of detailed features and options for Blue/Green
configuration options see the
[Hazelcast IMDG documentation on Blue-Green Deployment](https://docs.hazelcast.org/docs/4.0.3/manual/html-single/index.html#blue-green-deployment-and-disaster-recovery).
