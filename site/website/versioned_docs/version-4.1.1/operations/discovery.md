---
title: Cluster Discovery
description: How to configure Jet for cluster discovery on various deployments.
id: version-4.1.1-discovery
original_id: discovery
---

This section describes how Hazelcast Jet members forms a cluster in
different platforms.

Please note that, after a cluster is formed, communication between
cluster members is always via TCP/IP, regardless of the discovery
mechanism used.

The discovery mechanism must be configured before node startup and
the options are [configured](configuration) in `hazelcast.yaml`.

Hazelcast Jet supports the following discovery mechanisms.

## Multicast

With the multicast auto-discovery mechanism, Hazelcast Jet allows cluster
members to find each other using multicast communication. The cluster
members do not need to know the concrete addresses of the other members,
as they just multicast to all the other members. Whether
multicast is possible or allowed depends on your environment.

To configure your Hazelcast Jet member for multicast auto-discovery, set
the following configuration elements.

- Set the `enabled` element of the multicast element to `true`.

- Set `multicast-group`, `multicast-port`, `multicast-time-to-live`,
 `multicast-timeout-seconds`, `trusted-interfaces` etc. to your
 multicast values.

The following is an example configuration in `hazelcast.yaml`:

```yaml
hazelcast:
  network:
    join:
      multicast:
        enabled: true
        multicast-group: 224.2.2.3
        multicast-port: 54327
      tcp-ip:
        enabled: false
```

> Multicast mechanism is not recommended for production since UDP is
> often blocked in production environments and other discovery
> mechanisms are more definite.

## TCP/IP

Hazelcast Jet can be configured to discover members by TCP/IP. The
configuration must list all or a subset of the members' host names
and/or IP addresses as cluster members. You do not have to list all of
these cluster members, but at least one of the listed members has to be
active in the cluster when a new member joins.

To configure your Hazelcast Jet cluster with TCP/IP discovery, set the
following configuration elements.

- Set the `enabled` element of the `tcp-ip` element to true.
- Provide your member elements within the `member-list` element.

The following is an example configuration.

```yaml
hazelcast:
  network:
    join:
      multicast:
        enabled: false
      tcp-ip:
        enabled: true
        member-list:
          - 10.0.0.1
          - 10.0.0.2
```

## Amazon Web Services(EC2)

Hazelcast Jet supports EC2 auto-discovery with the
Hazelcast Discovery Plugin for AWS. The plugin is included in the main
Hazelcast Jet distribution so no extra dependencies needs to be added to
use it.

To use AWS discovery plugin, disable other join mechanisms
and enable `aws`.

The configuration below will filter the instances in the configured
 region with security-group and the tag and will try to form a cluster.

```yaml
hazelcast:
  network:
    join:
      multicast:
        enabled: false
      tcp-ip:
        enabled: false
      aws:
        enabled: true
        access-key: my-access-key
        secret-key: my-secret-key
        # optional, default is us-east-1
        region: us-west-1
        # optional, only instances belonging to this group will be discovered, default will try all running instances
        security-group-name: hazelcast-sg
        tag-key: type
        tag-value: hz-nodes

```

For more information on the discovery plugin regarding Zone Awareness
configuration, IAM roles, AWS Autoscaling and client connections from
outside of the AWS network please see [Hazelcast Discovery Plugin for AWS](https://github.com/hazelcast/hazelcast-aws)
.

## Google Cloud Platform

Hazelcast Jet supports automatic member discovery in the Google Cloud
Platform (Compute Engine) environment with the Hazelcast Discovery
Plugin for GCP. The plugin is included in the main Hazelcast Jet
distribution so no extra dependencies needs to be added to use it.

To use GCP discovery plugin, disable other join mechanisms
and enable `gcp`.

```yaml
hazelcast:
  network:
    join:
      multicast:
        enabled: false
      tcp-ip:
        enabled: false
      gcp:
        enabled: true
        zones: us-east1-a,us-east1-b
        label: application=hazelcast
        hz-port: 5701-5708
```

For more information on the discovery plugin regarding Zone Awareness and
client connections from outside of the AWS network please see
[Hazelcast Discovery Plugin for GCP](https://github.com/hazelcast/hazelcast-gcp)
.

## Azure Cloud

Hazelcast Jet supports automatic member discovery in the Microsoft
Azure environment with [Hazelcast Discovery Plugin for Microsoft Azure](https://github.com/hazelcast/hazelcast-azure).
The plugin is included in the main Hazelcast Jet distribution so no
extra dependencies needs to be added to use it.

To use Azure discovery plugin, disable other join  mechanisms and
enable `azure`.

```yaml
hazelcast:
  network:
    join:
      multicast:
        enabled: false
      azure:
        enabled: true
        tag: TAG-NAME=HZLCAST001
        hz-port: 5701-5703
```

Hazelcast Azure Plugin uses [Azure Instance Metadata Service](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/instance-metadata-service)
to get access token and other environment details. In order to use this
service, the plugin requires that [Azure managed identities](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview)
with the correct `READ` roles are setup. The other necessary
information such as subscription ID and and resource group name will be
retrieved from instance metadata service.

### Clients from outside Azure

Client instances might be running outside of an Azure VM which makes
Azure Instance Metadata service unavailable. Then, client instances
should be configured with the properties as shown below:

```yaml
hazelcast-client:
  network:
      azure:
        enabled: true
        instance-metadata-available: false
        client-id: CLIENT_ID
        tenant-id: TENANT_ID
        client-secret: CLIENT_SECRET
        subscription-id: SUB_ID
        resource-group: RESOURCE-GROUP-NAME
        scale-set: SCALE-SET-NAME
        use-public-ip: true
```

You will need to setup [Azure Active Directory Service Principal credentials](https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/)
for your Azure Subscription for this plugin to work. With the
credentials, fill in the placeholder values above.

For more information on the discovery plugin regarding Zone
Awareness and client connections from outside of the Azure network
please see [Hazelcast Discovery Plugin for Microsoft Azure](https://github.com/hazelcast/hazelcast-azure).

## Kubernetes

The Hazelcast Kubernetes plugin provides the automatic member discovery
in the Kubernetes environment by communicating with the Kubernetes
Master. The plugin included in the main
Hazelcast Jet distribution so no extra dependencies needs to be added to
use it.

This plugin supports two different options of how Hazelcast Jet members
discover each other:

- Kubernetes API
- DNS Lookup

### Kubernetes API

*Kubernetes API* mode means that each node makes a REST call to
Kubernetes Master in order to discover IPs of Pods (with Hazelcast Jet
members). Using Kubernetes API requires granting certain permissions.
Therefore, you may need to create a *Role Based Access Control*(`rbac.yaml`)
file with the following content.

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: default-cluster
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: view
subjects:
- kind: ServiceAccount
  name: default
  namespace: default
```

Then, apply `rbac.yaml`.

```bash
kubectl apply -f rbac.yaml
```

Hazelcast Kubernetes Discovery requires creating a service to Pods where
Hazelcast Jet is running. In case of using Kubernetes API mode, the
service can be of any type.

The Hazelcast Jet configuration to use Kubernetes Discovery with
Kubernetes API mode looks like the following.

```yaml
hazelcast:
  network:
    join:
      multicast:
        enabled: false
      kubernetes:
        enabled: true
        namespace: MY-KUBERNETES-NAMESPACE
        service-name: MY-SERVICE-NAME
```

### DNS Lookup

*DNS Lookup* mode uses a feature of Kubernetes that **headless**
(without cluster IP) services are assigned a DNS record which resolves
to the set of IPs of related Pods.

Headless service is a service of type *ClusterIP* with the `clusterIP`
property set to `None`.

The Hazelcast Jet configuration to use Kubernetes Discovery with DNS
Lookup mode looks like the following.

```yaml
hazelcast:
  network:
    join:
      multicast:
        enabled: false
      kubernetes:
        enabled: true
        service-dns: MY-SERVICE-DNS-NAME
```

See [Hazelcast Discovery Plugin for Kubernetes](https://github.com/hazelcast/hazelcast-kubernetes)
for more information about the plugin.
