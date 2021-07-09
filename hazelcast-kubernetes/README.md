# Hazelcast Discovery Plugin for Kubernetes

This repository contains a plugin which provides the automatic Hazelcast member discovery in the Kubernetes environment.

You can use it in your project deployed on Kubernetes in order to make the embedded Hazelcast members discover each other automatically. This plugin is also included in Hazelcast Docker images, Hazelcast Helm Charts, and Hazelcast OpenShift Docker image.

## Requirements and Recommendations

* Your Java Runtime Environment must support TLS 1.2 (which is the case for most modern JREs).
* Versions compatibility:
  * hazelcast-kubernetes 2.0+ is compatible with hazelcast 4+
  * hazelcast-kubernetes 1.3+ is compatible with hazelcast 3.11.x, 3.12.x
  * for older hazelcast versions you need to use hazelcast-kubernetes 1.2.x.
* The recommendation is to use StatefulSet for managing Hazelcast PODs; in case of using Deployment (or ReplicationController), the Hazelcast cluster may start with Split Brain (which will anyway re-form to one consistent cluster in a few minutes).

## Embedded mode

To use Hazelcast embedded in your application, you need to add the plugin dependency into your Maven/Gradle file. 
Then, when you provide `hazelcast.yaml` as presented below or an equivalent Java-based configuration, your Hazelcast 
instances discover themselves automatically.

#### Maven

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast-kubernetes</artifactId>
  <version>${hazelcast-kubernetes-version}</version>
</dependency>
```

#### Gradle

```groovy
compile group: "com.hazelcast", name: "hazelcast-kubernetes", version: "${hazelcast-kubernetes-version}"
```

## Understanding Discovery Modes

The following table summarizes the differences between the discovery modes: **Kubernetes API** and **DNS Lookup**.

|                | Kubernetes API  | DNS Lookup |
| -------------  | ------------- | ------------- |
| Description    | Uses REST calls to Kubernetes Master to fetch IPs of PODs | Uses DNS to resolve IPs of PODs related to the given service |
| Pros           | Flexible, supports **3 different options**: <br> - Hazelcast cluster per service<br> - Hazelcast cluster per multiple services (distinguished by labels)<br> - Hazelcast cluster per namespace | **No additional configuration** required, resolving DNS does not require granting any permissions  |
| Cons           | Requires setting up **RoleBinding** (to allow access to Kubernetes API)  | - Limited to **headless Cluster IP** service<br> - Limited to **Hazelcast cluster per service**  |

## Configuration

This plugin supports **two different options** of how Hazelcast members discover each others:
* Kubernetes API
* DNS Lookup

### Kubernetes API

**Kubernetes API** mode means that each node makes a REST call to Kubernetes Master in order to discover IPs of PODs (with Hazelcast members).

#### Granting Permissions to use Kubernetes API

Using Kubernetes API requires granting certain permissions. To grant them for 'default' service account in 'default' namespace execute the following command.

    kubectl apply -f https://raw.githubusercontent.com/hazelcast/hazelcast-kubernetes/master/rbac.yaml

#### Creating Service

Hazelcast Kubernetes Discovery requires creating a service to PODs where Hazelcast is running. In case of using Kubernetes API mode, the service can be of any type.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: MY-SERVICE-NAME
spec:
  type: LoadBalancer
  selector:
    app: APP-NAME
  ports:
  - name: hazelcast
    port: 5701
```

#### Hazelcast Configuration

The second step is to configure the discovery plugin inside `hazelcast.yaml` or an equivalent Java-based configuration.

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

```java
config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
config.getNetworkConfig().getJoin().getKubernetesConfig().setEnabled(true)
      .setProperty("namespace", "MY-KUBERNETES-NAMESPACE")
      .setProperty("service-name", "MY-SERVICE-NAME");
```

There are several properties to configure the plugin, all of them are optional.
 * `namespace`: Kubernetes Namespace where Hazelcast is running; if not specified, the value is taken from the environment variables `KUBERNETES_NAMESPACE` or `OPENSHIFT_BUILD_NAMESPACE`. If those are not set, the namespace of the POD will be used (retrieved from `/var/run/secrets/kubernetes.io/serviceaccount/namespace`).
 * `service-name`: service name used to scan only PODs connected to the given service; if not specified, then all PODs in the namespace are checked
 * `service-label-name`, `service-label-value`: service label and value used to tag services that should form the Hazelcast cluster together
 * `pod-label-name`, `pod-label-value`: pod label and value used to tag pods that should form the Hazelcast cluster together
 * `resolve-not-ready-addresses`: if set to `true`, it checks also the addresses of PODs which are not ready; `true` by default
 * `use-node-name-as-external-address`: if set to `true`, uses the node name to connect to a `NodePort` service instead of looking up the external IP using the API; `false` by default
 * `kubernetes-api-retries`: number of retries in case of issues while connecting to Kubernetes API; defaults to `3` 
 * `kubernetes-master`: URL of Kubernetes Master; `https://kubernetes.default.svc` by default
 * `api-token`: API Token to Kubernetes API; if not specified, the value is taken from the file `/var/run/secrets/kubernetes.io/serviceaccount/token`
 * `ca-certificate`: CA Certificate for Kubernetes API; if not specified, the value is taken from the file `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`
 * `service-port`: endpoint port of the service; if specified with a value greater than `0`, it overrides the default; `0` by default
 
You can use one of `service-name`,`service-label`(`service-label-name`, `service-label-value`) and `pod-label`(`pod-label-name`, `pod-label-value`) based discovery mechanisms, configuring two of them at once does not make sense.

*Note*: If you don't specify any property at all, then the Hazelcast cluster is formed using all PODs in your current namespace. In other words, you can look at the properties as a grouping feature if you want to have multiple Hazelcast clusters in one namespace.

### DNS Lookup

**DNS Lookup** mode uses a feature of Kubernetes that **headless** (without cluster IP) services are assigned a DNS record which resolves to the set of IPs of related PODs.

#### Creating Headless Service

Headless service is a service of type `ClusterIP` with the `clusterIP` property set to `None`.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: MY-SERVICE-NAME
spec:
  type: ClusterIP
  clusterIP: None
  selector:
    app: APP-NAME
  ports:
  - name: hazelcast
    port: 5701
```

#### Hazelcast Configuration

The Hazelcast configuration to use DNS Lookup looks as follows.

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

```java
config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
config.getNetworkConfig().getJoin().getKubernetesConfig().setEnabled(true)
      .setProperty("service-dns", "MY-SERVICE-DNS-NAME");
```

There are 3 properties to configure the plugin:
 * `service-dns` (required): service DNS, usually in the form of `SERVICE-NAME.NAMESPACE.svc.cluster.local`
 * `service-dns-timeout` (optional): custom time for how long the DNS Lookup is checked
 * `service-port` (optional): the Hazelcast port; if specified with a value greater than 0, it overrides the default (default port = `5701`)

**Note**: In this README, only YAML configurations are presented, however you can achieve exactly the same effect using 
XML or Java-based configurations.

## High Availability

### Zone Aware

When using `ZONE_AWARE` configuration, backups are created in the other availability zone. This feature is available only for the Kubernetes API mode.

**Note**: Your Kubernetes cluster must orchestrate Hazelcast Member PODs equally between the availability zones, otherwise Zone Aware feature may not work correctly.

#### YAML Configuration

```yaml
partition-group:
  enabled: true
  group-type: ZONE_AWARE
```

#### Java-based Configuration

```java
config.getPartitionGroupConfig()
    .setEnabled(true)
    .setGroupType(MemberGroupType.ZONE_AWARE);
```

Note the following aspects of `ZONE_AWARE`:
 * Kubernetes cluster must provide the [well-known Kubernetes annotations](https://kubernetes.io/docs/reference/kubernetes-api/labels-annotations-taints/#failure-domainbetakubernetesiozone)
 * Retrieving Zone Name uses Kubernetes API, so RBAC must be configured as described [here](#granting-permissions-to-use-kubernetes-api)
 * `ZONE_AWARE` feature works correctly when Hazelcast members are distributed equally in all zones, so your Kubernetes cluster must orchestrate PODs equally
 
 Note also that retrieving Zone Name assumes that your container's hostname is the same as POD Name, which is almost always true. If you happen to change your hostname in the container, then please define the following environment variable:
 
 ```yaml
 
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
 ``` 

### Node Aware

When using `NODE_AWARE` configuration, backups are created in the other Kubernetes nodes. This feature is available only for the Kubernetes API mode.

**Note**: Your Kubernetes cluster must orchestrate Hazelcast Member PODs equally between the nodes, otherwise Node Aware feature may not work correctly.

#### YAML Configuration

```yaml
partition-group:
  enabled: true
  group-type: NODE_AWARE
```

#### Java-based Configuration

```java
config.getPartitionGroupConfig()
    .setEnabled(true)
    .setGroupType(MemberGroupType.NODE_AWARE);
```

Note the following aspects of `NODE_AWARE`:
 * Retrieving name of the node uses Kubernetes API, so RBAC must be configured as described [here](#granting-permissions-to-use-kubernetes-api)
 * `NODE_AWARE` feature works correctly when Hazelcast members are distributed equally in all nodes, so your Kubernetes cluster must orchestrate PODs equally.
 
Note also that retrieving name of the node assumes that your container's hostname is the same as POD Name, which is almost always true. If you happen to change your hostname in the container, then please define the following environment variable:
 
 ```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
 ``` 

## Hazelcast Client Configuration

Depending on whether your Hazelcast Client runs inside or outside the Kubernetes cluster, its configuration looks different.

### Inside Kubernetes Cluster

If you have a Hazelcast cluster deployed on Kubernetes and you want to configure Hazelcast Client deployed on the same Kubernetes cluster, then the configuration looks very similar to Hazelcast Member.

Here's an example in case of the **Kubernetes API** mode.

```yaml
hazelcast-client:
  network:
    kubernetes:
      enabled: true
      namespace: MY-KUBERNETES-NAMESPACE
      service-name: MY-SERVICE-NAME
```

```java
clientConfig.getNetworkConfig().getKubernetesConfig().setEnabled(true)
            .setProperty("namespace", "MY-KUBERNETES-NAMESPACE")
            .setProperty("service-name", "MY-SERVICE-NAME");
```

### Outside Kubernetes Cluster

If your Hazelcast cluster is deployed on Kubernetes, but Hazelcast Client is in a completely different network, then they can connect only through the public Internet network. This requires a special configuration of both Hazelcast server and Hazelcast client. You can find a complete guide on how to set it up here: [How to Set Up Your Own On-Premises Hazelcast on Kubernetes](https://hazelcast.com/blog/how-to-set-up-your-own-on-premises-hazelcast-on-kubernetes/).

To use **Hazelcast Dummy Client** (`<smart-routing>false</smart-routing>`) you don't need any plugin, it's enough to expose your Hazelcast cluster with a LoadBalancer (or NodePort) service and set its IP/port as the TCP/IP Hazelcast Client configuration. Dummy Client, however, compromises the performance, since all the communication happens against a single Hazelcast member.

To configure **Hazelcast Smart Client**, you need to perform the following steps:
* Expose each Hazelcast Member POD with a separate LoadBalancer or NodePort service (the simplest way to do it is to install [Metacontroller](https://metacontroller.app/) and [service-per-pod](https://github.com/GoogleCloudPlatform/metacontroller/tree/master/examples/service-per-pod) Decorator Controller)
* Configure ServiceAccount with ClusterRole having at least `get` and `list` permissions to the following resources: `endpoints`, `pods`, `nodes`, `services`
* Use credentials from the created ServiceAccount in the Hazelcast Client configuration (credentials can be fetched with `kubectl get secret <sevice-account-secret> -o jsonpath={.data.token} | base64 --decode` and `kubectl get secret <sevice-account-secret> -o jsonpath={.data.ca\\.crt} | base64 --decode`)

```yaml
hazelcast-client:
  network:
    kubernetes:
      enabled: true
      namespace: MY-KUBERNETES-NAMESPACE
      service-name: MY-SERVICE-NAME
      use-public-ip: true
      kubernetes-master: https://35.226.182.228
      api-token: eyJhbGciOiJSUzI1NiIsImtpZCI6IiJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJkZWZhdWx0Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6InNhbXBsZS1zZXJ2aWNlLWFjY291bnQtdG9rZW4tNnM5NGgiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoic2FtcGxlLXNlcnZpY2UtYWNjb3VudCIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6IjI5OTI1NzBmLTI1NDQtMTFlOS1iNjg3LTQyMDEwYTgwMDI4YiIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDpkZWZhdWx0OnNhbXBsZS1zZXJ2aWNlLWFjY291bnQifQ.o-j4e-ducrMmQc23xYDnPr6TIyzlAs3pLNAmGLqPe9Vq1mwsxOh3ujcVKR90HAdkfHIF_Sw66qC9hXIDvxfqN_rLXlOKbvTX3gjDrAnyY_93Y3MpmSBj8yR9yHMb4O29a9UIwN5F2_VoCsc0IGumScU_EhPYc9mvEXlwp2bATQOEU-SVAGYPqvVPs9h5wjWZ7WUQa_-RBLMF6KRc9EP2i3c7dPSRVL9ZQ6k6OyUUOVEaPa1tqIxP7vOgx9Tg2C1KmYF5RDrlzrWkhEcjd4BLTiYDKEyaoBff9RqdPYlPwu0YcEH-F7yU8tTDN74KX5jvah3amg_zTiXeNoe5ZFcVdg
      ca-certificate: |
        -----BEGIN CERTIFICATE-----
        MIIDCzCCAfOgAwIBAgIQVcTHv3jK6g1l7Ph9Xyd9DTANBgkqhkiG9w0BAQsFADAv
        MS0wKwYDVQQDEyQ4YjRhNjgwMS04NzJhLTQ2NDEtYjIwOC0zYjEyNDEwYWVkMTcw
        HhcNMTkwMTMxMDcyNDMxWhcNMjQwMTMwMDgyNDMxWjAvMS0wKwYDVQQDEyQ4YjRh
        NjgwMS04NzJhLTQ2NDEtYjIwOC0zYjEyNDEwYWVkMTcwggEiMA0GCSqGSIb3DQEB
        AQUAA4IBDwAwggEKAoIBAQCaty8l9aHeWE1r9yLWKJMa3YQotVclYoEHegB8y6Ke
        +zKqa06JKKrz3Qony97VdWR/NMpRYXouSF0owDv9BIoLTC682wlQtNB1c4pTVW7a
        AikoNtyNIT8gtA5w0MyjFrbNslUblXvuo0HIeSmJREUmT7BC3VaKgkg64mVdf0DJ
        NyrcL+qyCs1m03mi12hgzI72O3qgEtP91tu/oCUdOh39u13TB0fj5tgWURMFgkxo
        T0xiNfPueV3pe8uYxBntzFn/74ibiizLRP6d/hsuRdS7IA+bvRLKG/paYwyZuMFb
        BDA+kXXAIkOvCpIQCkAKMpyyDz9lBVCtl3eRSAJQLBefAgMBAAGjIzAhMA4GA1Ud
        DwEB/wQEAwICBDAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBP
        TBRY1IbkFJuboMKLW9tdpIzW7hf2qsTLOhtlaJbMXrWaXCTrl8qUgBUZ1sWAW9Uk
        qETwRoCMl1Ht7PhbnXEGDNt3Sw3Y3feR4PsffhcgWH0BK8pZVY0Q1zbZ6dVNbU82
        EUrrcnV0uiB/JFsJ3rg8qJurutro3uIzAhb9ixYRqYnXUR4q0bxahO04iSUHvtYQ
        JmWp1GCb/ny9MyeTkwh2Q+WIQBHsX4LfrKjPwJd6qZME7BmwryYBTkGa0FinmhRg
        SdSPEQKmuXmghPU5GLudiI2ooOaqOXIjVPfM/cw4uU9FCGM49qufccOOt6utk0SM
        DwupAKLLiaYs47a8JgUa
        -----END CERTIFICATE-----
```

**Note:** Hazelcast Client outside Kubernetes cluster works only in the **Kubernetes API** mode (it does not work in the **DNS Lookup** mode).

**Note:** If you use Minikube, you need to execute `minikube tunnel` in order to get LoadBalancer External IPs assigned.

## Rolling Upgrade and Scaling

Hazelcast cluster is easily scalable within Kubernetes. You can use the standard `kubectl scale` command to change the cluster size. The same applies the rolling upgrade procedure, you can depend on the standard Kubernetes behavior and just update the new version to your Deployment/StatefulSet configurations.

Note however that, by default, Hazelcast does not shutdown gracefully. It means that if you suddenly terminate more than your `backup-count` property (1 by default), you may lose the cluster data. To prevent that from happening, set the following properties:
- `terminationGracePeriodSeconds`:  in your StatefulSet (or Deployment) configuration; the value should be high enough to cover the data migration process
- `-Dhazelcast.shutdownhook.policy=GRACEFUL`: in the JVM parameters
- `-Dhazelcast.graceful.shutdown.max.wait`: in the JVM parameters; the value should be high enough to cover the data migration process

Additionally:
- If you use Deployment (not StatefulSet), you need to set your strategy to [RollingUpdate](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#updating-a-deployment) and ensure Pods are updated one by one.
- If you upgrade by the minor version, e.g., `3.11.4 => 3.12` (Enterprise feature), you need to set the following JVM property `-Dhazelcast.cluster.version.auto.upgrade.enabled=true` to make sure the cluster version updates automatically.

All these features are already included in [Hazelcast Helm Charts](#helm-chart).

## CP Subsystem

Hazelcast CP Subsystem can be used safely in Kubernetes only if CP Subsystem Persistence is enabled (Enterprise Feature). Otherwise, a CP Subsystem Group may not recover after scaling the cluster or performing the rolling upgrade operation.

## Plugin Usages

Apart from embedding Hazelcast in your application as described above, there are multiple other scenarios of how to use the Hazelcast Kubernetes plugin.
You can also check [Hazelcast Guides: Embedded Hazelcast on Kubernetes](https://guides.hazelcast.org/kubernetes-embedded/) to see the plugin in use.

### Docker images

This plugin is included in the official Hazelcast Docker images:

 * [hazelcast/hazelcast](https://hub.docker.com/r/hazelcast/hazelcast/)
 * [hazelcast/hazelcast-enterprise](https://hub.docker.com/r/hazelcast/hazelcast-enterprise)
 
 Please check [Hazelcast Kubernetes Code Samples](https://github.com/hazelcast/hazelcast-code-samples/tree/master/hazelcast-integration/kubernetes) for the their usage.

### Helm Chart

Hazelcast is available in the form of Helm Chart in a few versions:

 * [stable/hazelcast](https://github.com/helm/charts/tree/master/stable/hazelcast) - Hazelcast IMDG in the official Helm Chart repository
 * [hazelcast/hazelcast](https://github.com/hazelcast/charts/tree/master/stable/hazelcast) - Hazelcast IMDG with Management Center
 * [hazelcast/hazelcast-enterprise](https://github.com/hazelcast/charts/tree/master/stable/hazelcast-enterprise) - Hazelcast Enterprise with Management Center
 * [IBM/hazelcast-enterprise](https://github.com/IBM/charts/tree/master/community/hazelcast-enterprise) - Hazelcast Enterprise (with Management Center) dedicated for IBM ICP and IKS environments

You can also check [Hazelcast Helm Charts at Helm Hub](https://hub.helm.sh/charts?q=hazelcast).

### Kubernetes/OpenShift Operator

Hazelcast is available as Kubernetes/OpenShift Operator:

 * [hazelcast/hazelcast-operator](https://github.com/hazelcast/hazelcast-operator) - official Hazelcast Operator repository
 * [Operator Hub](https://operatorhub.io/operator/hazelcast-operator) - official Hazelcast Operator published at Operator Hub

### Red Hat OpenShift

The plugin is used to provide the OpenShift integration, please check [Hazelcast OpenShift Code Samples](https://github.com/hazelcast/hazelcast-code-samples/tree/master/hazelcast-integration/openshift) for details.

### Istio

Hazelcast works correctly with Istio >= 1.1.0 without any need of additional configuration. You can use it in both client-server and embedded modes. Also, the Hazelcast cluster can be either outside or inside the Istio mesh. Check [Hazelcast Guides: Hazelcast with Istio Service Mesh](https://guides.hazelcast.org/istio) for the details.
