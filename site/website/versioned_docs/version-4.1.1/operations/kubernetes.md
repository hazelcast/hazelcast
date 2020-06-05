---
title: Jet on Kubernetes
description: Using Kubernetes for orchestrating Jet clusters.
id: version-4.1.1-kubernetes
original_id: kubernetes
---

Hazelcast Jet has built-in support for Kubernetes deployments. It only
takes a few configuration parameters to make Hazelcast Jet cluster in
Kubernetes environments. Official Helm packages are also available to
bootstrap Hazelcast Jet deployments with a single command. Below are
the different ways to install Hazelcast Jet to a Kubernetes cluster.

- [Install using Helm](#install-using-helm)
- [Install using Operator](../how-tos/operator)
- [Install without any 3rd party tool](#install-without-any-3rd-party-tool)

## How it works

Hazelcast Jet shipped with Hazelcast Kubernetes Discovery plugin which
handles automatic member discovery in Kubernetes.

The plugin talks to Kubernetes API Server to resolve IP addresses of
the Hazelcast Jet pods.  

The plugin also supports member discovery using DNS resolution.

Each mode has its own advantages and drawbacks. You can find a
detailed comparison between two discovery methods in [this table](https://github.com/hazelcast/hazelcast-kubernetes#understanding-discovery-modes)
.

You might want to check out concrete details of the Kubernetes
discovery in the [relevant](discovery#kubernetes) section.

## Install using Helm

The easiest way to install Hazelcast Jet on Kubernetes is using Helm
charts, Hazelcast Jet provides stable Helm charts for open-source and
enterprise versions also for Hazelcast Jet Management Center.

### Prerequisites

- Kubernetes 1.9+
- Helm CLI

### Installing the Chart

You can install the latest version with default configuration values
using below command:

```bash
helm install my-cluster stable/hazelcast-jet
```

This will create a cluster with the name `my-cluster` and with default
configuration values. To change various configuration options you can
use `–set key=value`:

```bash
helm install my-cluster --set cluster.memberCount=3 stable/hazelcast-jet
```

Or you can create a `values.yaml` file which contains custom
configuration options. This file may contain custom `hazelcast` and
`hazelcast-jet` yaml files in it too.

```bash
helm install my-cluster -f values.yaml stable/hazelcast-jet
```

### Uninstalling the Chart

To uninstall/delete the `my-release` deployment:

```bash
helm uninstall my-release
```

The command removes all the Kubernetes components associated with the
chart and deletes the release.

### Configuration

The following table lists some  configurable parameters of the
Hazelcast Jet chart, and their default values.

| Parameter                  | Description                                                                    | Default                    |
|:---------------------------|:-------------------------------------------------------------------------------|:---------------------------|
| `image.repository`         | Hazelcast Jet Image name                                                       | `hazelcast/hazelcast-jet`  |
| `image.tag`                | Hazelcast Jet Image tag                                                        | {VERSION}                  |
| `cluster.memberCount`      | Number of Hazelcast Jet members                                                | 2                          |
| `jet.yaml.hazelcast-jet`   | Hazelcast Jet Configuration (`hazelcast-jet.yaml` embedded into `values.yaml`) | `{DEFAULT_JET_YAML}`       |
| `jet.yaml.hazelcast`       | Hazelcast IMDG Configuration (`hazelcast.yaml` embedded into `values.yaml`)    | `{DEFAULT_HAZELCAST_YAML}` |
| `managementcenter.enabled` | Turn on and off Hazelcast Jet Management Center application                    | `true`                     |

See
[stable charts repository](https://github.com/helm/charts/tree/master/stable/hazelcast-jet)
for more information and configuration options.

## Install without any 3rd party tool

Hazelcast Jet provides Kubernetes-ready Docker images, these images use
the Hazelcast Kubernetes plugin to discover other Hazelcast Jet members
by interacting with the Kubernetes APIs. See [relevant](discovery#kubernetes)
section for more details.

### Role Based Access Control

To communicate with the Kubernetes APIs, create the Role
Based Access Control definition, (`rbac.yaml`), with the following
content and apply it:

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

```bash
kubectl apply -f rbac.yaml
```

### ConfigMap

Then we need to configure Hazelcast Jet to use Kubernetes Discovery to
form the cluster. Create a file named `hazelcast-jet-config.yaml` with
following content and apply it. This will create a ConfigMap object.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: hazelcast-jet-configuration
data:
  hazelcast.yaml: |-
    hazelcast:
      network:
        join:
          multicast:
            enabled: false
          kubernetes:
            enabled: true
            namespace: default
            service-name: hazelcast-jet-service
        rest-api:
          enabled: true
          endpoint-groups:
            HEALTH_CHECK:
              enabled: true
```

```bash
kubectl apply -f hazelcast-jet-config.yaml
```

### StatefulSet and Service

Now we need to create a *StatefulSet* and a *Service* which defines the
container spec. You can configure the environment options and the
cluster size here. Create a file named `hazelcast-jet.yaml` with
following content and apply it.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: hazelcast-jet
  labels:
    app: hazelcast-jet
spec:
  replicas: 2
  serviceName: hazelcast-jet-service
  selector:
    matchLabels:
      app: hazelcast-jet
  template:
    metadata:
      labels:
        app: hazelcast-jet
    spec:
      containers:
      - name: hazelcast-jet
        image: hazelcast/hazelcast-jet:latest
        imagePullPolicy: IfNotPresent
        ports:
        - name: hazelcast-jet
          containerPort: 5701
        livenessProbe:
          httpGet:
            path: /hazelcast/health/node-state
            port: 5701
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          successThreshold: 1
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /hazelcast/health/node-state
            port: 5701
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 1
          successThreshold: 1
          failureThreshold: 1
        volumeMounts:
        - name: hazelcast-jet-storage
          mountPath: /data/hazelcast-jet
        env:
        - name: JAVA_OPTS
          value: "-Dhazelcast.config=/data/hazelcast-jet/hazelcast.yaml"
      volumes:
      - name: hazelcast-jet-storage
        configMap:
          name: hazelcast-jet-configuration
---
apiVersion: v1
kind: Service
metadata:
  name: hazelcast-jet-service
spec:
  selector:
    app: hazelcast-jet
  ports:
  - protocol: TCP
    port: 5701
```

```bash
kubectl apply -f hazelcast-jet.yaml
```

After deploying it, we can check the status of pods with the following
 command:

```bash
$ kubectl get pods
NAME              READY   STATUS    RESTARTS   AGE
hazelcast-jet-0   1/1     Running   0          2m23s
hazelcast-jet-1   1/1     Running   0          103s
```

Then we can verify from the logs of the pods that they formed a cluster
with the following command:

```log
$ kubectl logs hazelcast-jet-0
...
...
...
2020-03-05 10:02:44,698  INFO [c.h.i.c.ClusterService] [main]

Members {size:1, ver:1} [
 Member [172.17.0.6]:5701 - 03a22d3c-d88a-40bf-81b0-8f85e16acb0f this
]

2020-03-05 10:02:44,725  INFO [c.h.c.LifecycleService] [main] - [172.17.0.6]:5701 is STARTED
2020-03-05 10:03:20,387  INFO [c.h.i.n.t.TcpIpConnection] [hz.distracted_bartik.IO.thread-in-2] - [Initialized new cluster connection between /172.17.0.6:5701 and /172.17.0.7:49103
2020-03-05 10:03:27,381  INFO [c.h.i.c.ClusterService] [hz.distracted_bartik.priority-generic-operation.thread-0]

Members {size:2, ver:2} [
 Member [172.17.0.6]:5701 - 03a22d3c-d88a-40bf-81b0-8f85e16acb0f this
 Member [172.17.0.7]:5701 - a7295b91-939b-4181-acae-208145f773e6
]
```

## Deploying Jobs

To access this cluster inside Kubernetes environment from outside, we
need to do port forwarding. Port forwarding makes a port of a pod in the
remote Kubernetes cluster available to the local environment.

To submit a job, we'll do port forwarding from a Hazelcast Jet
pod on port 5701 to local port 5701. The command requires to
use a pod name to be forwarded.

Run the command below with to do port forwarding happen:

```bash
$ kubectl port-forward pod/hazelcast-jet-0 5701:5701
Forwarding from 127.0.0.1:5701 -> 5701
Forwarding from [::1]:5701 -> 5701
...
```

Download and unpack Hazelcast Jet distribution from [Jet
Website](https://jet-start.sh/download) to use sample job packaged with it.

In a different terminal window, unpack and navigate to the distribution
folder with the commands below:

```bash
tar xf hazelcast-jet-4.1.1.tar.gz
cd hazelcast-jet-4.1.1/
```

Verify that CLI can connect to the cluster with the command below:

```bash
$ bin/jet cluster
State: ACTIVE
Version: 4.1.1
Size: 2

ADDRESS                  UUID
[172.17.0.6]:5701        a720a93f-418a-4656-82a9-ae69eaeba99d
[172.17.0.7]:5701         5ac07dd4-e631-444e-a9ab-5e5faf1cd617
```

Then you can submit the example job from the distribution package with
the command below:

```bash
$ bin/jet submit examples/hello-world.jar
Submitting JAR 'examples/hello-world.jar' with arguments []
Top 10 random numbers in the latest window:
 1. 9,192,248,499,877,110,770
 2. 9,137,762,581,670,149,663
 3. 8,919,106,540,516,409,274
 4. 8,802,486,215,849,765,726
 5. 7,618,430,691,765,141,863
 6. 7,346,532,478,235,724,392
 7. 7,097,603,655,485,784,276
 8. 7,077,059,954,496,239,921
 9. 6,489,569,388,718,300,773
 10. 6,477,025,315,319,514,141
...
```

That's it, you've successfully deployed a job from your local machine to
the Hazelcast Jet cluster running inside Kubernetes
environment.

## Rolling Update and Scaling

Hazelcast Jet cluster is easily scalable within Kubernetes. You can use
the standard `kubectl scale` command to change the cluster size. The
same applies the rolling update procedure, you can depend on the
standard Kubernetes behavior and just update the new version to your
`Deployment/StatefulSet` configurations.

Note however that, by default, Hazelcast Jet does not shutdown
gracefully. It means that if you suddenly terminate more than your
backup-count property (1 by default), you may lose the cluster data. To
prevent that from happening, set the following properties:

- `terminationGracePeriodSeconds`: In your `StatefulSet/Deployment`
  configuration; the value should be high enough to cover the data
  migration process
- `-Dhazelcast.shutdownhook.policy=GRACEFUL`: In the JVM parameters
- `-Dhazelcast.graceful.shutdown.max.wait`: In the JVM parameters; the
  value should be high enough to cover the data migration process

Additionally if you use Deployment (not StatefulSet), you need to set
your strategy to
[RollingUpdate](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#updating-a-deployment)
and ensure Pods are updated one by one.

All these features included in Hazelcast Jet Helm Charts.
See [Install Hazelcast Jet using Helm](#install-using-helm)
for more information.
