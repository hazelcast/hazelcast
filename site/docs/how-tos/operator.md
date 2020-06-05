---
title: Install Kubernetes Operator 
description: A step-by-step guide on how to install Hazelcast Jet Kubernetes Operator on your Kubernetes Cluster.
---

The [Operator Framework](https://github.com/operator-framework) is an
open source toolkit to manage Kubernetes native applications, called
operators, in an effective, automated, and scalable way.

Hazelcast Jet provides an Operator to simplify deployment on Kubernetes
clusters. This is a step-by-step guide how to deploy Hazelcast Jet
cluster together with Management Center on your Kubernetes cluster.

## Prerequisites

- Kubernetes cluster (with admin rights) and the `kubectl` command
  configured (you may use [Minikube](https://kubernetes.io/docs/getting-started-guides/minikube/)
  )

## Installation

Hazelcast Jet Operator is published on [OperatorHub.io](https://operatorhub.io/)
, registry for Kubernetes Operators. It can be either installed via
Operator Lifecycle Manager or manually.

### Installing with Operator Lifecycle Manager(OLM)

Operator Lifecycle Manager helps you to install, update, and generally
manage the lifecycle of all of the operators (and their associated
services) running across your clusters.

#### Installing the OLM

You need to install the OLM to your cluster if it's not yet installed.

Check out the [latest releases on GitHub](https://github.com/operator-framework/operator-lifecycle-manager/releases)
for release-specific install instructions for OLM.

To install the OLM, Download the `install.sh` script from the Github
Releases page and execute it with the latest OLM version like below:

```bash
chmod +x install.sh
./install.sh <olm-version>
```

Then you can verify that the OLM is installed and running on your cluster
with the command below:

```bash
$ kubectl get pods -n olm
NAME                                READY   STATUS    RESTARTS   AGE
catalog-operator-66cf4f96f4-d9t4w   1/1     Running   0          1m
olm-operator-9b64f8547-cd67m        1/1     Running   0          1m
operatorhubio-catalog-7z5wh         1/1     Running   0          1m
packageserver-854879f5cf-49z64      1/1     Running   0          1m
packageserver-854879f5cf-r7kzz      1/1     Running   0          1m
```

#### Installing the Hazelcast Jet Operator

Install the Hazelcast Jet Operator by running the following command:

```bash
kubectl create -f https://operatorhub.io/install/hazelcast-jet-operator.yaml
```

Hazelcast Jet Operator will be installed in the `operators` namespace
and will be usable from all namespaces in the cluster.

After install, watch your operator come up using the following command.
It might take around a minute for installation to be succeed.

```bash
$ kubectl get csv -n operators
NAME                            DISPLAY                  VERSION   REPLACES   PHASE
hazelcast-jet-operator.v0.1.0   Hazelcast Jet Operator   0.1.0                Succeeded
```

### Manual Installation

If you'd like to manually install the Hazelcast Jet Operator without
installing the OLM, please follow the README on [Hazelcast Jet Operator](https://github.com/hazelcast/hazelcast-jet-operator/tree/master/hazelcast-jet-operator)
GitHub repository.

## Creating a Hazelcast Jet Cluster

After successfully installing the Hazelcast Jet Operator to your cluster
we can now create Hazelcast Jet clusters with the `HazelcastJet` custom
resource.

Create a file name `hazelcast-jet.yaml` with the contents below:

```yaml
apiVersion: hazelcast.com/v1alpha1
kind: HazelcastJet
metadata:
  name: jet-cluster
spec:
  cluster:
    memberCount: 2
  securityContext:
    runAsUser: ""
    runAsGroup: ""
    fsGroup: ""
```

**Note:** You can use [this
file](https://github.com/hazelcast/hazelcast-jet-operator/blob/master/hazelcast-jet-operator/hazelcast-jet-full.yaml)
for configuration options reference.

>### Setting the License Key for Management Center (Optional)
>
>If you do have an
>active license subscription for Hazelcast Jet Management Center, use
>the configuration below to start your cluster. You can
>apply for a 30-day trial if you don't have one from [Hazelcast Website](https://hazelcast.com/get-started/#deploymenttype-jet)
>. This step is optional and only for Hazelcast Jet Management Center.
> Hazelcast Jet doesn't require a license key. You can also skip the
> deployment of Management Center altogether by setting the `managementcenter.enabled`
> property to `false`.
>
>```yaml
>apiVersion: hazelcast.com/v1alpha1
>kind: HazelcastJet
>metadata:
>  name: jet-cluster
>spec:
>  cluster:
>    memberCount: 2
>  securityContext:
>    runAsUser: ""
>    runAsGroup: ""
>    fsGroup: ""
>  managementcenter:
>    licenseKey: <YOUR_LICENSE_KEY>
>```

Start Hazelcast Jet cluster with the following command.

```bash
kubectl apply -f hazelcast-jet.yaml
```

Your Hazelcast Jet cluster (together with Hazelcast Jet Management Center)
should be created.

```bash
$ kubectl get all
NAME                                                               READY   STATUS    RESTARTS   AGE
pod/jet-cluster-hazelcast-jet-0                                    1/1     Running   0          6m28s
pod/jet-cluster-hazelcast-jet-1                                    1/1     Running   0          5m54s
pod/jet-cluster-hazelcast-jet-management-center-85bbf6948b-p5n5h   1/1     Running   0          6m30s

NAME                                                  TYPE           CLUSTER-IP     EXTERNAL-IP     PORT(S)                        AGE
service/jet-cluster-hazelcast-jet                     ClusterIP      None           <none>          5701/TCP                       6m32s
service/jet-cluster-hazelcast-jet-management-center   LoadBalancer   10.47.244.94   34.72.142.219   8081:32476/TCP,443:32535/TCP   6m32s
service/kubernetes                                    ClusterIP      10.47.240.1    <none>          443/TCP                        2d22h

NAME                                                          READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/jet-cluster-hazelcast-jet-management-center   1/1     1            1           6m32s

NAME                                                                     DESIRED   CURRENT   READY   AGE
replicaset.apps/jet-cluster-hazelcast-jet-management-center-85bbf6948b   1         1         1       6m32s

NAME                                         READY   AGE
statefulset.apps/jet-cluster-hazelcast-jet   2/2     6m33s
```

Check the logs of the Hazelcast Jet nodes to verify cluster formation
with the command below:

```bash
$ kubectl logs pod/jet-cluster-hazelcast-jet-1
...
...
2020-05-11 07:42:50,785 [ INFO] [hz.naughty_mendel.generic-operation.thread-0] [c.h.i.c.ClusterService]:

Members {size:2, ver:2} [
 Member [10.44.1.11]:5701 - a720a93f-418a-4656-82a9-ae69eaeba99d
 Member [10.44.2.9]:5701 - 5ac07dd4-e631-444e-a9ab-5e5faf1cd617 this
]

2020-05-11 07:42:51,747 [ INFO] [main] [c.h.c.LifecycleService]: [10.44.2.9]:5701 is STARTED
...
```

As you can verify from the logs that we do have a 2 node Hazelcast Jet
cluster formed successfully.

## Submitting the Sample Job

To access this cluster inside Kubernetes environment from outside, we
need to do port forwarding. Port forwarding makes a port of a pod in the
remote Kubernetes cluster available to the local environment.

To submit a job, we'll do port forwarding from a Hazelcast Jet
pod on port 5701 to local port 5701. The command requires to
use a pod name to be forwarded.

Run the command below with to do port forwarding happen:

```bash
$ kubectl port-forward pod/jet-cluster-hazelcast-jet-1 5701:5701
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
Version: 4.1
Size: 2

ADDRESS                  UUID
[10.44.1.11]:5701        a720a93f-418a-4656-82a9-ae69eaeba99d
[10.44.2.9]:5701         5ac07dd4-e631-444e-a9ab-5e5faf1cd617
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

## Connect to Hazelcast Jet Management Center

Run the following command to get Hazelcast Jet Management Center service
details:

```bash
$ kubectl get svc
NAME                                          TYPE           CLUSTER-IP     EXTERNAL-IP     PORT(S)                        AGE
jet-cluster-hazelcast-jet                     ClusterIP      None           <none>          5701/TCP                       54m
jet-cluster-hazelcast-jet-management-center   LoadBalancer   10.47.244.94   34.72.142.219   8081:32476/TCP,443:32535/TCP   54m
kubernetes                                    ClusterIP      10.47.240.1    <none>          443/TCP                        2d23h
```

To connect to Hazelcast Jet Management Center, you can use `EXTERNAL-IP`
and open your browser at: `http://<EXTERNAL-IP>:8081`.

> If you are running on a Minikube environment which does not have
> Load Balancer support out of the box and it will show the `EXTERNAL-IP`
>as `pending`.
>
>To solve this problem, on a separate terminal window, run the following
>command:
>
>```bash
>minikube tunnel
>```
>
>It will ask for a password and expose service to the host operating
>system.
>
>When asked for service details with the following:
>
>```bash
>kubectl get svc
>```

It should show the `EXTERNAL-IP` and Hazelcast Jet Management Center
should be accessible at  `http://<EXTERNAL-IP>:8081`.

You should see the login page at `http://<EXTERNAL-IP>:8081` like below and
use `admin` as both username and password to login.

![Hazelcast Jet Management Center Login](assets/mc-login.png)

You can see various details regarding cluster and the job in the
dashboard.

![Hazelcast Jet Management Center Dashboard](assets/mc-dashboard.png)

Click to running job named `hello-world`, which we've submitted earlier,
in the `Active Jobs` section to see the details of it.

![Hazelcast Jet Management Center Job Details](assets/mc-job.png)

## Stopping the Job

You can stop the job when you are done with it.

To stop the job we need to know its name or id. List the jobs running
in the cluster with the command below:

```bash
$ bin/jet list-jobs
ID                  STATUS             SUBMISSION TIME         NAME
045e-25d1-b680-0001 RUNNING            2020-05-18T14:47:04.595 hello-world
```

Then you can cancel it using either name or id of the job like
following:

```bash
$ bin/jet cancel hello-world
Cancelling job id=045e-25d1-b680-0001, name=hello-world, submissionTime=2020-05-18T14:47:04.595
Job cancelled.
```
