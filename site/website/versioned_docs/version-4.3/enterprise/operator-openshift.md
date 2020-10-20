---
title: Installation on Red Hat OpenShift
description: Step-by-step guide to install Hazelcast Jet Enteprise Operator on Red Hat Openshift and deploying a sample job.
id: version-4.3-operator-openshift
original_id: operator-openshift
---

[OpenShift](https://www.openshift.com/learn/what-is-openshift) is a
hybrid cloud, enterprise Kubernetes application platform from RedHat.
[Red Hat Marketplace](https://marketplace.redhat.com/en-us), an open
cloud marketplace that enables discovery and access to a certified
software which is available to deploy on any Red Hat OpenShift cluster.

[Hazelcast Jet
Enterprise](https://marketplace.redhat.com/en-us/products/hazelcast-jet)
is available to you on Red Hat Marketplace as a Certified Enterprise
Ready Software.

## Prerequisites

- A running OpenShift cluster with administration rights.

## How to install Operator from Developer Catalog

Hazelcast Jet Enterprise operator needs to be installed to create and
manage Hazelcast Jet Enterprise clusters.

To install the operator, navigate to the OperatorHub page under
Operators section in the Administrator view.

![OperatorHub](assets/operatorhub.png)

Find the `Hazelcast Jet Enterprise Operator` in the catalog either by
scrolling down or you can filter by typing in `jet`

![Hazelcast Jet Enterprise Operator](assets/operatorhub-jet.png)

Click to `Hazelcast Jet Enterprise Operator` card to continue the
installation process.

![Hazelcast Jet Enterprise Operator Install
Screen](assets/operatorhub-jet-install.png)

Click to `Install` button. When clicked you'd see a screen with various
configuration options regarding the installation.

### Installing for a specific namespace

Hazelcast Jet Enterprise Operator can be installed to a specific
namespace and operates only in that namespace. It cannot respond to
requests from any other namespace.

![Single Namespace](assets/operatorhub-jet-install-single-ns.png)

### Installing for all namespaces

Hazelcast Jet Enterprise Operator can be installed to all namespaces and
operates Hazelcast Jet Enterprise clusters in any namespace on the
cluster. In this mode, operator will be installed to
`openshift-operators` namespace, but it will listen for changes in all
namespaces.

![All Namespaces](assets/operatorhub-jet-install-all-ns.png)

### Finishing the Installation

After you've selected the installation mode, then click to `Subscribe`
button, it will create necessary resources to make the operator work
properly. You should be able to see `Hazelcast Jet Enterprise` operator
in the `Installed Operators` section under `Operators` menu.

![Subscribe](assets/operatorhub-jet-installed.png)

> Note: In this example we choose to install the operator to all
namespaces so it's installed in the `openshift-operators` namespace.

## Creating the Cluster

Let's switch to the `Developer` view and follow the instructions below
to create a Hazelcast Jet Enterprise cluster.  

### Creating a License Key Secret

You'll need a license key to start a Hazelcast Jet Enterprise cluster.
To pass the license key to the cluster to be started, we need to create
a `Secret` object which holds our license key.

To create a Licese Key Secret, click to `Add` link from left menu then
click `From YAML` card.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: jet-license-key-secret
stringData:
  key: YOUR_LICENSE_KEY
```

Paste the YAML content above with your License Key replaced to the
editor on the screen like below and click `Create` button.

![Create License Key Secret](assets/developer-jet-license.png)

Alternatively, if you do have an access to the `oc` command line
utility, you can run the command below:

```bash
oc create secret generic jet-license-key-secret --from-literal=key=your-license-key
```

Click to `Add` from left menu then click `From Catalog` card.

![Developer View](assets/developer.png)

Find the `Hazelcast Jet Enterprise` in the catalog either by scrolling
down or you can filter items by typing in `jet`

![Hazelcast Jet Enterprise](assets/developer-jet.png)

Click to `Hazelcast Jet Enterprise` card, then click to `Create` button
on the new section.

![Hazelcast Jet Enterprise](assets/developer-jet-install.png)

It will show a YAML editor like below, you can configure the Hazelcast
Jet Enterprise cluster here. Default configuration will create a 2 node
cluster.

You can use [this
file](https://github.com/hazelcast/hazelcast-jet-operator/blob/master/hazelcast-jet-enterprise-operator/hazelcast-jet-enterprise-full.yaml)
for configuration options reference.

Click to `Create` button at the bottom to create the cluster.

![Hazelcast Jet Enterprise](assets/developer-jet-install-2.png)

> Note that we are passing in the name of secret which we created
earlier that holds our license key information in the
`jet.licenseKeySecretName` property.

You should be able to see the cluster has been created like below:

![Hazelcast Jet Enterprise](assets/developer-jet-installed.png)

If you navigate to the `Topology` view from the left menu, you can see
that Hazelcast Jet Enterprise and Hazelcast Jet Management Center
deployment/statefulsets has been created by Hazelcast Jet Enterprise
Operator.

![Topology View](assets/developer-jet-topology.png)

## Checking the Logs

### Check Hazelcast Jet Enterprise Logs

Navigate to the `Topology` view and click to the resource which has the
`hazelcast-jet-enterprise` suffix. It will show a section on the right
which shows the details of that resource like below:

![Hazelcast Jet Enterprise Logs](assets/developer-jet-topology.png)

Then click to `View logs` link to see the logs of corresponding pod.

![Hazelcast Jet Enterprise Logs](assets/developer-jet-topology-logs.png)

As you can verify from the logs that we do have a 2 node Hazelcast Jet
Enterprise cluster formed successfully.

### Check Hazelcast Jet Management Center Logs

Navigate to the `Topology` view and click to the resource which has the
`hazelcast-jet-enterprise-management-center` suffix. It will show a
section on the right which shows the details of that resource like
below:

![Hazelcast Jet Enterprise Logs](assets/developer-jet-topology-mc.png)

Then click to `View logs` link to see the logs of corresponding pod.

![Hazelcast Jet Management Center
Logs](assets/developer-jet-topology-mc-logs.png)

As you can verify from the logs that we do have a 2 node Hazelcast Jet
Enterprise cluster and Management Center application connected to it
successfully.

## Submitting the Sample Job

To access this cluster inside Openshift environment from outside, we
need do port forwarding. Port forwarding makes a port of a pod in the
remote Kubernetes cluster available to the local environment.

To submit a job, we'll make port forwarding from a Hazelcast Jet
Enterprise pod on port 5701 to local port 5701. The command requires to
use a pod name to be forwarded. We will retrieve the pod name from the
`Topology` view. Click to the `jet-hazelcast-jet-enterprise` stateful
set, a new panel will be shown on the right side. You can use any pods
listed in the `Pods` section.

>If you've installed operator into a specific namespace, switch to that
>namespace first with the following:
>
>```bash
>$ oc project my-namespace
>Now using project "my-namespace" on server "XXXXXX"
>```

Run the command below with to make port forwarding happen:

```bash
$ oc port-forward pods/jet-hazelcast-jet-enterprise-0 5701:5701
Forwarding from 127.0.0.1:5701 -> 5701
Forwarding from [::1]:5701 -> 5701
...
```

Download and unpack Hazelcast Jet distribution from [Jet
Website](https://jet-start.sh/) to use sample job packaged with it.

In a different terminal window, unpack and navigate to the distribution
folder with the commands below:

```bash
tar xf hazelcast-jet-4.3.tar.gz
cd hazelcast-jet-4.3/
```

Verify that CLI can connect to the cluster with the command below:

```bash
$ bin/jet cluster
State: ACTIVE
Version: 4.3
Size: 2

ADDRESS                  UUID
[10.131.0.6]:5701        a129440a-51d1-43dd-8bc4-decfcf5df09d
[10.131.0.8]:5701        a230f987-89ac-4161-8e63-c6d51ee8da78

```

Then you can submit the example job from the distribution package with
the command below:

```bash
$ bin/jet submit examples/hello-world.jar
Submitting JAR 'examples/hello-world.jar' with arguments []

Top 10 random numbers in the latest window:
 1. 8,898,539,899,168,029,424
 2. 7,720,861,389,917,424,780
 3. 7,625,232,857,233,015,099
 4. 6,777,278,758,369,921,301
 5. 6,265,041,662,117,114,223
 6. 6,181,674,658,021,839,025
 7. 5,639,320,479,621,760,652
 8. 5,251,546,821,760,470,933
 9. 4,544,929,439,891,010,872
 10. 4,513,471,017,811,828,408

...
```

That's it, you've successfully deployed a job from your local machine to
the Hazelcast Jet Enterprise cluster running inside Openshift
environment. In the next section we'll show how to connect Hazelcast Jet
Management Center to monitor and manage your jobs.

## Monitoring the cluster and the job with Management Center

Navigate to the `Topology` view and click to the resource which has the
`hazelcast-jet-enterprise-management-center` suffix. It will show a
section on the right which shows the details of that resource like
below:

![Hazelcast Jet Enterprise Logs](assets/developer-jet-topology-mc.png)

Then click to `jet-hazelcast-jet-enterprise-management-center` link in
the `Services` section to view the details of the service like below:

![Hazelcast Jet Management Center
Service](assets/developer-jet-topology-mc-service.png)

In the `Service Routing` section you can see the `LoadBalancer` location
for the Hazelcast Jet Management Center service. Open your favorite
browser and navigate to `http://<Location>:8081` to access Hazecast Jet
Management Center.

You should see the login page at `http://<Location>:8081` like below and
use `admin` as both username and password to login. ![Hazelcast Jet
Management Center Login](assets/mc-login.png)

You can see various details regarding cluster and the job in the
dashboard.

![Hazelcast Jet Management Center Dashboard](assets/mc-dashboard.png)

Click to running job named `hello-world`, which we've submitted earlier,
in the `Active Jobs` section to see the details of it.

![Hazelcast Jet Management Center Job Details](assets/mc-job.png)

You can also see information regarding Hazelcast Jet Enterprise Cluster
on the `Cluster` link in the side menu.

![Hazelcast Jet Management Center Cluster View](assets/mc-cluster.png)

## Stopping the Job

You can stop the job when you are done with it.

To stop the job we need to know it's name or id. List the jobs running
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
