---
title: Installation
description: Install Hazelcast Jet Enterprise
id: version-4.2-installation
original_id: installation
---

Hazelcast Jet Enterprise requires a license key to run. You can get a
30-day trial license from the [Hazelcast website](https://hazelcast.com/download).

## Download Hazelcast Jet

Hazelcast Jet requires a minimum of JDK 8, which can be acquired from
[AdoptOpenJDK](https://adoptopenjdk.net/).

Once you have a license key, download Hazelcast Jet and unzip it to a
folder we will refer as `JET_HOME`.

```bash
wget https://download.hazelcast.com/jet-enterprise/hazelcast-jet-enterprise-4.2.tar.gz
tar zxvf hazelcast-jet-enterprise-4.2.tar.gz
cd hazelcast-jet-enterprise-4.2
```

## Set License Key

Before you can start the node, you will need to set the license key. You
can do this by editing `<JET_HOME>/config/hazelcast.yaml`:

```yaml
hazelcast:
  cluster-name: jet
  license-key: <enter license key>
```

Once the license key is set, you can start the node using
`<JET_HOME>/bin/jet-start` per usual. The same license key can be used
on all the nodes.

It's also possible to configure the license key using `JET_LICENSE_KEY`
environment variable or `-Dhazelcast.enterprise.license.key` system
property.

When you start the node, you should the license information as below which
will list the support features.

```bash
2020-03-12 10:56:47,592  INFO [c.h.i.i.NodeExtension] [main] -  License{allowedNumberOfNodes=8,
expiryDate=02/25/2022 23:59:59, featureList=[ Management Center, Clustered JMX,
Clustered Rest, Security, Wan Replication, High Density Memory, Hot Restart,
Rolling Upgrade, Jet Management Center, Jet Lossless Recovery, Jet Rolling Job Upgrade,
Jet Enterprise, Cluster Client Filtering ],
type=Enterprise HD, companyName=null, ownerEmail=null, keyHash=NNNN, No Version Restriction}
```

## Verify Installation

You can verify the installation by submitting the `hello-word` job in
the examples folder:

```bash
bin/jet submit examples/hello-world
```

## Client Configuration

When using Hazelcast Jet Enterprise Client, there isn't any need to set
the license key as the client itself doesn't require it. A Jet client
can be created as normal using the `Jet.newJetClient` or
`Jet.bootstrappedInstance` syntax. The client binaries are not available
on Maven Central, but need to be downloaded from a repository hosted by
Hazelcast.

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
repositories {
    maven {
        url "https://repository.hazelcast.com/release/"
    }
}

compile 'com.hazelcast.jet:hazelcast-jet-enterprise:4.2'
```

<!--Maven-->

```xml
<repository>
    <id>private-repository</id>
    <name>Hazelcast Private Repository</name>
    <url>https://repository.hazelcast.com/release/</url>
</repository>

<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet-enterprise</artifactId>
    <version>4.2</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

## Embedded Mode

When using Jet in embedded mode, there are no changes to the API used
for creating the `JetInstance`. Enterprise version is automatically
detected during startup. License key needs to be set inside the config
before node startup.

## Install Using Docker

Hazelcast maintains an official Docker image for Hazelcast Jet
 Enterprise. The Docker Image requires a license key to start Hazelcast
 Jet instance properly. The license key can be passed to the container
 with the `JET_LICENSE_KEY` environment property.

To start a node, run the following command:

```bash
docker run -e JET_LICENSE_KEY=<your_license_key> hazelcast/hazelcast-jet-enterprise
```

This should start a Hazelcast Jet node in a Docker container. Inspect
the log output for a line like this:

```text
Members {size:1, ver:1} [
    Member [172.17.0.2]:5701 - 4bc3691d-2575-452d-b9d9-335f177f6aff this
]
```

Note the IP address of the Docker container and use it in the commands
below instead of our example's `172.17.0.2`. Let's submit the Hello
World application from the distribution package:

```bash
cd hazelcast-jet-enterprise-4.2
docker run -it -v "$(pwd)"/examples:/examples hazelcast/hazelcast-jet-enterprise jet -t 172.17.0.2 submit /examples/hello-world.jar
```

The command mounts the local `examples` directory from `hazelcast-jet-enterprise-4.2`
to the container and uses `jet submit` to submit the example JAR. While
the job is running, it should produce output like this:

```text
Top 10 random numbers in the latest window:
    1. 9,148,584,845,265,430,884
    2. 9,062,844,734,542,410,944
    3. 8,803,176,683,229,613,741
    4. 8,779,035,965,085,775,340
    5. 8,542,080,641,730,428,499
    6. 8,528,134,348,376,217,974
    7. 8,290,200,710,152,066,026
    8. 8,008,893,323,519,996,615
    9. 7,804,055,086,912,769,625
    10. 7,681,774,251,691,230,162
```

## Install Using Helm

Hazelcast Jet provides Helm charts for enterprise edition which
also includes [Hazelcast Jet Management Center](management-center).

Helm charts for Enterprise Edition are hosted on the Hazelcast Charts
 repository which needs to ne added to the list of repositories of your
 Helm CLI.

Run the following commands to add Hazelcast Charts repository and pull
the latest charts from there:

```bash
helm repo add hazelcast https://hazelcast.github.io/charts/
helm repo update
```

The Helm chart requires a license key to start Hazelcast
 Jet instances properly. The license key can be passed to the chart
 with the `jet.licenseKey` property.

Run the following command to create a Helm release from the Hazelcast
 Jet Enterprise Helm chart:

 <!--DOCUSAURUS_CODE_TABS-->

<!--Helm 2-->

```bash
helm install --set jet.licenseKey=<license-key> hazelcast/hazelcast-jet-enterprise
```

<!--Helm 3-->

```bash
helm install --generate-name --set jet.licenseKey=<license-key> hazelcast/hazelcast-jet-enterprise
```

<!--END_DOCUSAURUS_CODE_TABS-->

The command above will going to create a two node Hazelcast Jet
Enterprise cluster and a Hazelcast Jet Management Center deployment.

After executing the command, you can follow the notes printed on screen
to obtain a connection to the Hazelcast Jet Enterprise cluster and the
Hazelcast Jet Management Center.

For the list of available configuration options on the Hazelcast Jet
Enterprise Helm chart please refer to [this table](https://github.com/hazelcast/charts/tree/master/stable/hazelcast-jet-enterprise#configuration)
.
