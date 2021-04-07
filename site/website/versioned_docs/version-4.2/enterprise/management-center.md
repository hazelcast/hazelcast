---
title: Management Center
description: Install Hazelcast Jet Management Center
id: version-4.2-management-center
original_id: management-center
---

Hazelcast Jet Enterprise comes with a management center which can be
used to monitor a Jet cluster and manage the lifecycle of the jobs

## Download Management Center

Download Jet Management Center and unzip it to a folder:

```bash
wget https://download.hazelcast.com/hazelcast-jet-management-center/hazelcast-jet-management-center-4.2.tar.gz
tar zxvf hazelcast-jet-management-center-4.2.tar.gz
```

## Setting License Key

Like with the Jet Enterprise Server, Jet Management Center also requires
a license key. You can get a 30-day trial license from
[the Hazelcast website](https://hazelcast.com/download).

You can also run the Management Center without a license key,
but it will only work with a single node cluster. To update the license
key, edit the file `application.properties`:

```bash
# path for client configuration file (yaml or xml)
jet.clientConfig=hazelcast-client.xml
# license key for management center
jet.licenseKey=
```

## Configure Cluster IP

Hazelcast Management Center requires the address of the Hazelcast Jet
cluster to connect to it. You can configure the cluster connection
settings in `hazelcast-client.xml` file by editing the following
section:

```xml
<cluster-name>jet</cluster-name>
<network>
    <cluster-members>
        <address>127.0.0.1</address>
    </cluster-members>
</network>
```

## Start Management Center

Start the management center using the supplied script:

```bash
./jet-management-center.sh
```

The script also offers additional options during startup, which can be
viewed via:

```bash
./jet-management-center.sh --help
```

The server will by default start on port 8081 and you can browse to it
on your browser at `http://localhost:8081`. The default username and
password is `admin:admin` which you can change inside
`application.properties`.

You should be able to see the "hello-world" job running on the cluster,
if you've submitted it earlier.

## Run using Docker

Hazelcast Jet Management Center can also be started with Docker.
The Docker image requires a license key to start Management Center.
The license key can be passed to the container with the `MC_LICENSE_KEY`
 environment property.

Run the following command to start Management Center container:

```bash
docker run -e MC_LICENSE_KEY=<your-license-key> -p 8081:8081 hazelcast/hazelcast-jet-management-center
```

After the container has been started, the Hazelcast Jet Management
Center can be reached from the  browser using the URL <http://localhost:8081>
. One thing to note that we are using `-p` flag to map the `8081` port
of the container to the same port on the host machine.

To point Management Center to an existing cluster on Docker, a
configuration file which contains IP addresses of the cluster members
needs to be added to Management Center container. To achieve that,
create a `hazelcast-client.yaml` file with the following content on the
host machine:

```yaml
hazelcast-client:
  cluster-name: jet
  network:
    cluster-members:
      - 172.17.0.2:5701
```

Then Management Center container can be started with the configuration
file above by mapping the folder on host machine to the container.

```bash
docker run -e MC_LICENSE_KEY=<your-license-key> -p 8081:8081 -v /path/to/hazelcast-client.yaml:/conf/hazelcast-client.yaml -e MC_CLIENT_CONFIG=/conf/hazelcast-client.yaml hazelcast/hazelcast-jet-management-center
```

After running the command above, the output should be similar to the
below:

```log
...
...
2020-03-17 14:31:18.947  INFO 1 --- [           main] c.h.j.management.service.LicenseService  : License Info : License{allowedNumberOfNodes=NNN, expiryDate=MM/DD/YYYY 23:59:59, featureList=[ Management Center, Clustered JMX, Clustered Rest, Security, Hot Restart, Jet Management Center, Jet Lossless Recovery, Jet Rolling Job Upgrade, Jet Enterprise ], type=Enterprise HD, companyName=null, ownerEmail=null, keyHash=NNN, No Version Restriction}

...
2020-03-17 14:31:17.523  INFO 1 --- [-center.event-5] c.h.c.impl.spi.ClientClusterService      : jet-management-center [jet]

Members [1] {
    Member [172.17.0.2]:5701 - 32349cdf-8c9a-413f-8dad-80f2ef7bbcd6
}
...
...
╔═════════════════════════════════════════════════════════════════════════════════╗
║ Hazelcast Jet Management Center successfully started at http://localhost:8081/  ║
╚═════════════════════════════════════════════════════════════════════════════════╝

````

You should be able to log into <http://localhost:8081/> and see the
details of the cluster.

## Configuring TLS

To configure Jet Management Center to use TLS, you need to edit the
`hazelcast-client.xml` file and configure these properties in the SSL
section:

```bash
<property name="protocol">TLS</property>
<property name="trustStore">/opt/hazelcast-client.truststore</property>
<property name="trustStorePassword">123456</property>
<property name="trustStoreType">JKS</property>
```
