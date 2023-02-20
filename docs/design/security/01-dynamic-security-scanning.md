# Dynamic Security Scanning

Internal PRD link: https://hazelcast.atlassian.net/wiki/spaces/PM/pages/2695987811

## Summary

Provide an automated way of doing dynamic security scanning of the IMDG cluster.

## Goals

* introduce automated dynamic security scanning of Hazelcast clusters;
* define set of scenarios to be covered;
* implement unified reporting;

## Non-Goals

* static code analysis;
* check for vulnerable dependencies;

## Motivation

Hazelcast users/customers may be bound by security requirements defined
for libraries and tools used in their products. The requirements may
require static and dynamic security scans.

Hazelcast already has regular static scans (e.g. Sonar, Black Duck),
but we are missing dynamic scans focused on evaluating a running cluster.

This design document describes a dynamic security scanning approach that can be used.

## Functional design

The approach suggested by this design document uses an existing security scanning software 
[**OpenVAS**](https://www.openvas.org/) - Open Vulnerability Assessment Scanner.
OpenVAS is a powerful open-source vulnerability scanner. It supports
various high-level and low-level internet and industrial protocols. It uses an internal programming language 
(NASL - Nessus Attack Scripting Language) to implement vulnerability tests.

Each OpenVAS test is a plugin called Network Vulnerability Test (NVT).
Tests for vulnerabilities are actively developed and updated in feeds managed by Greenbone company:
https://community.greenbone.net/c/feed-services

Greenbone company also drives the development of related software:
* Greenbone Vulnerability Manager (GVM) - central management service staying between security scanners and user clients
* Greenbone Security Assistant (GSA) - web interface developed
* GVM tools - `gvm-cli` and Python library with helper `gvm-script` and `gvm-pyshell` launchers

These tools can be utilized and development can be focused on the integration into the Hazelcast CI/CD toolchain.

### Scenarios (Hazelcast configurations)

The security scanning will be performed on node configurations where additional
protocols (REST, Memcached) will be explicitly enabled.

The initial implementation should provide 3 networking configuration:
* default networking
* default networking + TLS (Hazelcast Enterprise only)
* advanced networking

The sample network configurations for these items are listed later in this section.

In addition to single node configurations, multi-node clusters will also be scanned.
After the security scan, the cluster will be checked if it's still in a safe state (i.e. no split-brain occurred).

#### Default networking (unified protocol)

```xml
<network>
    <port auto-increment="false">5701</port>
    <join>
        <multicast enabled="false"/>
        <tcp-ip enabled="true">
            <member>${member}</member>
        </tcp-ip>
    </join>
    <memcache-protocol enabled="true"/>
    <rest-api enabled="true">
        <endpoint-group name="CLUSTER_READ" enabled="true"/>
        <endpoint-group name="CLUSTER_WRITE" enabled="true"/>
        <endpoint-group name="HEALTH_CHECK" enabled="true"/>
        <endpoint-group name="HOT_RESTART" enabled="true"/>
        <endpoint-group name="WAN" enabled="true"/>
        <endpoint-group name="DATA" enabled="true"/>
    </rest-api>
</network>
```

#### TLS

```xml
<network>
    <port auto-increment="false">5701</port>
    <join>
        <multicast enabled="false"/>
        <tcp-ip enabled="true">
            <member>${member}</member>
        </tcp-ip>
    </join>
    <ssl enabled="true">
        <properties>
            <property name="protocol">TLS</property>
            <property name="mutualAuthentication">REQUIRED</property>
            <property name="keyStore">${keystore.path}</property>
            <property name="keyStorePassword">${keystore.password}</property>
            <property name="trustStore">${truststore.path}</property>
            <property name="trustStorePassword">${truststore.password}</property>
        </properties>
    </ssl>
    <memcache-protocol enabled="true"/>
    <rest-api enabled="true">
        <endpoint-group name="CLUSTER_READ" enabled="true"/>
        <endpoint-group name="CLUSTER_WRITE" enabled="true"/>
        <endpoint-group name="HEALTH_CHECK" enabled="true"/>
        <endpoint-group name="HOT_RESTART" enabled="true"/>
        <endpoint-group name="WAN" enabled="true"/>
        <endpoint-group name="DATA" enabled="true"/>
    </rest-api>
</network>
```

#### Advanced networking

```xml
<advanced-network enabled="false">
    <join>
        <multicast enabled="false"/>
        <tcp-ip enabled="true">
            <member>${member}</member>
        </tcp-ip>
    </join>
    <member-server-socket-endpoint-config>
        <port auto-increment="false">5701</port>
    </member-server-socket-endpoint-config>
    <!-- server socket listening for connections from hazelcast clients -->
    <client-server-socket-endpoint-config>
        <port auto-increment="false" port-count="100">9999</port>
    </client-server-socket-endpoint-config>
    <!-- create a server socket for REST API -->
    <rest-server-socket-endpoint-config>
        <port auto-increment="false">8080</port>
        <endpoint-groups>
            <endpoint-group name="CLUSTER_READ" enabled="true"/>
            <endpoint-group name="CLUSTER_WRITE" enabled="true"/>
            <endpoint-group name="HEALTH_CHECK" enabled="true"/>
            <endpoint-group name="HOT_RESTART" enabled="true"/>
            <endpoint-group name="WAN" enabled="true"/>
            <endpoint-group name="DATA" enabled="true"/>
        </endpoint-groups>
    </rest-server-socket-endpoint-config>
    <memcache-server-socket-endpoint-config>
        <port auto-increment="false">6000</port>
    </memcache-server-socket-endpoint-config>
</advanced-network>
```

### Considered alternative approach

Another approach how to deal with the dynamic security scanning task is using a set of simple opensource tools and providing an integration glue
with reporting capabilities. This way is tempting as it allows great flexibility, but it's also very demanding and
its maintainability would be costly. Therefore this idea was abandoned.

## User Interaction

New Jenkins CI jobs will be created and placed into a new view. These jobs will allow testing Hazelcast Docker images
with custom configurations (basic config, advanced networking, TLS enabled).

Greenbone security assistant web interface will be deployed so it's simple to define manual scans of systems and review
test results.

## Technical design

The heart of the implementation is a python script `scan.gmp.py` it takes 2 or 3 arguments:

```bash
scan.gmp.py <HOSTs> <PORT_LISTs> [/path/to/report.pdf]

HOSTs = comma-separated IP addresses of scanned machines
PORT_LISTs = comma-separated port definitions (e.g. T8080,T5701-5703)
The optional third parameter is a path the result PDF file.
```

Jenkins jobs will be responsible for starting the Hazelcast member (or cluster) and executing the `scan.gmp.py`
script against it.

```bash
gvm-script --gmp-username "$GVM_USER" --gmp-password "$GVM_PASSWORD" tls --hostname $GVM_HOST \
      scan.gmp.py "$HAZELCAST_ADDRESSES" "$PORT_LISTS" ${JOB_NAME}-report.pdf
```

Related links:
* https://github.com/hazelcast/hazelcast-qe/tree/master/gvm - contains the `scan.gmp.py`, helper bash scripts for Jenkins and Hazelcast member configuration files.
* http://jenkins.hazelcast.com/view/GVM/ - Jenkins view for security scan jobs
* http://jenkins.hazelcast.com/view/GVM/job/gvm-trigger-for-hazelcast-docker-tag/ - Jenkins trigger job which runs security scans for several Hazelcast configurations and then puts result reports into a ZIP file

### GVM Docker image

Related links:
* https://hub.docker.com/r/devopshazelcast/gvm - Docker repository with Greenbone Vulnerability Manager image (`devopshazelcast/gvm`)
* https://github.com/hazelcast-dockerfiles/GVM-Docker - the `Dockerfile` source repository (for `devopshazelcast/gvm` Docker image)

### Snapshot Hazelcast Docker images

To have a possibility to scan the current codebase new Docker images will be published
on Docker push to our repositories (`hazelcast`, `hazelcast-enterprise`).
Each repository will have a "snapshot counterpart" in the `hazelcast-dockerfiles` GitHub organization.
The new snapshot repositories will contain `Dockerfiles` used to build the snapshot images.
Branch names in snapshot repositories will mirror the names in the base repositories.

Docker tags will be based on branch names. E.g.:
* `master` -> `master` (+ `latest`)
* `3.12.z` -> `3.12.z`
* `4.1.z` -> `4.1.z`

Related links:
* Hazelcast Docker snapshots:
  * https://github.com/hazelcast-dockerfiles/hazelcast-snapshot
  * https://hub.docker.com/r/devopshazelcast/hazelcast-snapshot
* Hazelcast Enterprise Docker snapshots:
  * https://github.com/hazelcast-dockerfiles/hazelcast-enterprise-snapshot
  * https://hub.docker.com/r/devopshazelcast/hazelcast-enterprise-snapshot

#### Highfive GitHub webhooks

To allow Docker builds on GitHub push to base repositories, our `highfive` application fork is extended to allow
GitHub push notifications transformation to DockerHub build trigger format.

Related links:
* https://github.com/hazelcast/highfive/blob/6823e94ec56cd394602b102dfb55e79cf653dab4/app.py#L88-L114
* https://github.com/hazelcast/highfive/blob/6823e94ec56cd394602b102dfb55e79cf653dab4/config.sample#L6-L9

## Testing

The generated reports will be manually reviewed before sharing with users/customers.
