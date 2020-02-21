# Hazelcast Discovery Plugin for GCP

This repository contains a plugin which provides the automatic Hazelcast member discovery in the Google Cloud Platform (Compute Engine) environment.

## Requirements

- Hazelcast 3.10+
- GCP VM instances must have access to Cloud API (at minimum "Read Only" Access Scope to "Compute Engine" API)
- Versions compatibility: hazelcast-gcp 1.1+ is compatible with hazelcast 3.11+; for older hazelcast versions you need to use hazelcast-gcp 1.0.

## Embedded Mode

To use Hazelcast GCP embedded in your application, you need to add the plugin dependency into your Maven/Gradle file. Then, when you provide `hazelcast.xml` (or Java-based configuration) as presented above, your Hazelcast instances discover themselves automatically.

#### Maven

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast-gcp</artifactId>
  <version>${hazelcast-gcp-version}</version>
</dependency>
```

#### Gradle

```groovy
compile group: "com.hazelcast", name: "hazelcast-gcp", version: "${hazelcast-gcp-version}"
```

## Understanding GCP Discovery Strategy

Hazelcast member starts by fetching a list of all instances filtered by projects, zones, and label. Then, each instance is checked one-by-one with its IP and each of the ports defined in the `hz-port` property. When a member is discovered under IP:PORT, then it joins the cluster.

If users want to create multiple Hazelcast clusters in one project/zone, then they need to manually label the instances.

## Configuration

The plugin supports **Members Discovery SPI** and **Zone Aware** features.

### Hazelcast Members Discovery SPI

Make sure you have the `hazelcast-gcp.jar` dependency in your classpath. Then, you can configure Hazelcast in one of the following manners.

#### XML Configuration

```xml
<hazelcast>
  <network>
    <join>
      <multicast enabled="false"/>
      <gcp enabled="true">
        <zones>us-east1-a,us-east1-b</zones>
        <label>application=hazelcast</label>
        <hz-port>5701-5708</hz-port>
      </gcp>
    </join>
  </network>
</hazelcast>
```

#### YAML Configuration

```yml
hazelcast:
  network:
    join:
      multicast:
        enabled: false
      gcp:
        enabled: false
        zones: us-east1-a,us-east1-b
        label: application=hazelcast
        hz-port: 5701-5708
```

#### Java-based Configuration

```java
config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
config.getNetworkConfig().getJoin().getGcpConfig().setEnabled(true)
      .setProperty("zones", "us-east1-a,us-east1-b")
      .setProperty("label", "application=hazelcast")
      .setProperty("hz-port", "5701-5708");
```

The following properties can be configured:
* `private-key-path`: a filesystem path to the private key for GCP service account in the JSON format; if not set, the access token is fetched from the GCP VM instance
* `projects`: a list of projects where the plugin looks for instances; if not set, the current project is used
* `zones`: a list of zones where the plugin looks for instances; if not set, the current zone is used
* `label`: a filter to look only for instances labeled as specified; property format: `key=value`
* `hz-port`: a range of ports where the plugin looks for Hazelcast members; if not set, the default value `5701-5708` is used

Note that:
* Your GCP Service Account must have permissions to query for all the projects/zones specified in the configuration
* If you don't specify any of the properties, then the plugin forms a cluster from all Hazelcast members running in the current project, in the current zone
* If you use the plugin in the Hazelcast Client running outside of the GCP network, then the following parameters are mandatory: `private-key-path`, `projects`, and `zones`

### Zone Aware

When using `ZONE_AWARE` configuration, backups are created in the other availability zone.

#### XML Configuration

```xml
<partition-group enabled="true" group-type="ZONE_AWARE" />
```

#### YAML Configuration

```yml
hazelcast:
  partition-group:
    enabled: true
    group-type: ZONE-AWARE
```

#### Java-based Configuration

```java
config.getPartitionGroupConfig()
    .setEnabled(true)
    .setGroupType(MemberGroupType.ZONE_AWARE);
```

***NOTE:*** *When using the `ZONE_AWARE` partition grouping, a cluster spanning multiple availability zones should have an equal number of members in each AZ. Otherwise, it will result in uneven partition distribution among the members.*

### Hazelcast Client with Discovery SPI

If Hazelcast Client is run inside GCP, then the configuration is exactly the same as for the Member.

If Hazelcast Client is run outside GCP, then you always need to specify the following parameters:
- `private-key-path` - path to the private key for GCP service account
- `projects` - a list of projects where the plugin looks for instances
- `zones`: a list of zones where the plugin looks for instances
- `use-public-ip` - must be set to `true`

Following are example declarative and programmatic configuration snippets.

#### XML Configuration

```xml
<hazelcast-client>
  <network>
    <gcp enabled="true">
      <private-key-path>/home/name/service/account/key.json</private-key-path>
      <projects>project-1,project-2</projects>
      <zones>us-east1-a,us-east1-b</zones>
      <label>application=hazelcast</label>
      <hz-port>5701-5708</hz-port>
      <use-public-ip>true</use-public-ip>
    </gcp>
  </network>
</hazelcast-client>
```

#### YAML Configuration

```yml
hazelcast:
  network:
    gcp:
      enabled: true
      private-key-path: /home/name/service/account/key.json
      projects: project-1,project-2
      zones: us-east1-a,us-east1-b
      label: application=hazelcast
      hz-port: 5701-5708
      use-public-ip: true
```
#### Java-based Configuration

```java
clientConfig.getGcpConfig().setEnabled(true)
      .setProperty("private-key-path", "/home/name/service/account/key.json")
      .setProperty("projects", "project-1,project-2")
      .setProperty("zones", "us-east1-a,us-east1-b")
      .setProperty("label", "application=hazelcast")
      .setProperty("hz-port", "5701-5708")
      .setProperty("use-public-ip", "true");
```
 
## How to find us?

In case of any question or issue, please raise a GH issue, send an email to [Hazelcast Google Groups](https://groups.google.com/forum/#!forum/hazelcast) or contact as directly via [Hazelcast Gitter](https://gitter.im/hazelcast/hazelcast).
