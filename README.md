# Hazelcast Discovery Plugin for GCP

This repository contains a plugin which provides the automatic Hazelcast member discovery in the Google Cloud Platform (Compute Engine) environment.

## Requirements

- Hazelcast 3.10+
- GCP VM instances must have access to Cloud API (at minimum "Read Only" Access Scope to "Compute Engine" API)

## Configuration

The plugin supports **Members Discovery SPI** and **Zone Aware** features.

### Hazelcast Members Discovery SPI

Make sure you have the `hazelcast-gcp.jar` dependency in your classpath. Then, you can configure Hazelcast in one of the following manners.

#### XML Configuration

```xml
 <hazelcast>
   ...
  <properties>
     <property name="hazelcast.discovery.enabled">true</property>
  </properties>
  <network>
    ...
    <join>
        <tcp-ip enabled="false" />
        <multicast enabled="false"/>
        <aws enabled="false" />
        <discovery-strategies>
            <!-- class equals to the DiscoveryStrategy not the factory! -->
            <discovery-strategy enabled="true" class="com.hazelcast.gcp.GcpDiscoveryStrategy">
                <properties>
                   <property name="projects">project-1,project-2</property>
                   <property name="zones">us-east1-a,us-east1-b</property>
                   <property name="label">application=hazelcast</property>
                   <property name="hz-port">5701-5708</property>
                </properties>
            </discovery-strategy>
        </discovery-strategies>
    </join>
  </network>
 </hazelcast>
```

#### Java-based Configuration

```java
Config config = new Config();
config.getProperties().setProperty("hazelcast.discovery.enabled", "true");
JoinConfig joinConfig = config.getNetworkConfig().getJoin();
joinConfig.getTcpIpConfig().setEnabled(false);
joinConfig.getMulticastConfig().setEnabled(false);
joinConfig.getAwsConfig().setEnabled(false);
GcpDiscoveryStrategyFactory gcpDiscoveryStrategyFactory = new GcpDiscoveryStrategyFactory();
Map<String, Comparable> properties = new HashMap<String, Comparable>();
properties.put("projects","project-1,project-2");
properties.put("zones","us-east1-a,us-east1-b");
properties.put("label","application=hazelcast");
properties.put("hz-port","5701-5708");
DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(gcpDiscoveryStrategyFactory, properties);
joinConfig.getDiscoveryConfig().addDiscoveryStrategyConfig(discoveryStrategyConfig);
```

The following properties can be configured:
* `projects`: a list of projects where the plugin looks for instances; if not set, the current project is used
* `zones`: a list of zones where the plugin looks for instances; if not set, the current zone is used
* `label`: a filter to look only for instances labeled as specified; property format: `key=value`
* `hz-port`: a range of ports where the plugin looks for Hazelcast members; if not set, the default value `5701-5708` is used

**Note**: Your GCP Service Account must have permissions to query for all the projects/zones specified in the configuration.

Note also that if you don't specify any of the properties, then the plugin forms a cluster from all Hazelcast members running in the current project, in the current zone.

### Zone Aware

When using `ZONE_AWARE` configuration, backups are created in the other availability zone.

#### XML Configuration

```xml
<partition-group enabled="true" group-type="ZONE_AWARE" />
```

#### Java-based Configuration

```java
PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
partitionGroupConfig.setEnabled(true)
    .setGroupType( MemberGroupType.ZONE_AWARE );
```

## Understanding GCP Discovery Strategy

Hazelcast member starts by fetching a list of all instances filtered by projects, zones, and label. Then, each instance is checked one-by-one with its IP and each of the ports defined in the `hz-port` property. When a member is discovered under IP:PORT, then it joins the cluster.

If users want to create multiple Hazelcast clusters in one project/zone, then they need to manually label the instances.
 
## Embedded Mode

To use Hazelcast GCP embedded in your application, you need to add the plugin dependency into your Maven/Gradle file. Then, when you provide `hazelcast.xml` (or Java-based configuration) as presented above, your Hazelcast instances discover themselves automatically.

### Maven

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast-gcp</artifactId>
  <version>${hazelcast-gcp-version}</version>
</dependency>
```

### Gradle

```groovy
compile group: "com.hazelcast", name: "hazelcast-gcp", version: "${hazelcast-gcp-version}"
```

## How to find us?

In case of any question or issue, please raise a GH issue, send an email to [Hazelcast Google Groups](https://groups.google.com/forum/#!forum/hazelcast) or contact as directly via [Hazelcast Gitter](https://gitter.im/hazelcast/hazelcast).