# Table of Contents

* [Introduction](#hazelcast-discovery-plugin-for-microsoft-azure)
* [Getting Started](#getting-started)
* [Configuring the Plugin](#configuring-the-plugin)
    * [Basic Configuration](#basic-configuration)
    * [Additional Properties](#additional-properties)
    * [Configuration for Hazelcast Clients Connecting from outside Azure](#configuration-for-hazelcast-clients-connecting-from-outside-azure)
    * [Configuration for WAN Replication Target Cluster Discovery](#configuration-for-wan-replication-target-cluster-discovery)
    * [Property Definitions](#property-definitions)
    * [Azure App Services Support](#azure-app-services-support)
    * [High Availability](#high-availability)
* [Automated Deployment](#automated-deployment)


# Hazelcast Discovery Plugin for Microsoft Azure

This project provides a discovery strategy for Hazelcast 4.0+ enabled applications running on Azure. It will provide all Hazelcast instances by returning VMs within your Azure resource group. It supports virtual machine scale sets and tagging.

# Getting Started

To add this plugin to your Java project, add the following lines to either your Maven POM file or Gradle configuration.

For Gradle:

```
repositories {
    jcenter() 
}

dependencies {
    compile 'com.hazelcast:hazelcast-azure:${hazelcast-azure-version}'
}
```

For Maven:

```xml
<dependencies>
    <dependency>
        <groupId>com.hazelcast</groupId>
        <artifactId>hazelcast-azure</artifactId>
        <version>${hazelcast-azure-version}</version>
    </dependency>
</dependencies>
```

Check the [releases](https://github.com/hazelcast/hazelcast-azure/releases) for the latest version.

# Configuring the Plugin

Firstly, please ensure that you have added the `hazelcast-azure` dependency to your Maven or Gradle configuration as mentioned above. 

## Basic Configuration

Hazelcast Azure Plugin uses [Azure Instance Metadata Service](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/instance-metadata-service) to get access token and other environment details. In order to use this service, the plugin requires that [Azure managed identities](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview) with the correct `READ` roles are setup.

After setting up the managed identities, you just need the following minimal configuration to enable Hazelcast Azure discovery service:  

#### XML Configuration:
```xml
<hazelcast>
  <network>
    <join>
      <multicast enabled="false"/>
      <azure enabled="true"/>
    </join>
  </network>
</hazelcast>
```
#### YAML Configuration:
```yaml
hazelcast:
  network:
    join:
      multicast:
        enabled: false
      azure:
        enabled: true
```

The other necessary information such as subscription ID and and resource group name will be retrieved from instance metadata service. This method allows to run the plugin without keeping any secret or password in the code or configuration.

#### Note

If you don't setup the correct access roles on Azure environment, then you will see an exception similar below:

```
WARNING: Cannot discover nodes, returning empty list
com.hazelcast.azure.RestClientException: Failure in executing REST call
        at com.hazelcast.azure.RestClient.call(RestClient.java:106)
        at com.hazelcast.azure.RestClient.get(RestClient.java:69)
        at com.hazelcast.azure.AzureMetadataApi.callGet(AzureMetadataApi.java:107)
        at com.hazelcast.azure.AzureMetadataApi.accessToken(AzureMetadataApi.java:96)
        at com.hazelcast.azure.AzureClient.fetchAccessToken(AzureClient.java:131)
        at com.hazelcast.azure.AzureClient.getAddresses(AzureClient.java:118)
        at com.hazelcast.azure.AzureDiscoveryStrategy.discoverNodes(AzureDiscoveryStrategy.java:136)
        at com.hazelcast.spi.discovery.impl.DefaultDiscoveryService.discoverNodes(DefaultDiscoveryService.java:71)
        at com.hazelcast.internal.cluster.impl.DiscoveryJoiner.getPossibleAddresses(DiscoveryJoiner.java:69)
        at com.hazelcast.internal.cluster.impl.DiscoveryJoiner.getPossibleAddressesForInitialJoin(DiscoveryJoiner.java:58)
        at com.hazelcast.internal.cluster.impl.TcpIpJoiner.joinViaPossibleMembers(TcpIpJoiner.java:136)
        at com.hazelcast.internal.cluster.impl.TcpIpJoiner.doJoin(TcpIpJoiner.java:96)
        at com.hazelcast.internal.cluster.impl.AbstractJoiner.join(AbstractJoiner.java:137)
        at com.hazelcast.instance.impl.Node.join(Node.java:796)
        at com.hazelcast.instance.impl.Node.start(Node.java:451)
        at com.hazelcast.instance.impl.HazelcastInstanceImpl.<init>(HazelcastInstanceImpl.java:122)
        at com.hazelcast.instance.impl.HazelcastInstanceFactory.constructHazelcastInstance(HazelcastInstanceFactory.java:241)
        at com.hazelcast.instance.impl.HazelcastInstanceFactory.newHazelcastInstance(HazelcastInstanceFactory.java:220)
        at com.hazelcast.instance.impl.HazelcastInstanceFactory.newHazelcastInstance(HazelcastInstanceFactory.java:158)
        at com.hazelcast.core.Hazelcast.newHazelcastInstance(Hazelcast.java:91)
        at com.hazelcast.core.server.HazelcastMemberStarter.main(HazelcastMemberStarter.java:46)
Caused by: com.hazelcast.azure.RestClientException: Failure executing: GET at: http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com. Message: {"error":"invalid_request","error_description":"Identity not found"},
        at com.hazelcast.azure.RestClient.call(RestClient.java:101)
        ... 20 more
```

## Additional Properties

Please see the available additional properties below:

#### XML Configuration:
```xml
<hazelcast>
  <network>
    <join>
      <multicast enabled="false"/>
      <azure enabled="true">
        <tag>TAG-NAME=HZLCAST001</tag>
        <hz-port>5701-5703</hz-port>
      </azure>  
    </join>
  </network>
</hazelcast>
```
#### YAML Configuration:
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

- `tag` - *(Optional)* The key-value pair of the tag on the Azure network interfaces (NIC). The format should be as `key=value`. If this setting is configured, the plugin will search for instances over only the resources that have this tag entry in their network interfaces. If not configured, the plugin will search for instances over all available resources.
- `hz-port` - *(Optional)* The port range where Hazelcast is expected to be running. The format should be as `5701` or `5701-5703`. The default value is `5701-5703`.

## Configuration for Hazelcast Clients Connecting from outside Azure
 
Hazelcast client instances might be running outside of an Azure VM which makes Azure Instance Metadata service unavailable. Then, Hazelcast client instances should be configured with the properties as shown below:

#### XML Configuration:
```xml
<hazelcast-client>
  <network>
      <azure enabled="true">
        <instance-metadata-available>false</instance-metadata-available>
        <client-id>CLIENT_ID</client-id>
        <client-secret>CLIENT_SECRET</client-secret>
        <tenant-id>TENANT_ID</tenant-id>
        <subscription-id>SUB_ID</subscription-id>
        <resource-group>RESOURCE-GROUP-NAME</resource-group>
        <scale-set>SCALE-SET-NAME</scale-set>
        <use-public-ip>true</use-public-ip>
      </azure>
  </network>
</hazelcast-client>
```
#### YAML Configuration:
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

Please see [Property Definitions](#property-definitions) below for details.

## Configuration for WAN Replication Target Cluster Discovery

Hazelcast allows you to configure [WAN replication](https://docs.hazelcast.org/docs/latest/manual/html-single/#wan) to work with the Azure Discovery plugin and determine the endpoint IP addresses at runtime. If the Hazelcast cluster is running outside Azure environment and the target cluster is running inside Azure Environment, then you should configure your WAN batch publisher as below:

#### XML Configuration:
```xml
<hazelcast>
    <wan-replication name="my-wan-cluster-batch">
        <batch-publisher>
          ...
          <azure enabled="true">
            <instance-metadata-available>false</instance-metadata-available>
            <client-id>CLIENT_ID</client-id>
            <client-secret>CLIENT_SECRET</client-secret>
            <tenant-id>TENANT_ID</tenant-id>
            <subscription-id>SUB_ID</subscription-id>
            <resource-group>RESOURCE-GROUP-NAME</resource-group>
            <scale-set>SCALE-SET-NAME</scale-set>
          </azure>
        </batch-publisher>
    </wan-replication>
</hazelcast>
``` 
#### YAML Configuration:
```yaml
hazelcast:
  wan-replication:
    name: my-wan-cluster-batch
    batch-publisher:
      ...
      azure:
        enabled: true
        instance-metadata-available: false
        client-id: CLIENT_ID
        tenant-id: TENANT_ID
        client-secret: CLIENT_SECRET
        subscription-id: SUB_ID
        resource-group: RESOURCE-GROUP-NAME
        scale-set: SCALE-SET-NAME
```
Please see [Property Definitions](#property-definitions) below for details.

## Property Definitions

You will need to setup [Azure Active Directory Service Principal credentials](https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/) for your Azure Subscription to be able to use these properties. 

- `instance-metadata-available` - This property should be configured as `false` in order to be able to use the following properties. It is `true` by default.
- `client-id` - The Azure Active Directory Service Principal client ID.
- `client-secret` - The Azure Active Directory Service Principal client secret.
- `tenant-id` - The Azure Active Directory tenant ID.
- `subscription-id` - The Azure subscription ID.
- `resource-group` - The name of Azure [resource group](https://azure.microsoft.com/en-us/documentation/articles/resource-group-portal/) which the Hazelcast instance is running in.
- `scale-set` - *(Optional)* The name of Azure [VM scale set](https://docs.microsoft.com/en-us/azure/virtual-machine-scale-sets/overview). If this setting is configured, the plugin will search for instances over the resources only within this scale set.
- `use-public-ip` - Enables the discovery joiner to use public IPs. It should be set to `true` in client instances running outside the Azure environment.

## Azure App Services Support

[Azure App Services](https://azure.microsoft.com/en-gb/services/app-service/) is a platform as a service (PaaS) which allows publishing applications running on multiple frameworks and written in different programming languages. If you would like to use Hazelcast in your App Service applications, you can use Hazelcast Azure Discovery Plugin with Hazelcast Client to connect to a Hazelcast cluster running in Azure environment. It is the same configuration to run a Hazelcast client outside Azure environment, thus please see the [Configuration for Hazelcast Clients Connecting from outside Azure](#configuration-for-hazelcast-clients-connecting-from-outside-azure) section to setup your client in App Services.

Azure App Services are unaware of the underlying VMs' network interfaces so it is not available to communicate among App Services using TCP/IP. Because of this reason, it is not possible to deploy a Hazelcast cluster into Azure App Services.  

## High Availability

By default, Hazelcast distributes partition replicas (backups) randomly and equally among cluster members. However, this
is not safe in terms of high availability when a partition and its replicas are stored on the same rack, using the same
network, or power source. To deal with that, Hazelcast offers logical partition grouping, so that a partition itself and
its backup(s) would not be stored within the same group. This way Hazelcast guarantees that a possible failure affecting
more than one member at a time will not cause data loss. The details of partition groups can be found in the documentation:
[Partition Group Configuration](https://docs.hazelcast.org/docs/latest/manual/html-single/#partition-group-configuration)

In addition to built-in grouping option `ZONE_AWARE`, you can customize the formation of
these groups based on the network interfaces of members. See more details on custom groups in the documentation:
[Custom Partition Groups](https://docs.hazelcast.org/docs/latest/manual/html-single/#custom).

### Multi-Zone Deployments

If `ZONE_AWARE` partition group is enabled, the backup(s) of a partition is always stored in a different availability
zone. Note that if a cluster is deployed to an Azure region that does not support availability zones, then the fault
domains of instances are used when forming partition groups. That is, the members on fault domain (FD) 0 form a single
group, and those on FD 1 form another group, and so on.

***NOTE:*** *When using the `ZONE_AWARE` partition grouping, a cluster spanning multiple availability zones (or fault
domains if the region does not support zones) should have an equal number of members in each zone (or fault domain).
Otherwise, it will result in uneven partition distribution among the members.*

#### XML Configuration

```xml
<partition-group enabled="true" group-type="ZONE_AWARE" />
```

#### YAML Configuration

```yaml
hazelcast:
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

# Automated Deployment

You can also use the [Azure Hazelcast Template](https://github.com/Azure/azure-quickstart-templates/tree/master/hazelcast-vm-cluster) to automatically deploy a Hazelcast cluster which uses this plugin.
