# Table of Contents

* [Discovery Implementation for Azure Services](#discovery-implementation-for-azure-services)
* [Getting Started](#getting-started)
* [Compiling with Gradle](#compiling-with-gradle)
* [Using the Plugin](#using-the-plugin)
    * [Configuration when Azure Instance Metadata Service is available](#configuration-when-azure-instance-metadata-service-is-available)
    * [Configuration when Azure Instance Metadata Service is NOT available](#configuration-when-azure-instance-metadata-service-is-not-available)
    * [Additional Properties](#additional-properties)
    * [ZONE_AWARE Partition Group](#zone_aware-partition-group)
    * [Configuring with Public IP Addresses](#configuring-with-public-ip-addresses)
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
    compile 'com.hazelcast.azure:hazelcast-azure:${hazelcast-azure-version}'
}
```

For Maven:

```xml
<dependencies>
    <dependency>
        <groupId>com.hazelcast.azure</groupId>
        <artifactId>hazelcast-azure</artifactId>
        <version>${hazelcast-azure-version}</version>
    </dependency>
</dependencies>
```

# Compiling with Gradle

Run the following command to compile the plugin:

```gradle
compile 'com.hazelcast.azure:hazelcast-azure:${hazelcast-azure-version}'
```

Check the [releases](https://github.com/hazelcast/hazelcast-azure/releases) for the latest version.

# Using the Plugin

Firstly, please ensure that you have added the package `hazelcast-azure` to your Maven or Gradle configuration as mentioned above. 

## Configuration when Azure Instance Metadata Service is available

Hazelcast Azure Plugin can use [Azure Instance Metadata Service](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/instance-metadata-service) to get access token and other environment details. 

You will need just the following minimal configuration to setup Hazelcast Azure Discovery if:
- Azure Instance Metadata Service is available. The Hazelcast instances (clients or members) should run in an Azure VM to make use of Azure Instance Metadata Service.  
- [Azure managed identities](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview) with the correct access roles are setup. 

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

## Configuration when Azure Instance Metadata Service is NOT available
 
There may be occasions that [Azure Instance Metadata Service](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/instance-metadata-service) is not available, such as:
- Hazelcast instances (clients or WAN Replication cluster members) might be running outside of an Azure VM.
- [Azure managed identities](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview) are not setup or available.

If so, then Hazelcast instances should be configured with the properties as shown below:

#### XML Configuration:
```xml
<hazelcast>
  <network>
    <join>
      <multicast enabled="false"/>
      <azure enabled="true">
        <use-instance-metadata>false</use-instance-metadata>
        <client-id>CLIENT_ID</client-id>
        <client-secret>CLIENT_SECRET</client-secret>
        <tenant-id>TENANT_ID</tenant-id>
        <subscription-id>SUB_ID</subscription-id>
        <resource-group>RESOURCE-GROUP-NAME</resource-group>
        <scale-set>SCALE-SET-NAME</scale-set>
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
        use-instance-metadata: false
        client-id: CLIENT_ID
        tenant-id: TENANT_ID
        client-secret: CLIENT_SECRET
        subscription-id: SUB_ID
        resource-group: RESOURCE-GROUP-NAME
        scale-set: SCALE-SET-NAME
        tag: TAG-NAME=HZLCAST001
        hz-port: 5701-5703
```
You will need to setup [Azure Active Directory Service Principal credentials](https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/) for your Azure Subscription to be able to use these properties. 

- `use-instance-metadata` - This property should be configured as `false` in order to be able to use the following properties. It is `true` by default.
- `client-id` - The Azure Active Directory Service Principal client ID.
- `client-secret` - The Azure Active Directory Service Principal client secret.
- `tenant-id` - The Azure Active Directory tenant ID.
- `subscription-id` - The Azure subscription ID.
- `resource-group` - The name of Azure [resource group](https://azure.microsoft.com/en-us/documentation/articles/resource-group-portal/) which the Hazelcast instance is running in.
- `scale-set` - *(Optional)* The name of Azure [VM scale set](https://docs.microsoft.com/en-us/azure/virtual-machine-scale-sets/overview). If this setting is configured, the plugin will search for instances over the resources only within this scale set.

## Additional Properties

The following properties can be configured at any of the circumstances above.  

- `tag` - *(Optional)* The key-value pair of the tag on the Azure VM resources. The format should be as `key=value`. If this setting is configured, the plugin will search for instances over only the resources that have this tag entry. If not configured, the plugin will search for instances over all available resources.
- `hz-port` - *(Optional)* The port range where Hazelcast is expected to be running. The format should be as `5701` or `5701-5703`. The default value is `5701-5703`.

## ZONE_AWARE Partition Group

When you use Azure plugin as discovery provider, you can configure Hazelcast Partition Grouping with Azure. You need to add fault domain or DNS domain to your machines. So machines will be grouped with respect to their fault or DNS domains.
For more information please read: http://docs.hazelcast.org/docs/3.7/manual/html-single/index.html#partition-group-configuration.

```xml
<partition-group enabled="true" group-type="ZONE_AWARE" />
```

## Configuring with Public IP Addresses

If you would like to use Azure Discovery Plugin to discover Hazelcast instances using public IPs, please note that you need to set the following two configurations:

- Configure `public-address` property in Hazelcast member configuration. See this property details [here](https://docs.hazelcast.org/docs/latest/manual/html-single/#public-address).
- Set `hazelcast.discovery.public.ip.enabled` system property to `true`. It enables the discovery joiner to use public IPs.   

# Automated Deployment

You can also use the [Azure Hazelcast Template](https://github.com/Azure/azure-quickstart-templates/tree/master/hazelcast-vm-cluster) to automatically deploy a Hazelcast cluster which uses this plugin.
