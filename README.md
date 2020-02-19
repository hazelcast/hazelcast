# Table of Contents

* [Discovery Implementation for Azure Services](#discovery-implementation-for-azure-services)
* [Getting Started](#getting-started)
* [Compiling with Gradle](#compiling-with-gradle)
* [Configuring at Hazelcast Side](#configuring-at-hazelcast-side)
* [Configuring at Azure Side](#configuring-at-azure-side)
* [Using Azure With ZONE_AWARE Partition Group](#using-azure-with-zone_aware-partition-group)
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

# Configuring at Hazelcast Side

Ensure that you have added the package `hazelcast-azure` to your Maven or Gradle configuration as mentioned above.

In your Hazelcast configuration, use the `AzureDiscoveryStrategy` as shown below:

#### XML Configuration:
```xml
<hazelcast>
  <network>
    <join>
      <multicast enabled="false"/>
      <azure enabled="true">
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
        client-id: CLIENT_ID
        tenant-id: TENANT_ID
        client-secret: CLIENT_SECRET
        subscription-id: SUB_ID
        resource-group: RESOURCE-GROUP-NAME
        scale-set: SCALE-SET-NAME
        tag: TAG-NAME=HZLCAST001
        hz-port: 5701-5703
```

You will need to setup [Azure Active Directory Service Principal credentials](https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/) for your Azure Subscription for this plugin to work. With the credentials, fill in the placeholder values above.

# Configuring at Azure Side

- `client-id` - *(Optional)* The Azure Active Directory Service Principal client ID.
- `client-secret` - *(Optional)* The Azure Active Directory Service Principal client secret.
- `tenant-id` - *(Optional)* The Azure Active Directory tenant ID.
- `subscription-id` - *(Optional)* The Azure subscription ID.
- `resource-group` - *(Optional)* The Azure [resource group](https://azure.microsoft.com/en-us/documentation/articles/resource-group-portal/) name of the cluster. You can find this in the Azure [portal](https://portal.azure.com) or [CLI](https://npmjs.org/azure-cli).
- `scale-set` - *(Optional)* The Azure [VM scale set](https://docs.microsoft.com/en-us/azure/virtual-machine-scale-sets/overview) name of the cluster. If this setting is configured, the plugin will search for instances over the resources only within this scale set.
- `tag` - *(Optional)* The key-value pair of the tag on the Hazelcast vm resources. The format should be as `key=value`.
- `hz-port` - *(Optional)* The port range where Hazelcast is expected to be running. The format should be as "5701" or "5701-5703". The default value is "5701-5703".

**Notes**

* You should configure all or none of `client-id`, `client-secret`, and `tenant-id` settings. If you *do not* configure all of them, the plugin will try to retrieve the Azure REST API access token from the [instance metadata service](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/instance-metadata-service). 
* If you use the plugin in the Hazelcast Client running outside of the Azure network, then the following settings are mandatory:  `client-id`, `client-secret`, and `tenant-id`     
* If you *do not* configure any of the `subscription-id`, `resource-group`, or `scale-set` settings, again the plugin will try to retrieve these settings' current values using instance metadata service.
* If you *do not* configure `tag` setting, the plugin will search for instances over all available resources. 

The only requirement is that every VM can access each other either by private or public IP address. Also, the resources should have the managed identity with correct access roles in order to use instance metadata service.

If you don't setup the correct access roles on Azure environment and try to use plugin without `client-id`, `client-secret`, and `tenant-id` settings, then you will see an exception similar below:

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

## Minimal Configuration Example

If you setup your Azure managed identities with the correct access roles, you will need just the following minimal configuration to setup Hazelcast Azure Discovery:

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

# Using Azure With ZONE_AWARE Partition Group

When you use Azure plugin as discovery provider, you can configure Hazelcast Partition Grouping with Azure. You need to add fault domain or DNS domain to your machines. So machines will be grouped with respect to their fault or DNS domains.
For more information please read: http://docs.hazelcast.org/docs/3.7/manual/html-single/index.html#partition-group-configuration.

```xml
<partition-group enabled="true" group-type="ZONE_AWARE" />
```

# Automated Deployment

You can also use the [Azure Hazelcast Template](https://github.com/Azure/azure-quickstart-templates/tree/master/hazelcast-vm-cluster) to automatically deploy a Hazelcast cluster which uses this plugin.
