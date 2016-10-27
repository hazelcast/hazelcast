# Table of Contents

* [Discovery Implementation for Azure Services](#discovery-implementation-for-azure-services)
* [Getting Started](#getting-started)
* [Compiling with Gradle](#compiling-with-gradle)
* [Configuring at Hazelcast Side](#configuring-at-hazelcast-side)
* [Configuring at Azure Side](#configuring-at-azure-side)
* [Using Azure With ZONE_AWARE Partition Group](#using-azure-with-zone_aware-partition-group)
* [Automated Deployment](#automated-deployment)


# Hazelcast Discovery Plugin for Microsoft Azure

This project provides a discovery strategy for Hazelcast 3.6-RC+1 enabled applications running on Azure. It will provide all Hazelcast instances by returning VMs within your Azure resource group that are tagged with a specified value.

![Architectual diagram](img/azurespi.png)

# Getting Started

To add this plugin to your Java project, add the following lines to either your Maven POM file or Gradle configuration.

For Gradle:

```
repositories {
    jcenter() 
}

dependencies {
    compile 'com.hazelcast.azure:hazelcast-azure:1.0'
}
```

For Maven:

```xml
<dependencies>
    <dependency>
        <groupId>com.hazelcast.azure</groupId>
        <artifactId>hazelcast-azure</artifactId>
        <version>1.0</version>
    </dependency>
</dependencies>
```

# Compiling with Gradle

Run the following command to compile the plugin:

```gradle
compile 'com.hazelcast.azure:hazelcast-azure:1.0'
```

Check the [releases](https://github.com/hazelcast/hazelcast-azure/releases) for the latest version.

# Configuring at Hazelcast Side

Ensure that you have added the package `hazelcast-azure` to your Maven or Gradle configuration as mentioned above.

In your Hazelcast configuration, use the `AzureDiscoveryStrategy` as shown below:

```xml
<network>
    <join>
        <multicast enabled="false"/>
        <tcp-ip enabled="false" />
        <aws enabled="false"/>
        <discovery-strategies>
            <discovery-strategy enabled="true" class="com.hazelcast.azure.AzureDiscoveryStrategy">
                <properties>
                    <property name="client-id">CLIENT_ID</property>
                    <property name="client-secret">CLIENT_SECRET</property>
                    <property name="tenant-id">TENANT_ID</property>
                    <property name="subscription-id">SUB_ID</property>
                    <property name="cluster-id">HZLCAST001</property>
                    <property name="group-name">GROUP-NAME</property>
                </properties>
            </discovery-strategy>
        </discovery-strategies>
    </join>
</network>
```

You will need to setup [Azure Active Directory Service Principal credentials](https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/) for your Azure Subscription for this plugin to work. With the credentials, fill in the placeholder values above.

# Configuring at Azure Side


- `client-id` - The Azure Active Directory Service Principal client ID
- `client-secret` - The Azure Active Directory Service Principal client secret
- `tenant-id` - The Azure Active Directory tenant ID
- `subscription-id` - The Azure subscription ID
- `cluster-id` - The name of the tag on the hazelcast vm resources
- `group-name` - The Azure [resource group](https://azure.microsoft.com/en-us/documentation/articles/resource-group-portal/) name of the cluster. You can find this in the Azure [portal](https://portal.azure.com) or [CLI](https://npmjs.org/azure-cli).

With every Hazelcast Virtual Machine you deploy in your resource group, you need to ensure that each VM is tagged with the value of `cluster-id` defined in your Hazelcast configuration. The only requirement is that every VM can access each other either by private or public IP address.

Read more about how you can [tag your virtual machines](https://azure.microsoft.com/en-us/documentation/articles/virtual-machines-tagging-arm/).

# Using Azure With ZONE_AWARE Partition Group

When you use Azure plugin as discovery provider, you can configure Hazelcast Partition Group configuration with Azure. You need to add fault domain or dns domain to your machines. So machines will be grouped with respect to their fault or dns domains.
For more information please read: http://docs.hazelcast.org/docs/3.7/manual/html-single/index.html#partition-group-configuration

```xml
...
<partition-group enabled="true" group-type="ZONE_AWARE" />
...
```

# Automated Deployment

You can also use the [Azure Hazelcast Template](https://github.com/Azure/azure-quickstart-templates/tree/master/hazelcast-vm-cluster) to automatically deploy a Hazelcast cluster which uses this plugin.
