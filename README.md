# hazelcast-azure

This project provides a DiscoveryStrategy for Hazelcast 3.6-RC+1 enabled applications running on Azure. It will provide all Hazelcast instances by returning VMs within your Azure resource group that are tagged with a specified value.

![Architectual diagram](img/azurespi.png)

# Getting Started

To add this plugin to your java project, add the following to either you maven pom or gradle configuration.

## Gradle

```
repositories {
    jcenter() 
}

dependencies {
    compile 'com.hazelcast.azure:hazelcast-azure:1.0'
}
```

## Maven

```xml
<dependencies>
    <dependency>
        <groupId>com.hazelcast.azure</groupId>
        <artifactId>hazelcast-azure</artifactId>
        <version>1.0</version>
    </dependency>
</dependencies>
```

## Gradle

```gradle
compile 'com.hazelcast.azure:hazelcast-azure:1.0'
```

Check the [releases](https://github.com/sedouard/hazelcast-azure/releases) for the latest version.

# Configuration

## Hazelcast Configuration

Ensure you've added the hazelcast `hazelcast-azure` package in your maven or gradle configuration as mentioned above.

In your Hazelcast configuration, use the `AzureDiscoveryStrategy`:

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

You'll need to setup [Azure Active Directory Service Principal credentials](https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/) for your Azure Subscription for this plugin to work. With the credentials, fill in the placeholder values above.

## Azure Configuration

- `client-id` - The Azure Active Directory Service Principal client ID
- `client-secret` - The Azure Active Directory Service Principal client secret
- `tenant-id` - The Azure Active Directory tenant id
- `subscription-id` - The Azure subscription id
- `cluster-id` - The name of the tag on the hazelcast vm resources
- `group-name` - The Azure [resource group](https://azure.microsoft.com/en-us/documentation/articles/resource-group-portal/) name of the cluster. You can find this in the Azure [portal](https://portal.azure.com) or [CLI](https://npmjs.org/azure-cli).

With every Hazelcast Virtual Machine you deploy in your resource group, you need to ensure that each VM is tagged with the value of `cluster-id` defined in your Hazelcast configuration. The only requirement is that every VM can access each other either by private or public IP address.

Read more about how you can [tag your virtual machines](https://azure.microsoft.com/en-us/documentation/articles/virtual-machines-tagging-arm/).

## Automated Deployment

You can also use the [Azure Hazelcast Template](https://github.com/Azure/azure-quickstart-templates/tree/master/hazelcast-vm-cluster) to automatically deploy a Hazelcast cluster which uses this plugin.
