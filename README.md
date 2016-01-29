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
    compile 'org.hazelcast.azure:hazelcast-azure:1.0'
}
```

## Maven

```xml
<dependencies>
    <dependency>
        <groupId>org.hazelcast.azure</groupId>
        <artifactId>hazelcast-azure</artifactId>
        <version>1.0</version>
    </dependency>

    <!-- include your preferred javax.ws.rs-api 
         (for the https://github.com/OrbitzWorldwide/consul-client dependency)
         implementation - see gradle example above 
    -->
</dependencies>

<repositories>
    <repository>
        <snapshots>
            <enabled>false</enabled>
        </snapshots>
        <id>central</id>
        <name>bintray</name>
        <url>http://jcenter.bintray.com</url>
    </repository>
</repositories>
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
                    <property name="hzlcst-cluster-id">HZLCAST001</property>
                    <property name="group-name">GROUP-NAME</property>
                </properties>
            </discovery-strategy>
        </discovery-strategies>
    </join>
</network>
```

You'll need to setup [Azure Active Directory Service Principal credentials](https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/) for your Azure Subscription for this plugin to work. With the credentials, fill in the placeholder values above.

## Azure Configuration

With every Hazelcast Virtual Machine you deploy in your resource group, you need to ensure that each VM is tagged with the value of `hzlcst-cluster-id` defined in your Hazelcast configuration. The only requirement is that every VM can access each other either by private or public IP address.

Read more about how you can [tag your virtual machines](https://azure.microsoft.com/en-us/documentation/articles/virtual-machines-tagging-arm/).
