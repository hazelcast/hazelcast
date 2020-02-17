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

**Notes**

* You should configure all or none of `client-id`, `client-secret`, and `tenant-id` settings. If you *do not* configure all of them, the plugin will try to retrieve the Azure REST API access token from the [instance metadata service](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/instance-metadata-service). 
* If you use the plugin in the Hazelcast Client running outside of the Azure network, then the following settings are mandatory:  `client-id`, `client-secret`, and `tenant-id`     
* If you *do not* configure any of the `subscription-id`, `resource-group`, or `scale-set` settings, again the plugin will try to retrieve these settings' current values using instance metadata service.
* If you *do not* configure `tag` setting, the plugin will search for instances over all available resources. 

The only requirement is that every VM can access each other either by private or public IP address. Also, the resources should have the managed identity with correct access roles in order to use instance metadata service.

## Setting up Azure Manage Identities for Resource Groups 

In order to use Azure instance metadata service, you need to configure managed identities with correct access roles. Below, you can find Azure CLI commands to setup a resource group with a managed identity.

Firstly, you should login to Azure before using Azure CLI:

```shell script
az login
```   

You need a Azure [location](https://azure.microsoft.com/en-gb/global-infrastructure/locations/) to run your resources. Please run the following command to see the available location names:

```shell script
az account list-locations -o table
```

Then you can create a resource group with the following command. Please choose a resource name and replace it with `[RESOURCE_NAME]`. For `[LOCATION]` parameter, please use the location name you would like to run your resource in.    

```shell script
az group create --name [RESOURCE_NAME] --location [LOCATION]
``` 

Now, we will create a managed identity using the command below. Please choose an identity name and replace it with `[IDENTITY_NAME]`. For `[RESOURCE_NAME]` parameter, use the resource name that you created in the previous step.

```shell script
az identity create -g [RESOURCE_NAME] -n [IDENTITY_NAME]
```

The output of this command will be similar below. Please keep the [PRINCIPAL_ID] value for later use.

```shell script
{
  "canDelegate": null,
  "id": "/subscriptions/aaaaaaa-bbbb-cccc-dddd-eeeeeeeeee/resourceGroups/[RESOURCE_GROUP]/providers/Microsoft.Authorization/roleAssignments/xxxxxxxx-yyyy-zzzz-tttt-uuuuuuuuuuuu",
  "name": "xxxxxxxx-yyyy-zzzz-tttt-uuuuuuuuuuuu",
  "principalId": "[PRINCIPAL_ID]",
  "principalType": "ServicePrincipal",
  "resourceGroup": "[RESOURCE_GROUP]",
  "roleDefinitionId": "/subscriptions/aaaaaaa-bbbb-cccc-dddd-eeeeeeeeee/providers/Microsoft.Authorization/roleDefinitions/xxxxxxxx-yyyy-zzzz-tttt-kkkkkkkkkkk",
  "scope": "/subscriptions/aaaaaaa-bbbb-cccc-dddd-eeeeeeeeee/resourceGroups/[RESOURCE_GROUP]",
  "type": "Microsoft.Authorization/roleAssignments"
}
```

The last step is to assign `READER` role to our identity. Please run the following command by replacing [PRINCIPAL_ID] from the output of previous command and [RESOURCE_GROUP] with the name of the resource group you created.

```shell script
az role assignment create -g [RESOURCE_GROUP] --role Reader --assignee-object-id [PRINCIPAL_ID]
``` 

If all commands are run successfully, then our resource group has a managed identity assigned with `READER` role over our resource group. Now, if you create VMs in this resource group then Hazelcast Azure Discovery Plugin will be able to use Azure instance metadata service. You will just need to setup your Hazelcast instances with the minimal configuration as explained in the next section.    


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
