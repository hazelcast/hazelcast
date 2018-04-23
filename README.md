***NOTE:*** 
*hazelcast-cloud module has been renamed as hazelcast-aws module (starting with Hazelcast 3.7.3). If you want to use AWS Discovery, you should add the library hazelcast-aws JAR to your environment.*

# Table of Contents

  * [Requirements](#requirements)
  * [Discovering Members within EC2 Cloud](#discovering-members-within-ec2-cloud)
    * [Zone Aware Support](#zone-aware-support)
    * [Configuring Hazelcast Members with Discovery SPI](#configuring-hazelcast-members-with-discovery-spi)
    * [Configuring Hazelcast Members for AWS ECS](#configuring-hazelcast-members-for-aws-ecs)
    * [Configuring Hazelcast Client with Discovery SPI](#configuring-hazelcast-client-with-discovery-spi)
    * [Configuring with "AwsConfig" (Deprecated)](#configuring-with-awsconfig-deprecated)
  * [IAM Roles](#iam-roles)
    * [IAM Roles in ECS Environment](#iam-roles-in-ecs-environment)
    * [Policy for IAM User](#policy-for-iam-user)
  * [AWS Autoscaling](#aws-autoscaling)
    * [AWS Autoscaling using Lifecycle Hooks](#aws-autoscaling-using-lifecycle-hooks)
      * [AWS Autoscaling Architecture](#aws-autoscaling-architecture)
      * [Lifecycle Hook Listener Script](#lifecycle-hook-listener-script)
    * [AWS Autoscaling Alternative Solutions](#aws-autoscaling-alternative-solutions)
      * [Cooldown Period](#cooldown-period)
      * [Graceful Shutdown](#graceful-shutdown)
  * [AWSClient Configuration](#awsclient-configuration)
  * [Debugging](#debugging)
  * [Hazelcast Performance on AWS](#hazelcast-performance-on-aws)
    * [Selecting EC2 Instance Type](#selecting-ec2-instance-type)
    * [Dealing with Network Latency](#dealing-with-network-latency)
    * [Selecting Virtualization](#selecting-virtualization)

## Requirements

- Hazelcast 3.6+
- Linux Kernel 3.19+ (TCP connections may get stuck when used with older Kernel versions, resulting in undefined timeouts)

## Discovering Members within EC2 Cloud

Hazelcast supports EC2 auto-discovery. It is useful when you do not want to provide or you cannot provide the list of possible IP addresses. 

There are two possible ways to configure your cluster to use EC2 auto-discovery.
You can either choose to configure your cluster with `AwsConfig` (or `aws` element in your XML config)
or you can choose configuring AWS discovery using [Discovery SPI](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#discovery-spi) implementation.
Starting with hazelcast-aws-1.2, it's strongly recommended to use Discovery SPI implementation as
the former one will be deprecated.

### Zone Aware Support

***NOTE:*** 
ZONE_AWARE configuration is only valid when you use Hazelcast Discovery SPI based configuration with `<discovery-strategies>`. `<aws>` based configuration is still using old implementation and does not support ZONE_AWARE feature

Zone Aware Support is available for Hazelcast Client 3.8.6 and newer releases.

As a discovery service, Hazelcast AWS plugin put the zone information into the Hazelcast's member attributes map during the discovery process. 
Please see the [Defining Member Attributes section](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#defining-member-attributes) 
to learn about the member attributes.

When using `ZONE_AWARE` configuration, backups are created in the other AZs. 
Each zone will be accepted as one partition group.

Following are declarative and programmatic configuration snippets that show how to enable `ZONE_AWARE` grouping.

```xml
<partition-group enabled="true" group-type="ZONE_AWARE" />
```

```java
Config config = ...;
PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
partitionGroupConfig.setEnabled( true )
    .setGroupType( MemberGroupType.ZONE_AWARE );
```

### Configuring Hazelcast Members with Discovery SPI

- Add the *hazelcast-aws.jar* dependency to your project. The hazelcast-aws plugin does not depend on any other third party modules.
- Disable join over multicast, TCP/IP and AWS by setting the `enabled` attribute of the related tags to `false`.
- Enable Discovery SPI by adding "hazelcast.discovery.enabled" property to your config.

Following are example declarative and programmatic configuration snippets:

```xml
 <hazelcast>
   ...
  <properties>
     <property name="hazelcast.discovery.enabled">true</property>
  </properties>
  <network>
    ...
    <join>
        <tcp-ip enabled="false"></tcp-ip>
        <multicast enabled="false"/>
        <aws enabled="false" />
        <discovery-strategies>
            <!-- class equals to the DiscoveryStrategy not the factory! -->
            <discovery-strategy enabled="true" class="com.hazelcast.aws.AwsDiscoveryStrategy">
                <properties>
                   <property name="access-key">my-access-key</property>
                   <property name="secret-key">my-secret-key</property>
                   <property name="iam-role">s3access</property>
                   <property name="region">us-west-1</property>
                   <property name="host-header">ec2.amazonaws.com</property>
                   <property name="security-group-name">hazelcast</property>
                   <property name="tag-key">aws-test-cluster</property>
                   <property name="tag-value">cluster1</property>
                   <property name="hz-port">5701</property>
                </properties>
            </discovery-strategy>
        </discovery-strategies>
    </join>
  </network>
 </hazelcast>
```

```java
        Config config = new Config();
        config.getProperties().setProperty("hazelcast.discovery.enabled", "true");
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getTcpIpConfig().setEnabled(false);
        joinConfig.getMulticastConfig().setEnabled(false);
        joinConfig.getAwsConfig().setEnabled(false);
        AwsDiscoveryStrategyFactory awsDiscoveryStrategyFactory = new AwsDiscoveryStrategyFactory();
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("access-key","my-access-key");
        properties.put("secret-key","my-secret-key");
        properties.put("iam-role","s3access");
        properties.put("region","us-west-1");
        properties.put("host-header","ec2.amazonaws.com");
        properties.put("security-group-name","hazelcast");
        properties.put("tag-key","aws-test-cluster");
        properties.put("tag-value","cluster1");
        properties.put("hzPort","5701");
        DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(awsDiscoveryStrategyFactory, properties);
        joinConfig.getDiscoveryConfig().addDiscoveryStrategyConfig(discoveryStrategyConfig);
        
        //if you want to configure multiple discovery strategies at once
        ArrayList<DiscoveryStrategyConfig> discoveryStrategyConfigs = new ArrayList<DiscoveryStrategyConfig>();
        joinConfig.getDiscoveryConfig().setDiscoveryStrategyConfigs(discoveryStrategyConfigs);
```

Here are the definitions of the properties

* `access-key`, `secret-key`: Access and secret keys of your account on EC2.
* `iam-role`: If you do not want to use access key and secret key, you can specify `iam-role`. Hazelcast-aws plugin fetches your credentials by using your IAM role. It is optional.
* `region`: The region where your members are running. Default value is `us-east-1`. You need to specify this if the region is other than the default one.
* `host-header`: The URL that is the entry point for a web service. It is optional.
* `security-group-name`: Name of the security group you specified at the EC2 management console. It is used to narrow the Hazelcast members to be within this group. It is optional.
* `tag-key`, `tag-value`: To narrow the members in the cloud down to only Hazelcast members, you can set these parameters as the ones you specified in the EC2 console. They are optional.
* `connection-timeout-seconds`: The maximum amount of time Hazelcast will try to connect to a well known member before giving up. Setting this value too low could mean that a member is not able to connect to a cluster. Setting the value too high means that member startup could slow down because of longer timeouts (for example, when a well known member is not up). Increasing this value is recommended if you have many IPs listed and the members cannot properly build up the cluster. Its default value is 5.
* `hz-port`: You can set searching for other ports rather than 5701 if you've members on different ports. It is optional.

### Configuring Hazelcast Members for AWS ECS

In order to enable discovery within AWS ECS Cluster, within `taskdef.json` or container settings, Hazelcast member should be bind to `host` network. Therefore, proper json representation for task should contain below segment:
```
"networkMode": "host"
```

Also, cluster member should have below interface binding in `hazelcast.xml` configuration file.
```
<interfaces enabled="true">
    <interface>10.0.*.*</interface>
</interfaces>
```
Please note that `10.0.*.*` value depends on your CIDR block definition.
If more than one `subnet` or `custom VPC` is used for cluster, it should be checked that `container instances` within cluster have newtork connectivity or have `tracepath` to each other. 

### Configuring Hazelcast Client with Discovery SPI

- Add the *hazelcast-aws.jar* dependency to your project. The hazelcast-aws plugin does not depend on any other third party modules.
- Enable Discovery SPI by adding "hazelcast.discovery.enabled" property to your config.
- Enable public/private IP address translation using "hazelcast.discovery.public.ip.enabled" if your Hazelcast Client is not in AWS.

Following are example declarative and programmatic configuration snippets:

```xml
 <hazelcast-client>
   ...
  <properties>
    <property name="hazelcast.discovery.enabled">true</property>
    <property name="hazelcast.discovery.public.ip.enabled">true</property>
  </properties>

  <network>
    <discovery-strategies>
        <!-- class equals to the DiscoveryStrategy not the factory! -->
        <discovery-strategy enabled="true" class="com.hazelcast.aws.AwsDiscoveryStrategy">
            <properties>
               <property name="access-key">my-access-key</property>
               <property name="secret-key">my-secret-key</property>
               <property name="iam-role">s3access</property>
               <property name="region">us-west-1</property>
               <property name="host-header">ec2.amazonaws.com</property>
               <property name="security-group-name">hazelcast</property>
               <property name="tag-key">aws-test-cluster</property>
               <property name="tag-value">cluster1</property>
               <property name="hz-port">5701</property>
            </properties>
        </discovery-strategy>
    </discovery-strategies>
  </network>
 </hazelcast-client>
```

```java
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getProperties().setProperty("hazelcast.discovery.enabled", "true");
        //if your Hazelcast Client is not in AWS
        clientConfig.getProperties().setProperty("hazelcast.discovery.public.ip.enabled", "true");
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("access-key","my-access-key");
        properties.put("secret-key","my-secret-key");
        properties.put("iam-role","s3access");
        properties.put("region","us-west-1");
        properties.put("host-header","ec2.amazonaws.com");
        properties.put("security-group-name","hazelcast");
        properties.put("tag-key","aws-test-cluster");
        properties.put("tag-value","cluster1");
        properties.put("hzPort","5701");
        AwsDiscoveryStrategyFactory awsDiscoveryStrategyFactory = new AwsDiscoveryStrategyFactory();
        DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(awsDiscoveryStrategyFactory, properties);
        ClientNetworkConfig clientNetworkConfig = clientConfig.getNetworkConfig();
        clientNetworkConfig.getDiscoveryConfig().addDiscoveryStrategyConfig(discoveryStrategyConfig);
        
        //if you want to configure multiple discovery strategies at once
        ArrayList<DiscoveryStrategyConfig> discoveryStrategyConfigs = new ArrayList<DiscoveryStrategyConfig>();
        clientNetworkConfig.getDiscoveryConfig().setDiscoveryStrategyConfigs(discoveryStrategyConfigs);
```


List of available properties and their documentation can be found at [AwsProperties.java](https://github.com/hazelcast/hazelcast-aws/blob/master/src/main/java/com/hazelcast/aws/AwsProperties.java)

### Configuring with "AwsConfig" (Deprecated)

- Add the *hazelcast-aws.jar* dependency to your project. The hazelcast-aws plugin does not depend on any other third party modules.
- Disable join over multicast and TCP/IP by setting the `enabled` attribute of the `multicast` element to "false", and set the `enabled` attribute of the `tcp-ip` element to "false".
- Set the `enabled` attribute of the `aws` element to "true".
- Within the `aws` element, provide your credentials (access and secret key), your region, etc.

The following is an example declarative configuration.

```xml
 <hazelcast>
   ...
  <network>
    ...
    <join>
      <multicast enabled="false"></multicast>
      <tcp-ip enabled="false"></tcp-ip>
      <aws enabled="true">
        <access-key>my-access-key</access-key>
        <secret-key>my-secret-key</secret-key>
        <iam-role>s3access</iam-role>
        <region>us-west-1</region>
        <host-header>ec2.amazonaws.com</host-header>
        <security-group-name>hazelcast-sg</security-group-name>
        <tag-key>type</tag-key>
        <tag-value>hz-members</tag-value>
      </aws>
    </join>
  </network>
 </hazelcast>
```  

Here are the definitions of `aws` element's attributes and sub-elements:

* `enabled`: Specifies whether the EC2 discovery is enabled or not, true or false.
* `access-key`, `secret-key`: Access and secret keys of your account on EC2.
* `iam-role`: If you do not want to use access key and secret key, you can specify `iam-role`. Hazelcast-aws plugin fetches your credentials by using your IAM role. It is optional.
* `region`: The region where your members are running. Default value is `us-east-1`. You need to specify this if the region is other than the default one.
* `host-header`: The URL that is the entry point for a web service. It is optional.
* `security-group-name`: Name of the security group you specified at the EC2 management console. It is used to narrow the Hazelcast members to be within this group. It is optional.
* `tag-key`, `tag-value`: To narrow the members in the cloud down to only Hazelcast members, you can set these parameters as the ones you specified in the EC2 console. They are optional.
* `connection-timeout-seconds`: The maximum amount of time Hazelcast will try to connect to a well known member before giving up. Setting this value too low could mean that a member is not able to connect to a cluster. Setting the value too high means that member startup could slow down because of longer timeouts (for example, when a well known member is not up). Increasing this value is recommended if you have many IPs listed and the members cannot properly build up the cluster. Its default value is 5.

NOTE: If both iamrole and secretkey/accesskey are defined, hazelcast-aws will use iamrole to retrieve credentials and ignore defined secretkey/accesskey pair

## IAM Roles

hazelcast-aws strongly recommends to use IAM Roles. When `iam-role` tag defined in hazelcast configuration, hazelcast-aws fetches your credentials by using defined iam-role name. If you want to use iam-role assigned to your machine, you don't have to define anything. hazelcast-aws will automatically retrieve credentials using default iam-role.

### IAM Roles in ECS Environment

hazelcast-aws supports ECS and will fetch default credentials if hazelcast is deployed into ECS environment. You don't have to configure `iam-role` tag. However, if you have a specific IAM Role to use, you can still use it via `iam-role` tag.

### Policy for IAM User

If you are using IAM role configuration (`iam-role`) for EC2 discovery, you need to give the following policy to your IAM user at the least:

`"ec2:DescribeInstances"`
```
{
  "Version": "XXXXXXXX",
  "Statement": [
    {
      "Sid": "XXXXXXXX",
      "Action": [
        "ec2:DescribeInstances"
      ],
      "Effect": "Allow",
      "Resource": "*"
    }
  ]
}
```

## AWS Autoscaling

There are specific requirements for the Hazelcast cluster to work correctly in the AWS Autoscaling Group:
* the number of instances must change by 1 at the time
* when an instance is launched or terminated, the cluster must be in the safe state

Otherwise, there is a risk of data loss or an impact on performance.

The recommended solution is to use **[Autoscaling Lifecycle Hooks](https://docs.aws.amazon.com/autoscaling/ec2/userguide/lifecycle-hooks.html)** with **Amazon SQS**, and the **custom lifecycle hook listener script**. If your cluster is small and predictable, then you can try the simpler alternative solution using **[Cooldown Period](https://docs.aws.amazon.com/autoscaling/ec2/userguide/Cooldown.html)**.

### AWS Autoscaling using Lifecycle Hooks

This solution is recommended and safe, no matter what your cluster and data sizes are. Nevertheless, if you find it too complex, you may want to try alternative solutions.

#### AWS Autoscaling Architecture

The necessary autoscaling architecture is presented on the diagram.

![architecture](markdown/images/aws-autoscaling-architecture.png)

The following activities must be performed to set up the Hazelcast AWS Autoscaling process:

* Create **AWS SQS** queue
* Add **`lifecycle_hook_listener.sh`** to each instance
  * The `lifecycle_hook_listener.sh` script can be started as the User Data script
  * Alternatively, it's possible to create a separate dedicated EC2 Instance running only `lifecycle_hook_listener.sh` in the same network and leave the autoscaled EC2 Instances only for Hazelcast members
* Set `Scaling Policy` to `Step scaling` and increase/decrease always by adding/removing `1 instance`
* Create `Lifecycle Hooks`
  * Instance Launch Hook
  * Instance Terminate Hook

#### Lifecycle Hook Listener Script

The `lifecycle_hook_listener.sh` script takes one argument as a parameter: `queue_name` (AWS SQS name). It performs operations that can be expressed in the following pseudocode.

```
while true:
    message = receive_message_from(queue_name)
    instance_ip = extract_instance_ip_from(message)
    while not is_cluster_safe(instance_ip):
        sleep 5
    send_continue_message
```

### AWS Autoscaling Alternative Solutions

The solutions below are not recommended, since they may not operate well under certain conditions. Therefore, please use them with caution.

#### Cooldown Period

[Cooldown Period](https://docs.aws.amazon.com/autoscaling/ec2/userguide/Cooldown.html) is a statically defined time interval that AWS Autoscaling Group waits before the next autoscaling operation takes place. If your cluster is small and predictable, then you can use it instead of Lifecycle Hooks. 

* Set `Scaling Policy` to `Step scaling` and increase/decrease always by adding/removing `1 instance`
* Set `Cooldown Period` to a reasonable value (which depends on the cluster and data size)

Note the drawbacks:

 * If the cluster contains a significant amount of data, it may be impossible to define one static cooldown period
 * Even if the cluster comes back to the safe state quicker, the next operation needs to wait the defined cooldown period

#### Graceful Shutdown

A solution that may sound simple and good (but is actually not recommended) is to use **[Hazelcast Graceful Shutdown](http://docs.hazelcast.org/docs/latest-development/manual/html/FAQ.html#page_How+can+I+shutdown+a+Hazelcast+member)** as a hook on the EC2 Instance Termination. In other words, without any Autoscaling-specific features (like Lifecycle Hooks), you could adapt the EC2 Instance to wait for the Hazelcast member to shutdown before terminating the instance.

Such solution may work correctly, however is definitely not recommended for the following reasons:

* [AWS Autoscaling documentation](https://docs.aws.amazon.com/autoscaling/ec2/userguide/what-is-amazon-ec2-auto-scaling.html) does not specify the instance termination process, so it's hard to rely on anything
* Some sources ([here](https://stackoverflow.com/questions/11208869/amazon-ec2-autoscaling-down-with-graceful-shutdown)) specify that it's possible to gracefully shut down the processes, however after 20 seconds (which may not be enough for Hazelcast) the processes can be killed anyway
* The [Amazon's recommended way](https://docs.aws.amazon.com/autoscaling/ec2/userguide/lifecycle-hooks.html) to deal with graceful shutdowns is to use Lifecycle Hooks

## AWSClient Configuration

To make sure EC2 instances are found correctly, you can use the AWSClient class. It determines the private IP addresses of EC2 instances to be connected. Give the AWSClient class the values for the parameters that you specified in the aws element, as shown below. You will see whether your EC2 instances are found.

```java
public static void main( String[] args )throws Exception{ 
  AwsConfig config = new AwsConfig(); 
  config.setAccessKey( ... ) ;
  config.setSecretKey( ... );
  config.setRegion( ... );
  config.setSecurityGroupName( ... );
  config.setTagKey( ... );
  config.setTagValue( ... );
  config.setEnabled( true );
  AWSClient client = new AWSClient( config );
  List<String> ipAddresses = client.getPrivateIpAddresses();
  System.out.println( "addresses found:" + ipAddresses ); 
  for ( String ip: ipAddresses ) {
    System.out.println( ip ); 
  }
}
```

## Debugging

When needed, Hazelcast can log the events for the instances that exist in a region. To see what has happened or to trace the activities while forming the cluster, change the log level in your logging mechanism to `FINEST` or `DEBUG`. After this change, you can also see in the generated log whether the instances are accepted or rejected, and the reason the instances were rejected. Note that changing the log level in this way may affect the performance of the cluster. Please see the [Logging Configuration](http://docs.hazelcast.org/docs/latest-dev/manual/html-single/index.html#logging-configuration) for information on logging mechanisms.


## Hazelcast Performance on AWS

Amazon Web Services (AWS) platform can be an unpredictable environment compared to traditional in-house data centers. This is because the machines, databases or CPUs are shared with other unknown applications in the cloud, causing fluctuations. When you gear up your Hazelcast application from a physical environment to Amazon EC2, you should configure it so that any network outage or fluctuation is minimized and its performance is maximized. This section provides notes on improving the performance of Hazelcast on AWS.

### Selecting EC2 Instance Type

Hazelcast is an in-memory data grid that distributes the data and computation to the members that are connected with a network, making Hazelcast very sensitive to the network. Not all EC2 Instance types are the same in terms of the network performance. It is recommended that you choose instances that have **High** or **10 Gigabit+** network performance for Hazelcast deployments.

You can check latest Instance Types on [Amazon EC2 Instance Types](https://aws.amazon.com/ec2/instance-types/).

### Dealing with Network Latency

Since data is sent and received very frequently in Hazelcast applications, latency in the network becomes a crucial issue. In terms of the latency, AWS cloud performance is not the same for each region. There are vast differences in the speed and optimization from region to region.

When you do not pay attention to AWS regions, Hazelcast applications may run tens or even hundreds of times slower than necessary. The following notes are potential workarounds.

- Create a cluster only within a region. It is not recommended that you deploy a single cluster that spans across multiple regions.
- If a Hazelcast application is hosted on Amazon EC2 instances in multiple EC2 regions, you can reduce the latency by serving the end users requests from the EC2 region which has the lowest network latency. Changes in network connectivity and routing result in changes in the latency between hosts on the Internet. Amazon has a web service (Route 53) that lets the cloud architects use DNS to route end-user requests to the EC2 region that gives the fastest response. This latency-based routing is based on latency measurements performed over a period of time. Please have a look at [Route53](http://docs.aws.amazon.com/Route53/latest/DeveloperGuide/HowDoesRoute53Work.html).
- Move the deployment to another region. The [CloudPing](http://www.cloudping.info/) tool gives instant estimates on the latency from your location. By using it frequently, CloudPing can be helpful to determine the regions which have the lowest latency.
- The [SpeedTest](http://cloudharmony.com/speedtest) tool allows you to test the network latency and also the downloading/uploading speeds.

### Selecting Virtualization

AWS uses two virtualization types to launch the EC2 instances: Para-Virtualization (PV) and Hardware-assisted Virtual Machine (HVM). According to the tests we performed, HVM provided up to three times higher throughput than PV. Therefore, we recommend you use HVM when you run Hazelcast on EC2.

***RELATED INFORMATION***

*You can download the white paper "Amazon EC2 Deployment Guide for Hazelcast IMDG" [here](https://hazelcast.com/resources/amazon-ec2-deployment-guide/).*
