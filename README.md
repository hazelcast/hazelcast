***NOTE:*** 
*hazelcast-cloud module has been renamed as hazelcast-aws module (starting with Hazelcast 3.7.3). If you want to use AWS Discovery, you should add the library hazelcast-aws JAR to your environment.*

# Table of Contents

  * [Supported Hazelcast Versions](#supported-hazelcast-versions)
  * [Discovering Members within EC2 Cloud](#discovering-members-within-ec2-cloud)
    * [Zone Aware Support](#zone-aware)
  * [IAM Roles](#iam-roles)
    * [IAM Roles in ECS Environment](#iam-roles-in-ecs-environment)
    * [Policy for IAM User](#policy-for-iam-user)
  * [AWSClient Configuration](#awsclient-configuration)
  * [Debugging](#debugging)
  * [Hazelcast Performance on AWS](#hazelcast-performance-on-aws)
    * [Selecting EC2 Instance Type](#selecting-ec2-instance-type)
    * [Dealing with Network Latency](#dealing-with-network-latency)
    * [Selecting Virtualization](#selecting-virtualization)

## Supported Hazelcast Versions

- Hazelcast 3.6+

<br></br>
***NOTE***

*We recommend you to use Linux Kernel versions 3.19 and higher. TCP connections may get stuck when used with older Kernel versions, resulting in undefined timeouts.*

<br></br>

## Discovering Members within EC2 Cloud

Hazelcast supports EC2 auto-discovery. It is useful when you do not want to provide or you cannot provide the list of possible IP addresses. 

There are two possible ways to configure your cluster to use EC2 auto-discovery.
You can either choose to configure your cluster with `AwsConfig` (or `aws` element in your XML config)
or you can choose configuring AWS discovery using [Discovery SPI](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#discovery-spi) implementation.
Starting with hazelcast-aws-1.2, it's strongly recommended to use Discovery SPI implementation as
the former one will be deprecated.

### Zone Aware Support

This support is available for Hazelcast Client 3.8.6 and newer releases.

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

### Configuring AWS Discovery using Discovery SPI for Hazelcast Cluster Members

- Add the *hazelcast-aws.jar* dependency to your project. The hazelcast-aws plugin does not depend on any other third party modules.
- Disable join over multicast, TCP/IP and AWS by setting the `enabled` attribute of the related tags to `false`.
- Enable Discovery SPI by adding "hazelcast.discovery.enabled" property to your config.

The following is an example declarative configuration.

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

Here are the definitions of the properties

* `access-key`, `secret-key`: Access and secret keys of your account on EC2.
* `iam-role`: If you do not want to use access key and secret key, you can specify `iam-role`. Hazelcast-aws plugin fetches your credentials by using your IAM role. It is optional.
* `region`: The region where your members are running. Default value is `us-east-1`. You need to specify this if the region is other than the default one.
* `host-header`: The URL that is the entry point for a web service. It is optional.
* `security-group-name`: Name of the security group you specified at the EC2 management console. It is used to narrow the Hazelcast members to be within this group. It is optional.
* `tag-key`, `tag-value`: To narrow the members in the cloud down to only Hazelcast members, you can set these parameters as the ones you specified in the EC2 console. They are optional.
* `connection-timeout-seconds`: The maximum amount of time Hazelcast will try to connect to a well known member before giving up. Setting this value too low could mean that a member is not able to connect to a cluster. Setting the value too high means that member startup could slow down because of longer timeouts (for example, when a well known member is not up). Increasing this value is recommended if you have many IPs listed and the members cannot properly build up the cluster. Its default value is 5.
* `hz-port`: You can set searching for other ports rather than 5701 if you've members on different ports. It is optional.

### Configuring Hazelcast Cluster Members for AWS ECS 

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

### Configuring AWS Discovery using Discovery SPI for Hazelcast Client

- Add the *hazelcast-aws.jar* dependency to your project. The hazelcast-aws plugin does not depend on any other third party modules.
- Enable Discovery SPI by adding "hazelcast.discovery.enabled" property to your config.
- Enable public/private IP address translation using "hazelcast.discovery.public.ip.enabled" if your Hazelcast Client is not in AWS.

The following is an example declarative configuration.

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

When needed, Hazelcast can log the events for the instances that exist in a region. To see what has happened or to trace the activities while forming the cluster, change the log level in your logging mechanism to `FINEST` or `DEBUG`. After this change, you can also see in the generated log whether the instances are accepted or rejected, and the reason the instances were rejected. Note that changing the log level in this way may affect the performance of the cluster. Please see the <a href="http://docs.hazelcast.org/docs/latest-dev/manual/html-single/index.html#logging-configuration" target="_blank">Logging Configuration</a> for information on logging mechanisms.


## Hazelcast Performance on AWS

Amazon Web Services (AWS) platform can be an unpredictable environment compared to traditional in-house data centers. This is because the machines, databases or CPUs are shared with other unknown applications in the cloud, causing fluctuations. When you gear up your Hazelcast application from a physical environment to Amazon EC2, you should configure it so that any network outage or fluctuation is minimized and its performance is maximized. This section provides notes on improving the performance of Hazelcast on AWS.

### Selecting EC2 Instance Type

Hazelcast is an in-memory data grid that distributes the data and computation to the members that are connected with a network, making Hazelcast very sensitive to the network. Not all EC2 Instance types are the same in terms of the network performance. It is recommended that you choose instances that have **10 Gigabit** or **High** network performance for Hazelcast deployments. Please see the below list for the recommended instances.

* m3.2xlarge - High
* m1.xlarge - High
* c3.2xlarge - High
* c3.4xlarge - High
* c3.8xlarge - 10 Gigabit
* c1.xlarge - High
* cc2.8xlarge - 10 Gigabit
* m2.4xlarge - High
* cr1.8xlarge - 10 Gigabit

### Dealing with Network Latency

Since data is sent and received very frequently in Hazelcast applications, latency in the network becomes a crucial issue. In terms of the latency, AWS cloud performance is not the same for each region. There are vast differences in the speed and optimization from region to region.

When you do not pay attention to AWS regions, Hazelcast applications may run tens or even hundreds of times slower than necessary. The following notes are potential workarounds.

- Create a cluster only within a region. It is not recommended that you deploy a single cluster that spans across multiple regions.
- If a Hazelcast application is hosted on Amazon EC2 instances in multiple EC2 regions, you can reduce the latency by serving the end users` requests from the EC2 region which has the lowest network latency. Changes in network connectivity and routing result in changes in the latency between hosts on the Internet. Amazon has a web service (Route 53) that lets the cloud architects use DNS to route end-user requests to the EC2 region that gives the fastest response. This latency-based routing is based on latency measurements performed over a period of time. Please have a look at <a href="http://docs.aws.amazon.com/Route53/latest/DeveloperGuide/HowDoesRoute53Work.html" target="_blank">Route53</a>.
- Move the deployment to another region. The <a href="http://www.cloudping.info/" target="_blank">CloudPing</a> tool gives instant estimates on the latency from your location. By using it frequently, CloudPing can be helpful to determine the regions which have the lowest latency.
- The <a href="http://cloudharmony.com/speedtest" target="_blank">SpeedTest</a> tool allows you to test the network latency and also the downloading/uploading speeds.

### Selecting Virtualization

AWS uses two virtualization types to launch the EC2 instances: Para-Virtualization (PV) and Hardware-assisted Virtual Machine (HVM). According to the tests we performed, HVM provided up to three times higher throughput than PV. Therefore, we recommend you use HVM when you run Hazelcast on EC2.






***RELATED INFORMATION***

*You can download the white paper "Amazon EC2 Deployment Guide for Hazelcast IMDG" <a href="https://hazelcast.com/resources/amazon-ec2-deployment-guide/" target="_blank">here</a>.*







