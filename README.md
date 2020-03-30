# Hazelcast Discovery Plugin for AWS

This repository contains a plugin which provides the automatic Hazelcast member discovery in the Amazon Web Services Platform.

## Requirements

* Hazelcast 3.6+
* Linux Kernel 3.19+ (TCP connections may get stuck when used with older Kernel versions, resulting in undefined timeouts)
* Versions compatibility:
  * hazelcast-aws 3+ is compatible with hazelcast 4+
  * hazelcast-aws 2.4 is compatible with hazelcast 3.12.x
  * hazelcast-aws 2.3 is compatible with hazelcast 3.11.x
  * hazelcast-aws 2.2 is compatible with older hazelcast versions

## Embedded mode

To use Hazelcast embedded in your application, you need to add the plugin dependency into your Maven/Gradle file. Then, when you provide `hazelcast.xml`/`hazelcast.yaml` as presented below or an equivalent Java-based configuration, your Hazelcast instances discover themselves automatically.

#### Maven

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast-aws</artifactId>
  <version>${hazelcast-aws.version}</version>
</dependency>
```

#### Gradle

```groovy
compile group: "com.hazelcast", name: "hazelcast-aws", version: "${hazelcast-aws.version}"
```

## Understanding AWS Discovery Strategy

Hazelcast member starts by fetching a list of all instances (accessible by the user) filtered by region, security group, and instance tag key/value. Then, each instance is checked one-by-one with its IP and each of the ports defined in the `hz-port` property. When a member is discovered under IP:PORT, then it joins the cluster.

If users want to create multiple Hazelcast clusters in one region, then they need to manually tag the instances.

## Configuration

The plugin supports **Members Discovery SPI** and **Zone Aware** features.

### Hazelcast Members Discovery SPI

Make sure you have the `hazelcast-aws.jar` dependency in your classpath. Then, you can configure Hazelcast in one of the following manners.

#### XML Configuration

```xml
<hazelcast>
  <network>
    <join>
      <multicast enabled="false"/>
      <aws enabled="true">
        <access-key>my-access-key</access-key>
        <secret-key>my-secret-key</secret-key>
        <region>us-west-1</region>
        <security-group-name>hazelcast</security-group-name>
        <tag-key>aws-test-cluster</tag-key>
        <tag-value>cluster1</tag-value>
        <hz-port>5701-5708</hz-port>
        <connection-retries>3</connection-retries>
      </aws>
    </join>
  </network>
</hazelcast>
```

#### YAML Configuration

```yaml
hazelcast:
  network:
      join:
        multicast:
          enabled: false
        aws:
          enabled: true
          access-key: my-access-key
          secret-key: my-secret-key
          iam-role: dummy
          region: us-west-1
          host-header: ec2.amazonaws.com
          security-group-name: hazelcast-sg
          tag-key: type
          tag-value: hz-nodes
          hz-port: 5701-5708
          connection-retries: 3
```

#### Java-based Configuration

```java
config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
config.getNetworkConfig().getJoin().getAwsConfig().setEnabled(true)
      .setProperty("access-key", "my-access-key")
      .setProperty("secret-key", "my-secret-key")
      .setProperty("region", "us-west-1")
      .setProperty("security-group-name", "hazelcast")
      .setProperty("tag-key", "aws-test-cluster")
      .setProperty("tag-value", "cluster1")
      .setProperty("hz-port", "5701-5708")
      .setProperty("connection-retries", 3);
```

Here are the definitions of the properties

* `access-key`, `secret-key`: access and secret keys of your account on EC2; if not set, `iam-role` is used
* `iam-role`: AWS IAM Role to fetch credentials (used if `access-key`/`secret-key` not specified); if not set, the default IAM Role assigned to EC2 Instance is used
* `region`: region where Hazelcast members are running; if not set, `us-east-1` region is used
* `host-header`: URL that is the entry point for a web service; it is optional
* `security-group-name`: filter to look only for EC2 Instances with the given security group; it is optional
* `tag-key`, `tag-value`: filter to look only for EC2 Instances with the given `tag-key`/`tag-value`; they are optional
* `connection-timeout-seconds`: maximum amount of time Hazelcast will try to connect to a well known member before giving up; setting this value too low could mean that a member is not able to connect to a cluster; setting the value too high means that member startup could slow down because of longer timeouts (for example, when a well known member is not up); its default value is 5
* `hz-port`: a range of ports where the plugin looks for Hazelcast members; if not set, the default value `5701-5708` is used
* `connection-retries`: number of retries while connecting to AWS Services; default to 10

Note that:
* If you don't specify any of the properties, then the plugin uses the IAM Role assigned to EC2 Instance and forms a cluster from all Hazelcast members running in the default region `us-east-1`
* If you use the plugin in the Hazelcast Client running outside of the AWS network, then the following parameters are mandatory: `access-key` and `secret-key`

### Zone Aware

When using `ZONE_AWARE` configuration, backups are created in the other Availability Zone.

#### XML Configuration

```xml
<partition-group enabled="true" group-type="ZONE_AWARE" />
```

#### YAML Configuration

```yaml
hazelcast:
  partition-group:
    enabled: true
    group-type: ZONE-AWARE
```

#### Java-based Configuration

```java
config.getPartitionGroupConfig()
    .setEnabled(true)
    .setGroupType(MemberGroupType.ZONE_AWARE);
```

***NOTE:*** *When using the `ZONE_AWARE` partition grouping, a cluster spanning multiple Availability Zones (AZ) should have an equal number of members in each AZ. Otherwise, it will result in uneven partition distribution among the members.*


### Hazelcast Client with Discovery SPI

If Hazelcast Client is run inside AWS, then the configuration is exactly the same as for the Member.

If Hazelcast Client is run outside AWS, then you always need to specify the following parameters:
- `access-key`, `secret-key` - IAM role cannot be used from outside AWS
- `use-public-ip` - must be set to `true`

Following are example declarative and programmatic configuration snippets.

#### XML Configuration

```xml
<hazelcast-client>
  <network>
    <aws enabled="true">
      <access-key>my-access-key</access-key>
      <secret-key>my-secret-key</secret-key>
      <region>us-west-1</region>
      <security-group-name>hazelcast</security-group-name>
      <tag-key>aws-test-cluster</tag-key>
      <tag-value>cluster1</tag-value>
      <hz-port>5701-5708</hz-port>
      <connection-retries>3</connection-retries>
      <use-public-ip>true</use-public-ip>
    </aws>
  </network>
</hazelcast-client>
```

#### YAML Configuration

```yaml
hazelcast-client:
  network:
    aws:
      enabled: true
      access-key: TEST_ACCESS_KEY
      secret-key: TEST_SECRET_KEY
      region: us-east-1
      security-group-name: hazelcast-sg
      tag-key: type
      tag-value: hz-nodes
      connection-retries: 3
      use-public-ip: true
```

#### Java-based Configuration

```java
clientConfig.getAwsConfig().setEnabled(true)
      .setProperty("access-key", "my-access-key")
      .setProperty("secret-key", "my-secret-key")
      .setProperty("region", "us-west-1")
      .setProperty("security-group-name", "hazelcast")
      .setProperty("tag-key", "aws-test-cluster")
      .setProperty("tag-value", "cluster1")
      .setProperty("hz-port", "5701-5708")
      .setProperty("connection-retries", 3)
      .setProperty("use-public-ip", "true");
```

## Configuration for AWS ECS

In order to enable discovery within AWS ECS Cluster, within `taskdef.json` or container settings, Hazelcast member should be bind to `host` network. Therefore, proper json representation for task should contain below segment:
```
"networkMode": "host"
```

Also, cluster member should have below interface binding in `hazelcast.xml`/`hazelcast.yaml` configuration file.

```xml
<interfaces enabled="true">
    <interface>10.0.*.*</interface>
</interfaces>
```

```yaml
hazelcast:
  network:
    interfaces:
      enabled: true
      interfaces:
        - 10.0.*.*
```
Please note that `10.0.*.*` value depends on your CIDR block definition.
If more than one `subnet` or `custom VPC` is used for cluster, it should be checked that `container instances` within cluster have network connectivity or have `tracepath` to each other. 

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

A solution that may sound simple and good (but is actually not recommended) is to use **[Hazelcast Graceful Shutdown](http://docs.hazelcast.org/docs/3.9.4/manual/html-single/index.html#how-can-i-shutdown-a-hazelcast-member)** as a hook on the EC2 Instance Termination. In other words, without any Autoscaling-specific features (like Lifecycle Hooks), you could adapt the EC2 Instance to wait for the Hazelcast member to shutdown before terminating the instance.

Such solution may work correctly, however is definitely not recommended for the following reasons:

* [AWS Autoscaling documentation](https://docs.aws.amazon.com/autoscaling/ec2/userguide/what-is-amazon-ec2-auto-scaling.html) does not specify the instance termination process, so it's hard to rely on anything
* Some sources ([here](https://stackoverflow.com/questions/11208869/amazon-ec2-autoscaling-down-with-graceful-shutdown)) specify that it's possible to gracefully shut down the processes, however after 20 seconds (which may not be enough for Hazelcast) the processes can be killed anyway
* The [Amazon's recommended way](https://docs.aws.amazon.com/autoscaling/ec2/userguide/lifecycle-hooks.html) to deal with graceful shutdowns is to use Lifecycle Hooks

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
