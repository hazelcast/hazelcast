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

To use Hazelcast embedded in your application, you need to add the plugin dependency into your Maven/Gradle file (or use [hazelcast-all](https://mvnrepository.com/artifact/com.hazelcast/hazelcast-all) which already includes the plugin). Then, when you provide `hazelcast.xml`/`hazelcast.yaml` as presented below or an equivalent Java-based configuration, your Hazelcast instances discover themselves automatically.

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

Hazelcast member starts by fetching a list of all running instances filtered by the plugin parameters (`region`, etc.). Then, each instance is checked one-by-one with its IP and each of the ports defined in the `hz-port` property. When a member is discovered under `IP:PORT`, then it joins the cluster.

Note that this plugin supports [Hazelcast Zone Aware](https://docs.hazelcast.org/docs/latest/manual/html-single/#zone_aware) feature.

The plugin is prepared to work for both **AWS EC2** and **AWS ECS/Fargate** environments. However, note that requirements and plugin properties vary depending on the environment you use.

## EC2 Configuration

The plugin works both for **Hazelcast Member Discovery** and **Hazelcast Client Discovery**.

### EC2 Hazelcast Member Discovery

Make sure that:

* you have the `hazelcast-aws.jar` (or `hazelcast-all.jar`) dependency in your classpath
* your IAM Role has `ec2:DescribeInstances` permission

Then, you can configure Hazelcast in one of the following manners.

#### XML Configuration

```xml
<hazelcast>
  <network>
    <join>
      <multicast enabled="false"/>
      <aws enabled="true">
        <tag-key>my-ec2-instance-tag-key</tag-key>
        <tag-value>my-ec2-instance-tag-value</tag-value>
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
        tag-key: my-ec2-instance-tag-key
        tag-value: my-ec2-instance-tag-value
```

#### Java-based Configuration

```java
config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
config.getNetworkConfig().getJoin().getAwsConfig().setEnabled(true)
      .setProperty("tag-key", "my-ec2-instance-tag-key")
      .setProperty("tag-value", "my-ec2-instance-tag-value");
```

The following properties can be configured (all are optional).


* `access-key`, `secret-key`: access and secret keys of your AWS account; if not set, `iam-role` is used
* `iam-role`: IAM Role attached to EC2 instance used to fetch credentials (if `access-key`/`secret-key` not specified); if not set, default IAM Role attached to EC2 instance is used
* `region`: region where Hazelcast members are running; default is the current region
* `host-header`: `ec2`, `ecs`, or the URL of a EC2/ECS API endpoint; automatically detected by default
* `security-group-name`: filter to look only for EC2 instances with the given security group
* `tag-key`, `tag-value`: filter to look only for EC2 Instances with the given `tag-key`/`tag-value`; multi values supported if comma-separated (e.g. `KeyA,KeyB`); comma-separated values behaves as AND conditions
* `connection-timeout-seconds`, `read-timeout-seconds`: connection and read timeouts when making a call to AWS API; default to `10`
* `connection-retries`: number of retries while connecting to AWS API; default to `3`
* `hz-port`: a range of ports where the plugin looks for Hazelcast members; default is `5701-5708`

Note that if you don't specify any of the properties, then the plugin uses the IAM Role assigned to EC2 Instance and forms a cluster from all Hazelcast members running in same region.

### EC2 Hazelcast Client Configuration

Hazelcast Client discovery parameters are the same as mentioned above.

If Hazelcast Client is run **outside AWS**, then you need to always specify the following parameters:
- `access-key`, `secret-key` - IAM role cannot be used from outside AWS
- `region` - it cannot be detected automatically
- `use-public-ip` - must be set to `true`

Note also that your EC2 instances must have public IP assigned.

Following are example declarative and programmatic configuration snippets.

#### XML Configuration

```xml
<hazelcast-client>
  <network>
    <aws enabled="true">
      <access-key>my-access-key</access-key>
      <secret-key>my-secret-key</secret-key>
      <region>us-west-1</region>
      <tag-key>my-ec2-instance-tag-key</tag-key>
      <tag-value>my-ec2-instance-tag-value</tag-value>
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
      access-key: my-access-key
      secret-key: my-secret-key
      region: us-west-1
      tag-key: my-ec2-instance-tag-key
      tag-value: my-ec2-instance-tag-value
      use-public-ip: true
```

#### Java-based Configuration

```java
clientConfig.getNetworkConfig().getAwsConfig()
      .setEnabled(true)
      .setProperty("access-key", "my-access-key")
      .setProperty("secret-key", "my-secret-key")
      .setProperty("region", "us-west-1")
      .setProperty("tag-key", "my-ec2-instance-tag-key")
      .setProperty("tag-value", "my-ec2-instance-tag-value")
      .setProperty("use-public-ip", "true");
```

## ECS/Fargate Configuration

The plugin works both for **Hazelcast Member Discovery** (forming Hazelcast cluster) and **Hazelcast Client Discovery**.

Note: for the detailed description, check out [Hazelcast Guides: Getting Started with Embedded Hazelcast on ECS](https://guides.hazelcast.org/ecs-embedded/).

### ECS Hazelcast Member Discovery

Make sure that your IAM Task Role has the following permissions:
* `ecs:ListTasks`
* `ecs:DescribeTasks`
* `ec2:DescribeNetworkInterfaces` (needed only if task have public IPs)

Then, you can configure Hazelcast in one of the following manners. Please note that `10.0.*.*` value depends on your VPC CIDR block definition.

#### XML Configuration

```xml
<hazelcast>
  <network>
    <join>
      <multicast enabled="false"/>
      <aws enabled="true" />
    </join>
    <interfaces enabled="true">
      <interface>10.0.*.*</interface>
    </interfaces>
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
    interfaces:
      enabled: true
      interfaces:
        - 10.0.*.*
```

#### Java-based Configuration

```java
config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
config.getNetworkConfig().getJoin().getAwsConfig().setEnabled(true);
config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("10.0.*.*");
```

The following properties can be configured (all are optional).

* `access-key`, `secret-key`: access and secret keys of AWS your account; if not set, IAM Task Role is used
* `region`: region where Hazelcast members are running; default is the current region
* `cluster`: ECS cluster short name or ARN; default is the current cluster
* `family`: filter to look only for ECS tasks with the given family name; mutually exclusive with `service-name`
* `service-name`: filter to look only for ECS tasks from the given service; mutually exclusive with `family`
* `host-header`: `ecs` or the URL of a ECS API endpoint; automatically detected by default
* `connection-timeout-seconds`, `read-timeout-seconds`: connection and read timeouts when making a call to AWS API; default to `10`
* `connection-retries`: number of retries while connecting to AWS API; default to `3`
* `hz-port`: a range of ports where the plugin looks for Hazelcast members; default is `5701-5708`

Note that if you don't specify any of the properties, then the plugin discovers all Hazelcast members running in the current ECS cluster.

### ECS Hazelcast Client Configuration

Hazelcast Client discovery parameters are the same as mentioned above.

If Hazelcast Client is run **outside ECS cluster**, then you need to always specify the following parameters:
- `access-key`, `secret-key` - IAM role cannot be used from outside AWS
- `region` - it cannot be detected automatically
- `cluster` - it cannot be detected automatically
- `use-public-ip` - must be set to `true`

Note also that your ECS Tasks must have public IPs assigned and your IAM Task Role must have `ec2:DescribeNetworkInterfaces` permission.

Following are example declarative and programmatic configuration snippets.

#### XML Configuration

```xml
<hazelcast-client>
  <network>
    <aws enabled="true">
      <access-key>my-access-key</access-key>
      <secret-key>my-secret-key</secret-key>
      <region>eu-central-1</region>
      <cluster>my-cluster</cluster>
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
      access-key: my-access-key
      secret-key: my-secret-key
      region: eu-central-1
      cluster: my-cluster
      use-public-ip: true
```

#### Java-based Configuration

```java
clientConfig.getNetworkConfig().getAwsConfig()
      .setEnabled(true)
      .setProperty("access-key", "my-access-key")
      .setProperty("secret-key", "my-secret-key")
      .setProperty("region", "eu-central-1")
      .setProperty("cluster", "my-cluster")
      .setProperty("use-public-ip", "true");
```

## ECS Environment with EC2 Discovery

If you use ECS on EC2 instances (not Fargate), you may also set up your ECS Tasks to use `host` network mode and then use EC2 discovery mode instead of ECS. In that case, your Hazelcast configuration would look as follows.

```yaml
hazelcast:
  network:
    join:
      multicast:
        enabled: false
      aws:
        enabled: true
        host-header: ec2
    interfaces:
      enabled: true
      interfaces:
        - 10.0.*.*
```

All other parameters can be used exactly the same as described in the EC2-related section.

## AWS Elastic Beanstalk

The plugin works correctly on the AWS Elastic Beanstalk environment. While deploying your application into the Java Platform, please make sure your Elastic Beanstalk Environment Configuration satisfies the following requirements:
* EC2 security groups contain a group which allows the port `5701`
* IAM instance profile contains IAM role which has `ec2:DescribeInstances` permission (or your Hazelcast configuration contains `access-key` and `secret-key`)
* Deployment policy is `Rolling` (instead of the default `All at once` which may cause the whole Hazelcast members to restart at the same time and therefore lose data)

## High Availability

By default, Hazelcast distributes partition replicas (backups) randomly and equally among cluster members. However, this is not safe in terms of high availability when a partition and its replicas are stored on the same rack, using the same network, or power source. To deal with that, Hazelcast offers logical partition grouping, so that a partition
itself and its backup(s) would not be stored within the same group. This way Hazelcast guarantees that a possible failure
affecting more than one member at a time will not cause data loss. The details of partition groups can be found in the
documentation: 
[Partition Group Configuration](https://docs.hazelcast.org/docs/latest/manual/html-single/#partition-group-configuration)

In addition to two built-in grouping options `ZONE_AWARE` and `PLACEMENT_AWARE`, you can customize the formation of
these groups based on the network interfaces of members. See more details on custom groups in the documentation:
[Custom Partition Groups](https://docs.hazelcast.org/docs/latest/manual/html-single/#custom).


### Multi-Zone Deployments

If `ZONE_AWARE` partition group is enabled, the backup(s) of a partition is always stored in a different availability
zone. Hazelcast AWS Discovery plugin supports ZONE_AWARE feature for both EC2 and ECS.

***NOTE:*** *When using the `ZONE_AWARE` partition grouping, a cluster spanning multiple Availability Zones (AZ)
should have an equal number of members in each AZ. Otherwise, it will result in uneven partition distribution among
the members.*

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

### Partition Placement Group Deployments

[AWS Partition Placement Group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html#placement-groups-partition)
(PPG) ensures low latency between the instances in the same partition of a placement group
and also provides availability since no two partitions share the same underlying hardware. As long as the partitions of a 
PPG contain an equal number of instances, it will be good practice for Hazelcast clusters formed within a single zone.

If EC2 instances belong to a PPG and `PLACEMENT_AWARE` partition group is enabled, then Hazelcast members will be grouped
by the partitions of the PPG. For instance, the Hazelcast members in the first partition of a PPG named `ppg` will belong
to the partition group of `ppg-1`, and those in the second partition will belong to `ppg-2` and so on. Furthermore, these
groups will be specific to each availability zone. That is, they are formed with zone names as well: `us-east-1-ppg-1`,
`us-east-2-ppg-1`, and the like. However, if a Hazelcast cluster spans multiple availability zones then you should
consider using `ZONE_AWARE`.

### Cluster Placement Group Deployments

[AWS Cluster Placement Group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html#placement-groups-cluster)
(CPG) ensures low latency by packing instances close together inside an availability zone.
If you favor latency over availability, then CPG will serve your purpose.

***NOTE:*** *In the case of CPG, using `PLACEMENT_AWARE` has no effect, so can use the default Hazelcast partition group
strategy.*

### Spread Placement Group Deployments

[AWS Spread Placement Groups](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html#placement-groups-spread)
(SPG) ensures high availability in a single zone by placing each instance in a group on a
distinct rack. It provides better latency than multi-zone deployment, but worse than Cluster Placement Group. SPG is
limited to 7 instances, so if you need a larger Hazelcast cluster within a single zone, you should use PPG instead.

***NOTE:*** *In the case of SPG, using `PLACEMENT_AWARE` has no effect, so can use the default Hazelcast partition group
strategy.*

#### XML Configuration

```xml
<partition-group enabled="true" group-type="PLACEMENT_AWARE" />
```

#### YAML Configuration

```yaml
hazelcast:
  partition-group:
    enabled: true
    group-type: PLACEMENT_AWARE
```

#### Java-based Configuration

```java
config.getPartitionGroupConfig()
    .setEnabled(true)
    .setGroupType(MemberGroupType.PLACEMENT_AWARE);
```

## Autoscaling

Hazelcast is prepared to work correctly within the autoscaling environments. Note that there are two specific requirements to prevent Hazelcast from losing data:
* the number of members must change by 1 at the time
* when a member is launched or terminated, the cluster must be in the safe state

Read about details in the blog post: [AWS Auto Scaling with Hazelcast](https://hazelcast.com/blog/aws-auto-scaling-with-hazelcast/).

## AWS EC2 Deployment Guide

You can download the white paper "Amazon EC2 Deployment Guide for Hazelcast IMDG" [here](https://hazelcast.com/resources/amazon-ec2-deployment-guide/).

## How to find us?

In case of any question or issue, please raise a GH issue, send an email to [Hazelcast Google Groups](https://groups.google.com/forum/#!forum/hazelcast) or contact as directly via [Hazelcast Gitter](https://gitter.im/hazelcast/hazelcast).
