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
* `tag-key`, `tag-value`: filter to look only for EC2 Instances with the given `tag-key`/`tag-value`
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

## Zone Aware

Hazelcast AWS Discovery plugin supports Hazelcast Zone Aware feature for both EC2 and ECS. When using `ZONE_AWARE` configuration, backups are created in the other Availability Zone.

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


## Autoscaling

Hazelcast is prepared to work correctly within the autoscaling environments. Note that there are two specific requirements to prevent Hazelcast data:
* the number of members must change by 1 at the time
* when a member is launched or terminated, the cluster must be in the safe state

Read about details in the blog post: [AWS Auto Scaling with Hazelcast](https://hazelcast.com/blog/aws-auto-scaling-with-hazelcast/).

## AWS EC2 Deployment Guide

You can download the white paper "Amazon EC2 Deployment Guide for Hazelcast IMDG" [here](https://hazelcast.com/resources/amazon-ec2-deployment-guide/).

## How to find us?

In case of any question or issue, please raise a GH issue, send an email to [Hazelcast Google Groups](https://groups.google.com/forum/#!forum/hazelcast) or contact as directly via [Hazelcast Gitter](https://gitter.im/hazelcast/hazelcast).
