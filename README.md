# Table of Contents

  * [Supported Hazelcast Versions](#supported-hazelcast-versions)
  * [Discovering Members within EC2 Cloud](#discovering-members-within-ec2-cloud)
  * [AWSClient Configuration](#awsclient-configuration)
  * [Debugging](#debugging)

## Supported Hazelcast Versions

- Hazelcast 3.6+


## Discovering Members within EC2 Cloud

Hazelcast supports EC2 auto-discovery. It is useful when you do not want to provide or you cannot provide the list of possible IP addresses. 

To configure your cluster to use EC2 auto-discovery, follow the below steps:

- Add the *hazelcast-aws.jar* dependency to your project. Note that it is also bundled inside *hazelcast-all.jar*. The Hazelcast aws module does not depend on any other third party modules.
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
```  

Here are the definitions of `aws` element's attributes and sub-elements:

* `enabled`: Specifies whether the EC2 discovery is enabled or not, true or false.
* `access-key`, `secret-key`: Access and secret keys of your account on EC2.
* `iam-role`: If you want to use access key and secret key. You can use iam-role configuration. Hazelcast-aws fetches your credentials by using your iam role. It is optional.
* `region`: The region where your members are running. Default value is us-east-1. You need to specify this if the region is other than the default one.
* `host-header`: The URL that is the entry point for a web service. It is optional.
* `security-group-name`: Name of the security group you specified at the EC2 management console. It is used to narrow the Hazelcast members to be within this group. It is optional.
* `tag-key`, `tag-value`: To narrow the members in the cloud down to only Hazelcast members, you can set these parameters as the ones you specified in the EC2 console. They are optional.
* `connection-timeout-seconds`: The maximum amount of time Hazelcast will try to connect to a well known member before giving up. Setting this value too low could mean that a member is not able to connect to a cluster. Setting the value too high means that member startup could slow down because of longer timeouts (for example, when a well known member is not up). Increasing this value is recommended if you have many IPs listed and the members cannot properly build up the cluster. Its default value is 5.

#### NOTE:
If you are using iam-role. You need to give at least following policy to your iam user.
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
  config.setSecretKey( ... ) ;
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


***RELATED INFORMATION***

*You can download the white paper "Hazelcast on AWS: Best Practices for Deployment" from <a href="http://hazelcast.com/resources/hazelcast-on-aws-best-practices-for-deployment/" target="_blank">Hazelcast.com</a>.*







