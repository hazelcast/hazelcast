
### EC2 Auto Discovery

Hazelcast supports EC2 Auto Discovery. It is useful when you do not want or cannot provide the list of possible IP addresses. To configure your cluster to be able to use EC2 Auto Discovery, disable join over multicast and TCP/IP and enable AWS. Also provide your credentials (access and secret keys). 

You need to add *hazelcast-cloud.jar* dependency into your project. Note that it is also bundled inside *hazelcast-all.jar*. Hazelcast cloud  module does not depend on any other third party modules.

Below is a sample configuration. 

```xml
<join>
  <multicast enabled="false">
    <multicast-group>224.2.2.3</multicast-group>
    <multicast-port>54327</multicast-port>
  </multicast>
  <tcp-ip enabled="false">
    <interface>192.168.1.2</interface>
  </tcp-ip>
  <aws enabled="true">
    <access-key>my-access-key</access-key>
    <secret-key>my-secret-key</secret-key>
    <!-- optional, default is us-east-1 -->
    <region>us-west-1</region>
    <!-- optional, default is ec2.amazonaws.com. If set, region 
         shouldn't be set as it will override this property -->
    <host-header>ec2.amazonaws.com</host-header>
    <!-- optional -->
    <security-group-name>hazelcast-sg</security-group-name>
    <!-- optional -->
    <tag-key>type</tag-key>
    <!-- optional -->
    <tag-value>hz-nodes</tag-value>
  </aws>
</join>
```

The `aws` tag accepts an attribute called *connection-timeout-seconds* whose default value is 5. Increasing this value is recommended if you have many IPs listed and members cannot properly build up the cluster.

The parameter `region` specifies where the members are running. Its default value is `us-east-1`. If the cluster is running on a different region, it must be specified here. Otherwise, the cluster will not be formed since the members will not discover each other.

The parameters `tag-key` and `tag-value` provides unique keys and values to members so that you can create multiple clusters in one data center.

The parameter `security-group-name` is also used to filter/group members.

***NOTE:*** *If you are using a cloud provider other than AWS, you can use the programmatic configuration specify a TCP/IP cluster. The members will needed to be retrieved from that provider (e.g. JClouds).*

#### AWSClient

To make sure EC2 instances are found correctly, AWSClient class can be used. It determines private IP addresses of EC2 instances to be connected. Just give the values of parameters you specified in `aws` tag to this class, as shown below. You will see whether your EC2 instances are found.

```java
public static void main( String[] args )throws Exception{ 
  AwsConfig config = new AwsConfig(); 
  config.setSecretKey( ... ) ;
  config.setSecretKey( ... );
  config.setRegion( ... );
  config.setSecurityGroupName( ... );
  config.setTagKey( ... );
  config.setTagValue( ... );
  config.setEnabled("true");
  AWSClient client = new AWSClient( config );
  List<String> ipAddresses = client.getPrivateIpAddresses();
  System.out.println( "addresses found:" + ipAddresses ); 
  for ( String ip: ipAddresses ) {
    System.out.println( ip ); 
  }
}``` 

#### Debugging

When and if needed, Hazelcast can log the events for the instances that exist in a region. To see what has happened or trace the activities while forming the cluster, change the log level in your logging mechanism to FINEST or DEBUG. After this change, you can also see whether the instances are accepted or rejected, and the reason of rejection for the rejected instances in the generated log. Note that, changing the log level to one of the mentioned levels may affect the performance of the cluster.

<br> </br>
***RELATED INFORMATION***

*You can download the white paper *"Hazelcast on AWS: Best Practices for Deployment"* from [Hazelcast.com](http://hazelcast.com/resources/hazelcast-on-aws-best-practices-for-deployment/).*
<br> </br>