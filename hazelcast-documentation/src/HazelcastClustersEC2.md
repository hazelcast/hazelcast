
### EC2 Cloud Auto-discovery

Hazelcast supports EC2 Auto Discovery. It is useful when you do not want or cannot provide the list of possible IP addresses. To configure your cluster to use EC2 Auto Discovery, disable join over multicast and TCP/IP, enable AWS, and provide your credentials (access and secret keys). 

You need to add the *hazelcast-cloud.jar* dependency to your project. Note that it is also bundled inside *hazelcast-all.jar*. The Hazelcast cloud module does not depend on any other third party modules.

The following is an example declarative configuration.


```xml
<join>
  <multicast enabled="false">
  </multicast>
  <tcp-ip enabled="false">
  </tcp-ip>
  <aws enabled="true">
    <access-key>my-access-key</access-key>
    <secret-key>my-secret-key</secret-key>
    <region>us-west-1</region>
    <host-header>ec2.amazonaws.com</host-header>
    <security-group-name>hazelcast-sg</security-group-name>
    <tag-key>type</tag-key>
    <tag-value>hz-nodes</tag-value>
  </aws>
</join>
```  

<br></br>
***RELATED INFORMATION***

*Please refer to the [aws element section](#aws-element) for the full description of EC2 auto-discovery configuration.*
<br></br>

#### Debugging

When needed, Hazelcast can log the events for the instances that exist in a region. To see what has happened or to trace the activities while forming the cluster, change the log level in your logging mechanism to `FINEST` or `DEBUG`. After this change, you can also see in the generated log whether the instances are accepted or rejected, and the reason the instances were rejected. Note that changing the log level in this way may affect the performance of the cluster. Please see the [Logging Configuration section](#logging-configuration) for information on logging mechanisms.

<br> </br>
***RELATED INFORMATION***

*You can download the white paper *"Hazelcast on AWS: Best Practices for Deployment"* from [Hazelcast.com](http://hazelcast.com/resources/hazelcast-on-aws-best-practices-for-deployment/).*
<br> </br>






