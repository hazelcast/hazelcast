

## Hazelcast Performance on AWS

Amazon Web Services (AWS) platform is rather an unpredictable environment than the traditional in-house data centers since the machines, databases or CPUs are shared with other unknown  applications in the cloud causing fluctuations. So, when you gear up your Hazelcast application from a physical environment to Amazon EC2, it should be configured in a way so that any network outage or fluctuation is minimized and its performance is maximized. This section provides a couple of notes on improving the performance of Hazelcast on AWS.

### Selecting EC2 Instance Type

Hazelcast is an in-memory data grid that distributes the data and computation to the nodes that are connected with a network, making it very sensitive to the network. Not all EC2 Instance types are the same in terms of the network performance. It is recommended to choose instances that have **10 Gigabit** or **High** network performance for Hazelcast deployments. Please see the below table for the recommended instances.

Instance Type|Network Performance
-|-
m3.2xlarge|High
m1.xlarge|High
c3.2xlarge|High
c3.4xlarge|High
c3.8xlarge|10 Gigabit
c1.xlarge|High
cc2.8xlarge|10 Gigabit
m2.4xlarge|High
cr1.8xlarge|10 Gigabit

### Dealing with Network Latency

Since data is sent-received very frequently in Hazelcast applications, latency in the network becomes a crucial issue. In terms of the latency, AWS cloud performance is not the same for each region and there are vast differences in the speed and optimization from region to region.

When you do not pay attention to AWS regions, Hazelcast applications may run tens or hundreds times slower than necessary. The following notes may be the workarounds:

- Create a cluster only within a region. It is not recommended to deploy a single cluster that spans across multiple regions.
- If a Hazelcast application is hosted on Amazon EC2 instances in multiple EC2 regions, the latency can be reduced by serving the end users` requests from the EC2 region which has the lowest network latency. Changes in network connectivity and routing result in changes in the latency between hosts on the Internet. Amazon has a web service (Route 53) that lets the cloud architects use DNS to route end-user requests to the EC2 region that gives the fastest response. This latency-based routing is based on latency measurements performed over a period of time. Please have a look at [Route53](http://docs.aws.amazon.com/Route53/latest/DeveloperGuide/HowDoesRoute53Work.html).
- Move the deployment to another region. The [CloudPing](http://www.cloudping.info/) tool gives instant estimates on the latency from your location. By using it frequently, it can be helpful to determine the regions which have the lowest latency.
- The [SpeedTest](http://cloudharmony.com/speedtest) tool allows you not only test the network latency but also the downloading/uploading speeds.

### Selecting Virtualization

AWS uses two virtualization types to launch the EC2 instances: Para-Virtualization (PV) and Hardware-assisted Virtual Machine (HVM). According to the tests we performed, HVM provided up to three times higher throughput than PV. Therefore, we recommend you use HVM when you run Hazelcast on EC2.





