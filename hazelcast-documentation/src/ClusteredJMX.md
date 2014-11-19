

## Clustered JMX

![](images/enterprise-onlycopy.jpg)

Clustered JMX via Management Center allows you to monitor clustered statistics of distributed objects from a JMX interface.


### Clustered JMX Configuration

In order to configure Clustered JMX, use two command line parameters for your Management Center deployment.

- `-Dhazelcast.mc.jmx.enabled=true` (default is false)
- `-Dhazelcast.mc.jmx.port=9000` (optional, default is 9999)

With embedded Jetty, you do not need to deploy your Management Center application to any container or application server.

You can start Management Center application with Clustered JMX enabled as shown below.

```
java -Dhazelcast.mc.jmx.enabled=true -Dhazelcast.mc.jmx.port=9999 -jar mancenter-3.3.jar
```

Once Management Center starts, you should see a log similar to below.

```
INFO: Management Center 3.3
Jun 05, 2014 11:55:32 AM com.hazelcast.webmonitor.service.jmx.impl.JMXService
INFO: Starting Management Center JMX Service on port :9999
```

You should be able to connect to Clustered JMX interface from the address `localhost:9999`.

You can use `jconsole` or any other JMX client to monitor your Hazelcast Cluster. As a sample, below is the `jconsole` screenshot of the Clustered JMX hierarchy.

![](images/ClusteredJMX.png)

### API Documentation

The management beans are exposed with the following object name format.

`ManagementCenter[`*cluster name*`]:type=<`*object type*`>,name=<`*object name*`>,member="<`*cluster member IP address*`>"`

Object name starts with `ManagementCenter` prefix. Then it has the cluster name in brackets followed by a colon. After that, `type`,`name` and `member` attributes follows, each separated with a comma.

-	`type` is the type of object. Values are `Clients`, `Executors`, `Maps`, `Members`, `MultiMaps`, `Queues`, `Services`, and `Topics`.

-	`name` is the name of object.

-	`member` is the node address of object (only required if the statistics are local to the node).


A sample bean is shown below.

```
ManagementCenter[dev]:type=Services,name=OperationService,member="192.168.2.79:5701"
```


Here is the list of attributes that are exposed from the Clustered JMX interface.

* **ManagementCenter[ClusterName]**
* Clients
 * Address
 * ClientType
 * Uuid
*  Executors
 * Cluster
  * Name
  * StartedTaskCount
  * CompletedTaskCount
  * CancelledTaskCount
  * PendingTaskCount
*  Maps
  * Cluster
  * Name
  * BackupEntryCount
  * BackupEntryMemoryCost
  * CreationTime
  * DirtyEntryCount
  * Events
  * GetOperationCount
  * HeapCost
  * Hits
  * LastAccessTime
  * LastUpdateTime
  * LockedEntryCount
  * MaxGetLatency
  * MaxPutLatency
  * MaxRemoveLatency
  * OtherOperationCount
  * OwnedEntryCount
  * PutOperationCount
  * RemoveOperationCount
*  Members
  * ConnectedClientCount
  * HeapFreeMemory
  * HeapMaxMemory
  * HeapTotalMemory
  * HeapUsedMemory
  * IsMaster
  * OwnedPartitionCount
*  MultiMaps
  * Cluster
  * Name
  * BackupEntryCount
  * BackupEntryMemoryCost
  * CreationTime
  * DirtyEntryCount
  * Events
  * GetOperationCount
  * HeapCost
  * Hits
  * LastAccessTime
  * LastUpdateTime
  * LockedEntryCount
  * MaxGetLatency
  * MaxPutLatency
  * MaxRemoveLatency
  * OtherOperationCount
  * OwnedEntryCount
  * PutOperationCount
  * RemoveOperationCount
*  Queues
  * Cluster
  * Name
  * MinAge
  * MaxAge
  * AvgAge
  * OwnedItemCount
  * BackupItemCount
  * OfferOperationCount
  * OtherOperationsCount
  * PollOperationCount
  * RejectedOfferOperationCount
  * EmptyPollOperationCount
  * EventOperationCount
  * CreationTime
*  Services
  * ConnectionManager
    * ActiveConnectionCount
    * ClientConnectionCount
    * ConnectionCount
  * EventService
    * EventQueueCapacity
    * EventQueueSize
    * EventThreadCount
  * OperationService
    * ExecutedOperationCount
    * OperationExecutorQueueSize
    * OperationThreadCount
    * RemoteOperationCount
    * ResponseQueueSize
    * RunningOperationsCount
  * PartitionService
    * ActivePartitionCount
    * PartitionCount
  * ProxyService
    * ProxyCount
  * ManagedExecutor[hz::async]
    * Name
    * CompletedTaskCount
    * MaximumPoolSize
    * PoolSize
    * QueueSize
    * RemainingQueueCapacity
    * Terminated
  * ManagedExecutor[hz::client]
    * Name
    * CompletedTaskCount
    * MaximumPoolSize
    * PoolSize
    * QueueSize
    * RemainingQueueCapacity
    * Terminated
  * ManagedExecutor[hz::global-operation]
    * Name
    * CompletedTaskCount
    * MaximumPoolSize
    * PoolSize
    * QueueSize
    * RemainingQueueCapacity
    * Terminated
  * ManagedExecutor[hz::io]
    * Name
    * CompletedTaskCount
    * MaximumPoolSize
    * PoolSize
    * QueueSize
    * RemainingQueueCapacity
    * Terminated
  * ManagedExecutor[hz::query]
    * Name
    * CompletedTaskCount
    * MaximumPoolSize
    * PoolSize
    * QueueSize
    * RemainingQueueCapacity
    * Terminated
  * ManagedExecutor[hz::scheduled]
    * Name
    * CompletedTaskCount
    * MaximumPoolSize
    * PoolSize
    * QueueSize
    * RemainingQueueCapacity
    * Terminated
  * ManagedExecutor[hz::system]
    * Name
    * CompletedTaskCount
    * MaximumPoolSize
    * PoolSize
    * QueueSize
    * RemainingQueueCapacity
    * Terminated  
*  Topics
  * Cluster
  * Name
  * CreationTime
  * PublishOperationCount
  * ReceiveOperationCount


### New Relic Integration

Use the Clustered JMX interface to integrate Hazelcast Management Center with *New Relic*. To perform this integration, attach New Relic Java agent and provide an extension file that describes which metrics will be sent to New Relic.

Please see [Custom JMX instrumentation by YAML](http://docs.newrelic.com/docs/java/custom-jmx-instrumentation-by-yml) on the New Relic webpage.

Below is an example Map monitoring `.yml` file for New Relic.

```plain
name: Clustered JMX
version: 1.0
enabled: true

jmx:
  - object_name: ManagementCenter[clustername]:type=Maps,name=mapname
    metrics:
      - attributes: PutOperationCount, GetOperationCount, RemoveOperationCount, Hits,\ 
            BackupEntryCount, OwnedEntryCount, LastAccessTime, LastUpdateTime
        type: simple
  - object_name: ManagementCenter[clustername]:type=Members,name="node address in\
        double quotes"
    metrics:
      - attributes: OwnedPartitionCount
        type: simple
```

Put the `.yml` file in the `extensions` folder in your New Relic installation. If an `extensions` folder does not exist there, create one.

After you set your extension, attach the New Relic Java agent and start Management Center as shown below.

```plain
java -javaagent:/path/to/newrelic.jar -Dhazelcast.mc.jmx.enabled=true\
    -Dhazelcast.mc.jmx.port=9999 -jar mancenter-3.3.jar
```

If your logging level is set as FINER, you should see the log listing in the file `newrelic_agent.log`, which is located in the `logs` folder in your New Relic installation. Below is an example log listing.

```plain
Jun 5, 2014 14:18:43 +0300 [72696 62] com.newrelic.agent.jmx.JmxService FINE:
    JMX Service : querying MBeans (1)
Jun 5, 2014 14:18:43 +0300 [72696 62] com.newrelic.agent.jmx.JmxService FINER:
    JMX Service : MBeans query ManagementCenter[dev]:type=Members,
    name="192.168.2.79:5701", matches 1
Jun 5, 2014 14:18:43 +0300 [72696 62] com.newrelic.agent.jmx.JmxService FINER:
    Recording JMX metric OwnedPartitionCount : 68
Jun 5, 2014 14:18:43 +0300 [72696 62] com.newrelic.agent.jmx.JmxService FINER:
    JMX Service : MBeans query ManagementCenter[dev]:type=Maps,name=orders, 
    matches 1
Jun 5, 2014 14:18:43 +0300 [72696 62] com.newrelic.agent.jmx.JmxService FINER: 
    Recording JMX metric Hits : 46,593
Jun 5, 2014 14:18:43 +0300 [72696 62] com.newrelic.agent.jmx.JmxService FINER: 
    Recording JMX metric BackupEntryCount : 1,100
Jun 5, 2014 14:18:43 +0300 [72696 62] com.newrelic.agent.jmx.JmxService FINER: 
    Recording JMX metric OwnedEntryCount : 1,100
Jun 5, 2014 14:18:43 +0300 [72696 62] com.newrelic.agent.jmx.JmxService FINER: 
    Recording JMX metric RemoveOperationCount : 0
Jun 5, 2014 14:18:43 +0300 [72696 62] com.newrelic.agent.jmx.JmxService FINER: 
    Recording JMX metric PutOperationCount : 118,962
Jun 5, 2014 14:18:43 +0300 [72696 62] com.newrelic.agent.jmx.JmxService FINER: 
    Recording JMX metric GetOperationCount : 0
Jun 5, 2014 14:18:43 +0300 [72696 62] com.newrelic.agent.jmx.JmxService FINER: 
    Recording JMX metric LastUpdateTime : 1,401,962,426,811
Jun 5, 2014 14:18:43 +0300 [72696 62] com.newrelic.agent.jmx.JmxService FINER: 
    Recording JMX metric LastAccessTime : 1,401,962,426,811
```

Then you can navigate to your New Relic account and create Custom Dashboards. Please see [Creating custom dashboards](http://docs.newrelic.com/docs/dashboards-menu/creating-custom-dashboards).

While you are creating the dashboard, you should see the metrics that you are sending to New Relic from Management Center in the **Metrics** section under the JMX folder.

### AppDynamics Integration

Use the Clustered JMX interface to integrate Hazelcast Management Center with *AppDynamics*. To perform this integration, attach AppDynamics Java agent to the Management Center.

For agent installation, refer to [Install the App Agent for Java](http://docs.appdynamics.com/display/PRO14S/Install+the+App+Agent+for+Java) page.

For monitoring on AppDynamics, refer to [Using AppDynamics for JMX Monitoring](http://docs.appdynamics.com/display/PRO14S/Monitor+JMX+MBeans#MonitorJMXMBeans-UsingAppDynamicsforJMXMonitoring) page.

After installing AppDynamics agent, you can start Management Center as shown below.

```plain
java -javaagent:/path/to/javaagent.jar -Dhazelcast.mc.jmx.enabled=true\
    -Dhazelcast.mc.jmx.port=9999 -jar mancenter-3.3.jar
```

When Management Center starts, you should see the logs below.

```plain
Started AppDynamics Java Agent Successfully.
Hazelcast Management Center starting on port 8080 at path : /mancenter
```
<br></br>