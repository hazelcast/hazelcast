

## Clustered REST

![](images/enterprise-onlycopy.jpg)

Clustered REST API is exposed from Management Center to allow you to monitor clustered statistics of distributed objects.

### Configuration

In order to enable Clustered REST on your Management Center you need to pass following system property at startup. This feature is disabled by default.

```
-Dhazelcast.mc.rest.enabled=true
```

### Clustered REST API Root [/rest/]

Entry point for Clustered REST API.

This resource does not have any attributes

### Clusters

This resource returns list of clusters that connected to the Management Center.

#### Retrieve Clusters [GET] [/rest/clusters/]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters
```
+ Response 200 (application/json)
+ Body
```json
["dev","qa"]
```

### Cluster Information

This resource returns information related to provided cluster name.

#### Retrieve Cluster Information [GET] [/rest/clusters/{clustername}]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/
```
+ Response 200 (application/json)
+ Body
```json
{"masterAddress":"192.168.2.78:5701"}
```

### Members

This resource returns list of members belong to provided clusters.

#### Retrieve Members [GET] [/rest/clusters/{clustername}/members]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/members
```
+ Response 200 (application/json)
+ Body
```json
["192.168.2.78:5701","192.168.2.78:5702","192.168.2.78:5703","192.168.2.78:5704"]
```

### Member Information

This resource returns information related to provided member.

#### Retrieve Member Information [GET] [/rest/clusters/{clustername}/members/{member}]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/members/192.168.2.78:5701
```
+ Response 200 (application/json)
+ Body
```json
{
  "cluster":"dev",
  "name":"192.168.2.78:5701",
  "maxMemory":129957888,
  "ownedPartitionCount":68,
  "usedMemory":60688784,
  "freeMemory":24311408,
  "totalMemory":85000192,
  "connectedClientCount":1,
  "master":true
}
```

#### Retrieve Connection Manager Information [GET] [/rest/clusters/{clustername}/members/{member}/connectionManager]
+ Request
      curl http://localhost:8083/mancenter/rest/clusters/dev/members/192.168.2.78:5701/connectionManager
+ Response 200 (application/json)
+ Body
```json
{
  "clientConnectionCount":2,
  "activeConnectionCount":5,
  "connectionCount":5
}
```

#### Retrieve Operation Service Information [GET] [/rest/clusters/{clustername}/members/{member}/operationService]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/members/192.168.2.78:5701/operationService
```
+ Response 200 (application/json)
+ Body
```json
{
  "responseQueueSize":0,
  "operationExecutorQueueSize":0,
  "runningOperationsCount":0,
  "remoteOperationCount":1,
  "executedOperationCount":461139,
  "operationThreadCount":8
}
```

#### Retrieve Event Service Information [GET] [/rest/clusters/{clustername}/members/{member}/eventService]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/members/192.168.2.78:5701/eventService
```
+ Response 200 (application/json)
+ Body
```json
{
  "eventThreadCount":5,
  "eventQueueCapacity":1000000,
  "eventQueueSize":0
}
```

#### Retrieve Partition Service Information [GET] [/rest/clusters/{clustername}/members/{member}/partitionService]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/members/192.168.2.78:5701/partitionService
```
+ Response 200 (application/json)
+ Body
```json
{
  "partitionCount":271,
  "activePartitionCount":68
}
```

#### Retrieve Proxy Service Information [GET] [/rest/clusters/{clustername}/members/{member}/proxyService]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/members/192.168.2.78:5701/proxyService
```
+ Response 200 (application/json)
+ Body
```json
{
  "proxyCount":8
}
```

#### Retrieve Managed Executors [GET] [/rest/clusters/{clustername}/members/{member}/managedExecutors]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/members/192.168.2.78:5701/managedExecutors
```
+ Response 200 (application/json)
+ Body
```json
["hz:system","hz:scheduled","hz:client","hz:query","hz:io","hz:async"]
```

#### Retrieve Managed Executor [GET] [/rest/clusters/{clustername}/members/{member}/managedExecutors/{managedExecutor}]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/members/192.168.2.78:5701/managedExecutors/hz:system
```
+ Response 200 (application/json)
+ Body
```json
{
  "name":"hz:system",
  "queueSize":0,
  "poolSize":0,
  "remainingQueueCapacity":2147483647,
  "maximumPoolSize":4,
  "completedTaskCount":12,
  "terminated":false
}
```

### Clients

This resource returns list of clients belong to provided cluster.

#### Retrieve List of Clients [GET] [/rest/clusters/{clustername}/clients]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/clients
```
+ Response 200 (application/json)
+ Body
```json
["192.168.2.78:61708"]
```

#### Retrieve Client Information [GET] [/rest/clusters/{clustername}/clients/{client}]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/clients/192.168.2.78:61708
```
+ Response 200 (application/json)
+ Body
```json
{
  "uuid":"6fae7af6-7a7c-4fa5-b165-cde24cf070f5",
  "address":"192.168.2.78:61708",
  "clientType":"JAVA"
}
```

### Maps

This resource returns list of maps belong to provided cluster.


#### Retrieve List of Maps [GET] [/rest/clusters/{clustername}/maps]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/maps
```
+ Response 200 (application/json)
+ Body
```json
["customers","orders"]
```

#### Retrieve Map Information [GET] [/rest/clusters/{clustername}/maps/{mapName}]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/maps/customers
```
+ Response 200 (application/json)
+ Body
```json
{
  "cluster":"dev",
  "name":"customers",
  "ownedEntryCount":1000,
  "backupEntryCount":1000,
  "ownedEntryMemoryCost":157890,
  "backupEntryMemoryCost":113683,
  "heapCost":297005,
  "lockedEntryCount":0,
  "dirtyEntryCount":0,
  "hits":3001,
  "lastAccessTime":1403608925777,
  "lastUpdateTime":1403608925777,
  "creationTime":1403602693388,
  "putOperationCount":110630,
  "getOperationCount":165945,
  "removeOperationCount":55315,
  "otherOperationCount":0,
  "events":0,
  "maxPutLatency":52,
  "maxGetLatency":30,
  "maxRemoveLatency":21
}
```



### MultiMaps

This resource returns list of multimaps belong to provided cluster.


#### Retrieve List of MultiMaps [GET] [/rest/clusters/{clustername}/multimaps]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/multimaps
```
+ Response 200 (application/json)
+ Body
```json
["customerAddresses"]
```

#### Retrieve MultiMap Information [GET] [/rest/clusters/{clustername}/multimaps/{multimapname}]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/multimaps/customerAddresses
```
+ Response 200 (application/json)
+ Body
```json
{
  "cluster":"dev",
  "name":"customerAddresses",
  "ownedEntryCount":996,
  "backupEntryCount":996,
  "ownedEntryMemoryCost":0,
  "backupEntryMemoryCost":0,
  "heapCost":0,
  "lockedEntryCount":0,
  "dirtyEntryCount":0,
  "hits":0,
  "lastAccessTime":1403603095521,
  "lastUpdateTime":1403603095521,
  "creationTime":1403602694158,
  "putOperationCount":166041,
  "getOperationCount":110694,
  "removeOperationCount":55347,
  "otherOperationCount":0,
  "events":0,
  "maxPutLatency":77,
  "maxGetLatency":69,
  "maxRemoveLatency":42
}
```


### Queues

This resource returns list of queues belong to provided cluster.


#### Retrieve List of Queues [GET] [/rest/clusters/{clustername}/queues]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/queues
```
+ Response 200 (application/json)
+ Body
```json
["messages"]
```

#### Retrieve Queue Information [GET] [/rest/clusters/{clustername}/queues/{queueName}]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/queues/messages
```
+ Response 200 (application/json)
+ Body
```json
{
  "cluster":"dev",
  "name":"messages",
  "ownedItemCount":55408,
  "backupItemCount":55408,
  "minAge":0,
  "maxAge":0,
  "aveAge":0,
  "numberOfOffers":55408,
  "numberOfRejectedOffers":0,
  "numberOfPolls":0,
  "numberOfEmptyPolls":0,
  "numberOfOtherOperations":0,
  "numberOfEvents":0,
  "creationTime":1403602694196
}
```


### Topics

This resource returns list of topics belong to provided cluster.


#### Retrieve List of Topics [GET] [/rest/clusters/{clustername}/topics]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/topics
```
+ Response 200 (application/json)
+ Body
```json
["news"]
```

#### Retrieve Topic Information [GET] [/rest/clusters/{clustername}/topics/{topicName}]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/topics/news
```
+ Response 200 (application/json)
+ Body
```json
{
  "cluster":"dev",
  "name":"news",
  "numberOfPublishes":56370,
  "totalReceivedMessages":56370,
  "creationTime":1403602693411
}
```


### Executors

This resource returns list of executors belong to provided cluster.


#### Retrieve List of Executors [GET] [/rest/clusters/{clustername}/executors]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/executors
```
+ Response 200 (application/json)
+ Body
```json
["order-executor"]
```

#### Retrieve Executor Information [GET] [/rest/clusters/{clustername}/executors/{executorName}]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/executors/order-executor
```
+ Response 200 (application/json)
+ Body
```json
{
  "cluster":"dev",
  "name":"order-executor",
  "creationTime":1403602694196,
  "pendingTaskCount":0,
  "startedTaskCount":1241,
  "completedTaskCount":1241,
  "cancelledTaskCount":0
}
```
