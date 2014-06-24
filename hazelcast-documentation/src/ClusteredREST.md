

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

### Clusters [/rest/clusters/]

This resource returns list of clusters that connected to the Management Center.

#### Retrieve Clusters [GET]
+ Request
      curl http://localhost:8083/mancenter/rest/clusters
+ Response 200 (application/json)
+ Body
      ["dev","qa"]

### Cluster Information [/rest/clusters/{clustername}]

This resource returns information related to provided cluster name.

#### Retrieve Cluster Information [GET]
+ Request
      curl http://localhost:8083/mancenter/rest/clusters/dev/
+ Response 200 (application/json)
+ Body
      {"masterAddress":"192.168.2.78:5701"}

### Members [/rest/clusters/{clustername}/members]

This resource returns list of members belong to provided clusters.

#### Retrieve Members [GET]
+ Request
```
curl http://localhost:8083/mancenter/rest/clusters/dev/members
```
+ Response 200 (application/json)
+ Body
```
["192.168.2.78:5701","192.168.2.78:5702","192.168.2.78:5703","192.168.2.78:5704"]
```
### Member Information [/rest/clusters/{clustername}/members/{member}]

This resource returns information related to provided member.

#### Retrieve Members [GET]
+ Request
      curl http://localhost:8083/mancenter/rest/clusters/dev/members/192.168.2.78:5701
+ Response 200 (application/json)
+ Body
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
