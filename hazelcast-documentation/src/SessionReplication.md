
## Session Replication

![](images/enterprise-onlycopy.jpg)

Session Replication with Hazelcast Enterprise is a container specific module where no application change is required to enable session replication for JEE Web Applications. 

Features

1. Seamless Tomcat 6,7 integration
2. Sticky + Non-Sticky sessions supported
3. Tomcat failover
4. Deferred write for performance boost

### Deployments

#### Client-Server Deployment

In this deployment type, Tomcat instances work as clients to an existing Hazelcast Cluster.

**Features:**

-	Existing Hazelcast cluster is used as the Session Replication Cluster.
-	Offload Session Cache from Tomcat to Hazelcast Cluster.
-	The architecture is completely independent. Complete reboot of Tomcat instances.

**Configuration:**

1. Set `clientOnly` property as **true** (optional, default is false).
2. Set `mapName` property (optional, configured map for special cases like WAN Replication, Eviction, MapStore, etc.).
3. Configure Hazelcast client via `hazelcast.client.config` system property or by putting `hazelcast-client.xml` into classpath.
4. Add `hazelcast-client-<version>.jar` and `hazelcast-sessions-tomcat6-<version>.jar` to $CATALINA_HOME/lib/ folder.


**Sample Configuration**

```xml
<Context>
  <Manager className="com.hazelcast.session.HazelcastSessionManager" clientOnly="true" mapName="sessionMap"/>
</Context>
```

#### P2P Deployment

This launches embedded Hazelcast Node in each server instance.

**Features**

This type of deployment is the simplest approach. You can just configure your Tomcat and launch. There is no need for an  external Hazelcast cluster.

**Configuration**

1. Set `mapName` property (optional, configured map for special cases like WAN Replication, Eviction, MapStore, etc.).
2. Configure Hazelcast client via `hazelcast.config` system property or by putting `hazelcast.xml` into classpath.
3. Add `hazelcast-<version>.jar` and `hazelcast-sessions-tomcat6-<version>.jar` to $CATALINA_HOME/lib/ folder.

**Sample Configuration**

```xml
<Context>
  <Manager className="com.hazelcast.session.HazelcastSessionManager" mapName="sessionMap"/>
</Context>
```


### Session Affinity 

**Sticky Sessions (default)**

Sticky Sessions are used to improve the performance since the sessions do not move around the cluster.
 
Request goes always to the same instance where the session was firstly created. By using a sticky session, you eliminate session replication problems mostly, except for the failover cases. In case of failovers, Hazelcast helps you not lose existing sessions.


**Non-Sticky Sessions**

Non-Sticky Sessions are not good for performance because you need to move session data all over the cluster every time a new request comes in.

However, with Non-Sticky caches, load balancing might be super easy as some load increasing case you can distribute the request to the least used Tomcat instance. Hazelcast supports Non-Sticky Sessions as well. 

### Supported Containers

- Tomcat 6.0.39
- Tomcat 7.0.54





