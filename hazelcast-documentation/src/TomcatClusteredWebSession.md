
## Session Replication - Enterprise Only

Session Replication with Hazelcast Enterprise is container specif module where no application change is required to enable session replication for JEE Web Applicaitions. 

Features

1. Seamless Tomcat 6,7 integration
2. Sticky + Non-Sticky sessions supported
3. Tomcat failover
4. Deferred write for performance boost

### Deployments

**Client-Server Deployment**

Tomcat instances works as client to an existing Hazelcast Cluster

Features

1. use existing Cluster as Session Replication Cluster
2. offload Session Cache from Tomcat to Hazelcast Cluster
3. completely independent architecture. complete reboot of tomcat instances

Configuration

1. use clientOnly=true (optional, default is false)
2. mapName (optional, configured map for special cases like WAN Replication, Eviction, MapStore etc.)
3. configure hazelcast client via hazelcast.client.config system property or putting hazelcast-client.xml into classpath
4. add hazelcast-client-{version}.jar and hazelcast-sessions-tomcat6-{version}.jar to $CATALINA_HOME/lib/ folder


Sample Configuration

```xml
<Context>
  <Manager className="com.hazelcast.session.HazelcastSessionManager" clientOnly="true" mapName="sessionMap"/>
</Context>
```
**P2P Deployment**

This launchs embedded Hazelcast Node in each Server Instance

Features

1. simplest approach . just configure tomcat and launch. No external Hazelcast Cluster needed.

Configuration

1. mapName (optional, configured map for special cases like WAN Replication, Eviction, MapStore etc.)
2. configure hazelcast client via hazelcast.config system property or putting hazelcast.xml into classpath
3. add hazelcast-{version}.jar and hazelcast-sessions-tomcat6-{version}.jar to $CATALINA_HOME/lib/ folder

Sample Configuration

```xml
<Context>
  <Manager className="com.hazelcast.session.HazelcastSessionManager" mapName="sessionMap"/>
</Context>
```


### Session Affinity 

**Sticky Sessions (used by default)**

Sticky Sessions is performance improvement technique because sessions never move around the cluster. 
Request goes always to the same instance where the session was firstly created. By using sticky session, you eliminate session replication problems mostly except failover cases. In case of failover, Hazelcast helps you not lose existing sessions.


**Non-Sticky Sessions**

Non-Sticky Sessions are bad for performance because you need to move session data all over the cluster every time a new request comes in. 
However, with non-sticky caches load balancing might be super easy as some load increasing case you can distribute the request to the least used Tomcat instance. Hazelcast supports Non-Sticky Sessions as well. 

### Supported Containers

- Tomcat 6.0.39
- Tomcat 7.0.54





