
### Tomcat Based Web Session Replication

![](images/enterprise-onlycopy.jpg)

#### Overview

Session Replication with Hazelcast Enterprise is a container specific module where no application change is required to enable session replication for JEE Web Applications. 

***Features***

1. Seamless Tomcat 6 & 7 integration
2. Support for sticky and non-sticky sessions
3. Tomcat failover
4. Deferred write for performance boost
<br></br>

***Supported Containers***

Tomcat Web Session Replication Module has been tested against following containers.

- Tomcat 6.0.x - It can be downloaded [here](http://tomcat.apache.org/download-60.cgi).
- Tomcat 7.0.x - It can be downloaded [here](http://tomcat.apache.org/download-70.cgi).

Latest tested versions are **6.0.39** and **7.0.40**.
<br></br>

***Requirements***

 - Tomcat instance must be running with Java 1.6 or higher.
 - Session objects that need to be clustered have to be Serializable.

#### Client-Server Deployment

In this deployment type, Tomcat instances work as clients to an existing Hazelcast Cluster.

***Features***

-	Existing Hazelcast cluster is used as the Session Replication Cluster.
-	Offload Session Cache from Tomcat to Hazelcast Cluster.
-	The architecture is completely independent. Complete reboot of Tomcat instances.
<br></br>

***Configuration***

1. Set `clientOnly` property as **true** (optional, default is false).
2. Set `mapName` property (optional, configured map for special cases like WAN Replication, Eviction, MapStore, etc.).
3. Set `sticky` property (optional, default value is `true`)
4. Configure Hazelcast client via `hazelcast.client.config` system property or by putting `hazelcast-client.xml` into classpath.
5. Add `hazelcast-client-<version>.jar` and `hazelcast-sessions-tomcat6-<version>.jar` to $CATALINA_HOME/lib/ folder.
<br></br>

***Sample Configuration to use Hazelcast Session Replication***

Update `<Manager>` tag in the `$CATALINA_HOME$/conf/context.xml` as shown below.

```xml
<Context>
  <Manager className="com.hazelcast.session.HazelcastSessionManager" clientOnly="true" mapName="sessionMap" sticky="false"/>
</Context>
```
<br></br>

#### P2P Deployment

This launches embedded Hazelcast Node in each server instance.

***Features***

This type of deployment is the simplest approach. You can just configure your Tomcat and launch. There is no need for an  external Hazelcast cluster.

***Configuration***

1. Set `mapName` property (optional, configured map for special cases like WAN Replication, Eviction, MapStore, etc.).
2. Set `sticky` property (optional, default value is `true`)
3. Configure Hazelcast client via `hazelcast.config` system property or by putting `hazelcast.xml` into classpath.
4. Add `hazelcast-<version>.jar` and `hazelcast-sessions-tomcat6-<version>.jar` to $CATALINA_HOME/lib/ folder.

***Sample Configuration to use Hazelcast Session Replication***

Update `<Manager>` tag in the `$CATALINA_HOME$/conf/context.xml` as shown below.

```xml
<Context>
  <Manager className="com.hazelcast.session.HazelcastSessionManager" mapName="sessionMap" sticky="false"/>
</Context>
```
<br></br>


#### Session Affinity 

***Sticky Sessions (default)***

Sticky Sessions are used to improve the performance since the sessions do not move around the cluster.
 
Request goes always to the same instance where the session was firstly created. By using a sticky session, you eliminate session replication problems mostly, except for the failover cases. In case of failovers, Hazelcast helps you not lose existing sessions.


***Non-Sticky Sessions***

Non-Sticky Sessions are not good for performance because you need to move session data all over the cluster every time a new request comes in.

However, load balancing might be super easy with Non-Sticky caches. In case of heavy load, you can distribute the request to the least used Tomcat instance. Hazelcast supports Non-Sticky Sessions as well. 

#### Session Caching

Tomcat Web Session Replication Module has its own nature of caching attribute changes during the HTTP Request/HTTP Response cycle. Each HTTP Request can change one or more HTTP Session attributes, and distributing those changes to the Hazelcast Cluster is costly. Because of that, Session Replication is only done at the end of each request for updated and deleted attributes. The risk in this approach is to lose data in case a Tomcat crash happens in the middle of HTTP Request operation.


<br></br>