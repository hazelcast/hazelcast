
### Tomcat Based Web Session Replication

![image](images/NoteSmall.jpg) ***NOTE:*** *This feature is supported for Hazelcast Enterprise 3.3 or higher.*


![](images/enterprise-onlycopy.jpg)


***Sample Code:*** *Please see our [sample application](https://github.com/hazelcast/hazelcast-code-samples/tree/master/hazelcast-integration/enterprise-session-replication) for Tomcat Based Web Session Replication.*
<br></br>

#### Overview

Session Replication with Hazelcast Enterprise is a container specific module that enables session replication for JEE Web Applications without requiring changes to the application.

***Features***

1. Seamless Tomcat 6 & 7 integration
2. Support for sticky and non-sticky sessions
3. Tomcat failover
4. Deferred write for performance boost
<br></br>

***Supported Containers***

Tomcat Web Session Replication Module has been tested against the following containers.

- Tomcat 6.0.x - It can be downloaded [here](http://tomcat.apache.org/download-60.cgi).
- Tomcat 7.0.x - It can be downloaded [here](http://tomcat.apache.org/download-70.cgi).

The latest tested versions are **6.0.39** and **7.0.40**.
<br></br>

***Requirements***

 - Tomcat instance must be running with Java 1.6 or higher.
 - Session objects that need to be clustered have to be Serializable.

#### How Tomcat Session Replication works

Tomcat Session Replication in Hazelcast Enterprise is a Hazelcast Module where each created `HttpSession` Object is kept in the Hazelcast Distributed Map. If configured with Sticky Sessions, each Tomcat Instance has its own local copy of the session for performance boost. 

Since the sessions are in Hazelcast Distributed Map, you can use all the available features offered by Hazelcast Distributed Map implementation, such as MapStore and WAN Replication.

Tomcat Web Sessions run in two different modes:

- **P2P**: all Tomcat instances launch its own Hazelcast Instance and join to the Hazelcast Cluster and,
- **Client/Server**: all Tomcat instances put/retrieve the session data to/from an existing Hazelcast Cluster.

#### P2P (Peer-to-Peer) Deployment

P2P deployment launches an embedded Hazelcast Node in each server instance.

***Features***

This type of deployment is simple: just configure your Tomcat and launch. There is no need for an  external Hazelcast cluster.

***Sample P2P Configuration to use Hazelcast Session Replication***

- Go to [hazelcast.com](http://www.hazelcast.com/products/hazelcast-enterprise/) and download the latest Hazelcast Enterprise.
- Unzip the Hazelcast Enterprise zip file into the folder `$HAZELCAST_ENTERPRISE_ROOT`.
- Update `$HAZELCAST_ENTERPRISE_ROOT/bin/hazelcast.xml` with the provided Hazelcast Enterprise License Key. 
- Put `$HAZELCAST_ENTERPRISE_ROOT/lib/hazelcast-enterprise-all-`<*version*>`.jar`,    `$HAZELCAST_ENTERPRISE_ROOT/lib/hazelcast-enterprise-`<*tomcatversion*>`-`<*version*>`.jar` and `hazelcast.xml` in the folder `$CATALINA_HOME/lib/`.

- Put a `<Listener>` tag into the file `$CATALINA_HOME$/conf/server.xml` as shown below.

```xml
<Server>
	...
    <Listener className="com.hazelcast.session.P2PLifecycleListener"/>
    ...
</Server>
```

- Put a `<Manager>` tag into the file `$CATALINA_HOME$/conf/context.xml` as shown below.

```xml
<Context>
	...
    <Manager className="com.hazelcast.session.HazelcastSessionManager"/>
    ...
</Context>
```

- Start Tomcat instances with a configured load balancer and deploy the web application.

***Optional Listener Tag Parameters***

- Optionally, you can add `configLocation` attribute into the `<Listener>` tag. If not provided, `hazelcast.xml` in the classpath is used by default. URL or full filesystem path as a `configLocation` value is supported.

<br></br>

#### Client/Server Deployment

In this deployment type, Tomcat instances work as clients on an existing Hazelcast Cluster.

***Features***

-	The existing Hazelcast cluster is used as the Session Replication Cluster.
-	Offloading Session Cache from Tomcat to the Hazelcast Cluster.
-	The architecture is completely independent. Complete reboot of Tomcat instances.
<br></br>

***Sample Client/Server Configuration to use Hazelcast Session Replication***

- Go to [hazelcast.com](http://www.hazelcast.com/products/hazelcast-enterprise/) and download the latest Hazelcast Enterprise.
- Unzip the Hazelcast Enterprise zip file into the folder `$HAZELCAST_ENTERPRISE_ROOT`.
- Put `$HAZELCAST_ENTERPRISE_ROOT/lib/hazelcast-client-`<*version*>`.jar`,            `$HAZELCAST_ENTERPRISE_ROOT/lib/hazelcast-enterprise-`<*version*>`.jar` and           `$HAZELCAST_ENTERPRISE_ROOT/lib/hazelcast-enterprise-`<*tomcatversion*>`-`<*version*>`.jar` in the folder `$CATALINA_HOME/lib/`.

- Put a `<Listener>` tag into the `$CATALINA_HOME$/conf/server.xml` as shown below.

```xml
<Server>
	...
    <Listener className="com.hazelcast.session.ClientServerLifecycleListener"/>
    ...
</Server>
```

- Update the `<Manager>` tag in the `$CATALINA_HOME$/conf/context.xml` as shown below.

```xml
<Context>
     <Manager className="com.hazelcast.session.HazelcastSessionManager"
      clientOnly="true"/>
</Context>
```
- Launch a Hazelcast Instance using `$HAZELCAST_ENTERPRISE_ROOT/bin/server.sh` or `$HAZELCAST_ENTERPRISE_ROOT/bin/server.bat`.

- Start Tomcat instances with a configured load balancer and deploy the web application.



***Optional Listener Tag Parameters***

- Optionally, you can add `configLocation` attribute into the `<Listener>` tag. If not provided, `hazelcast-client-default.xml` in `hazelcast-client-`<*version*>`.jar` file is used by default. Any client XML file in the classpath, URL or full filesystem path as a `configLocation` value is also supported.

#### Optional Manager Tag Parameters

`<Manager>` tag is used both in P2P and Client/Server mode. You can use the following parameters to configure Tomcat Session Replication Module to better serve your needs.

- Add `mapName` attribute into `<Manager>` tag. Its default value is *default Hazelcast Distributed Map*. Use this attribute if you have a specially configured map for special cases like WAN Replication, Eviction, MapStore, etc.
- Add `sticky` attribute into `<Manager>` tag. Its default value is *true*.
- Add `processExpiresFrequency` attribute into `<Manager>` tag. It specifies the frequency of session validity check, in seconds. Its default value is *6* and the minimum value that you can set is *1*.
- Add `deferredWrite` attribute into `<Manager>` tag. Its default value is *true*.

<br></br>

#### Session Caching and deferredWrite parameter

Tomcat Web Session Replication Module has its own nature of caching. Attribute changes during the HTTP Request/HTTP Response cycle is cached by default. Distributing those changes to the Hazelcast Cluster is costly. Because of that, Session Replication is only done at the end of each request for updated and deleted attributes. The risk in this approach is losing data if a Tomcat crash happens in the middle of the HTTP Request operation.

You can change that behavior by setting `deferredWrite=false` in your `<Manager>` tag configuration. By disabling it, all updates that are done on session objects are directly distributed into Hazelcast Cluster.

#### Session Expiry

Based on Tomcat configuration or `sessionTimeout` setting in `web.xml`, sessions are expired over time. This requires a cleanup on the Hazelcast Cluster since there is no need to keep expired sessions in the cluster. 

`processExpiresFrequency`, which is defined in [`<Manager>`](#optional-manager-tag-parameters), is the only setting that controls the behavior of session expiry policy in the Tomcat Web Session Replication Module. By setting this, you can set the frequency of the session expiration checks in the Tomcat Instance.

#### Enabling Session Replication in Multi-App environment

Tomcat can be configured in two ways to enable Session Replication for deployed applications.

- Server Context.xml Configuration
- Application Context.xml Configuration

***Server Context.xml Configuration***

By configuring `$CATALINA_HOME$/conf/context.xml`, you can enable session replication for all applications deployed in the Tomcat Instance. 


***Application Context.xml Configuration***

By configuring `$CATALINA_HOME/conf/[enginename]/[hostname]/[applicationName].xml`, you can enable Session Replication per deployed application.

#### Session Affinity 

***Sticky Sessions (default)***

Sticky Sessions are used to improve the performance since the sessions do not move around the cluster.
 
Request goes always to the same instance where the session was firstly created. By using a sticky session, you eliminate session replication problems mostly, except for the failover cases. In case of failovers, Hazelcast helps you not lose existing sessions.


***Non-Sticky Sessions***

Non-Sticky Sessions are not good for performance because you need to move session data all over the cluster every time a new request comes in.

However, load balancing might be super easy with Non-Sticky caches. In case of heavy load, you can distribute the request to the least used Tomcat instance. Hazelcast supports Non-Sticky Sessions as well. 

#### Tomcat Failover and jvmRoute Parameter

Each HTTP Request is redirected to the same Tomcat instance if sticky sessions are enabled. The parameter `jvmRoute` is added to the end of session ID as a suffix, to make Load Balancer aware of the target Tomcat instance. 

When Tomcat Failure happens and Load Balancer cannot redirect the request to the owning instance, it sends a request to one of the available Tomcat instances. Since the `jvmRoute` parameter of session ID is different than that of the target Tomcat instance, Hazelcast Session Replication Module updates the session ID of the session with the new `jvmRoute` parameter. That means that the Session is moved to another Tomcat instance and Load Balancer will redirect all subsequent HTTP Requests to the new Tomcat Instance.

![image](images/NoteSmall.jpg) ***NOTE:*** *If stickySession is enabled, `jvmRoute` parameter must be set in `$CATALINA_HOME$/conf/server.xml` and unique among Tomcat instances in the cluster.*

```xml
 <Engine name="Catalina" defaultHost="localhost" jvmRoute="tomcat-8080">
```


<br></br>