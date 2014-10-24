
### Jetty Based Web Session Replication

![image](images/NoteSmall.jpg) ***NOTE:*** *This feature is supported for Hazelcast Enterprise 3.4 or higher.*


![](images/enterprise-onlycopy.jpg)


***Sample Code**: Please see our sample application for [Jetty Based Web Session Replication](https://github.com/hazelcast/hazelcast-code-samples/tree/master/hazelcast-integration/jetty-session-replication).*

#### Overview

Jetty Web Session Replication with Hazelcast Enterprise is a container specific module where no application change is required to enable session replication for JEE Web Applications. 

***Features***

1. Jetty 7 & 8 & 9 support
2. Support for sticky and non-sticky sessions
3. Jetty failover
4. Deferred write for performance boost
5. Client/Server and P2P modes
6. Declarative and programmatic configuration
<br></br>

***Supported Containers***

Jetty Web Session Replication Module has been tested against following containers.

- Jetty 7  - It can be downloaded [here](http://download.eclipse.org/jetty/stable-7/dist/).
- Jetty 8  - It can be downloaded [here](http://download.eclipse.org/jetty/stable-8/dist/).
- Jetty 9  - It can be downloaded [here](http://download.eclipse.org/jetty/stable-9/dist/).

Latest tested versions are **7.6.16.v20140903**, **8.1.16.v20140903** and **9.2.3.v20140905**
<br></br>


***Requirements***

 - Jetty instance must be running with Java 1.6 or higher.
 - Session objects that need to be clustered have to be Serializable.
 - Hazelcast Jetty Web Session Replication is built on top of jetty-nosql module. This module (jetty-nosql-<*jettyversion*>.jar) needs to be added to $JETTY_HOME/lib/ext
   This module can be found [here](http://mvnrepository.com/artifact/org.eclipse.jetty/jetty-nosql)

#### How Jetty Session Replication works

Jetty Session Replication in Hazelcast Enterprise is a Hazelcast Module where each created `HttpSession` Object's state kept in Hazelcast Distributed Map. 

As the session data are in Hazelcast Distributed Map, you can use all the available features offered by Hazelcast Distributed Map implementation such as MapStore and WAN Replication.

Jetty Web Session Replication run in two different modes:

- **P2P** where all Jetty instances launch its own Hazelcast Instance and join to the Hazelcast Cluster and,
- **Client/Server** mode where all Jetty instances put/retrieve the session data to/from an existing Hazelcast Cluster.


#### P2P (Peer-to-Peer) Deployment

This launches embedded Hazelcast Node in each server instance.

***Features***

This type of deployment is the simplest approach. You can just configure your Jetty and launch. There is no need for an  external Hazelcast cluster.

***Sample P2P Configuration to use Hazelcast Session Replication***

- Go to [hazelcast.com](http://www.hazelcast.com/products/hazelcast-enterprise/) and download the latest Hazelcast Enterprise.
- Unzip the Hazelcast Enterprise zip file into the folder `$HAZELCAST_ENTERPRISE_ROOT`.
- Update `$HAZELCAST_ENTERPRISE_ROOT/bin/hazelcast.xml` with the provided Hazelcast Enterprise License Key. 
- Put `$HAZELCAST_ENTERPRISE_ROOT/lib/hazelcast-enterprise-all-`<*version*>`.jar`,    `$HAZELCAST_ENTERPRISE_ROOT/lib/hazelcast-enterprise-`<*jettyversion*>`-`<*version*>`.jar` and `hazelcast.xml` to the folder `$JETTY_HOME/lib/ext`.
- Configure Session ID Manager and Session Manager

*Configuring the HazelcastSessionIdManager*

You need to configure an com.hazelcast.session.HazelcastSessionIdManager instance in jetty.xml. Add below section to your jetty.xml

```xml
<Set name="sessionIdManager">
    <New id="hazelcastIdMgr" class="com.hazelcast.session.HazelcastSessionIdManager">
        <Arg><Ref id="Server"/></Arg>
        <Set name="configLocation">etc/hazelcast.xml</Set>
    </New>
</Set>
```

*Configuring the HazelcastSessionManager*
  
HazelcastSessionManager can be configured from a context.xml file. (Each application has a context file under $CATALINA_HOME$/contexts folder, you need to create this file if it does not exist. 
Filename must be the same as the application name, i.e: example.war should have context file named example.xml)

```xml
<Ref name="Server" id="Server">
    <Call id="hazelcastIdMgr" name="getSessionIdManager"/>
</Ref>
<Set name="sessionHandler">
    <New class="org.eclipse.jetty.server.session.SessionHandler">
        <Arg>
            <New id="hazelcastMgr" class="com.hazelcast.session.HazelcastSessionManager">
                <Set name="idManager">
                    <Ref id="hazelcastIdMgr"/>
                </Set>
            </New>
        </Arg>
    </New>
</Set>
```

- Start Jetty instances with a configured load balancer and deploy web application.


NOTE: In Jetty 9 there is no folder with name contexts. You have to put context file under webapps directory. And you need to add these configs to context file.

```xml
<Ref name="Server" id="Server">
    <Call id="hazelcastIdMgr" name="getSessionIdManager"/>
</Ref>
<Set name="sessionHandler">
    <New class="org.eclipse.jetty.server.session.SessionHandler">
        <Arg>
            <New id="hazelcastMgr" class="com.hazelcast.session.HazelcastSessionManager">
                <Set name="sessionIdManager">
                    <Ref id="hazelcastIdMgr"/>
                </Set>
            </New>
        </Arg>
    </New>
</Set>
```




#### Client/Server Deployment

In this deployment type, Jetty instances work as clients to an existing Hazelcast Cluster.

***Features***

-	Existing Hazelcast cluster is used as the Session Replication Cluster.
-	The architecture is completely independent. Complete reboot of Jetty instances without losing data.
<br></br>

***Sample Client/Server Configuration to use Hazelcast Session Replication***

- Go to [hazelcast.com](http://www.hazelcast.com/products/hazelcast-enterprise/) and download the latest Hazelcast Enterprise.
- Unzip the Hazelcast Enterprise zip file into the folder `$HAZELCAST_ENTERPRISE_ROOT`.
- Update `$HAZELCAST_ENTERPRISE_ROOT/bin/hazelcast.xml` with the provided Hazelcast Enterprise License Key. 
- Put `$HAZELCAST_ENTERPRISE_ROOT/lib/hazelcast-enterprise-all-`<*version*>`.jar`,    `$HAZELCAST_ENTERPRISE_ROOT/lib/hazelcast-enterprise-`<*jettyversion*>`-`<*version*>`.jar` and `hazelcast.xml` to the folder `$JETTY_HOME/lib/ext`.
- Configure Session ID Manager and Session Manager

*Configuring the HazelcastSessionIdManager*

You need to configure an com.hazelcast.session.HazelcastSessionIdManager instance in jetty.xml. Add below section to your jetty.xml

```xml
<Set name="sessionIdManager">
    <New id="hazelcastIdMgr" class="com.hazelcast.session.HazelcastSessionIdManager">
        <Arg><Ref id="Server"/></Arg>
        <Set name="configLocation">etc/hazelcast.xml</Set>
        <Set name="clientOnly">true</Set>
    </New>
</Set>
```

*Configuring the HazelcastSessionManager*

HazelcastSessionManager can be configured from a context.xml file. (Each application has a context file under $CATALINA_HOME$/contexts folder, you need to create this file if it does not exist. 
Filename must be the same as the application name, i.e: example.war should have context file named example.xml)


```xml
    <Ref name="Server" id="Server">
        <Call id="hazelcastIdMgr" name="getSessionIdManager"/>
    </Ref>    
    <Set name="sessionHandler">
        <New class="org.eclipse.jetty.server.session.SessionHandler">
            <Arg>
                <New id="mongoMgr" class="com.hazelcast.session.HazelcastSessionManager">
                    <Set name="idManager">
                        <Ref id="hazelcastIdMgr"/>
                    </Set>
                </New>
            </Arg>
        </New>
    </Set>
```

NOTE: In Jetty 9 there is no folder with name contexts. You have to put context file under webapps directory. And you need to add these configs to context file.
```xml
    <Ref name="Server" id="Server">
        <Call id="hazelcastIdMgr" name="getSessionIdManager"/>
    </Ref>    
    <Set name="sessionHandler">
        <New class="org.eclipse.jetty.server.session.SessionHandler">
            <Arg>
                <New id="mongoMgr" class="com.hazelcast.session.HazelcastSessionManager">
                    <Set name="sessionIdManager">
                        <Ref id="hazelcastIdMgr"/>
                    </Set>
                </New>
            </Arg>
        </New>
    </Set>
```

- Launch a Hazelcast Instance using `$HAZELCAST_ENTERPRISE_ROOT/bin/server.sh` or `$HAZELCAST_ENTERPRISE_ROOT/bin/server.bat`.

- Start Tomcat instances with a configured load balancer and deploy web application.



#### Optional HazelcastSessionIdManager Parameters

`HazelcastSessionIdManager` is used both in P2P and Client/Server mode. Following parameters are used to configure Jetty Session Replication Module to better serve your needs.

- `workerName` attribute. This attribute should be set to a unique value for each Jetty instance to enable session affinity with a sticky-session configured load balancer 
- `cleanUpPeriod`attribute. This attribute is defined in milliseconds and defines the working period of session clean-up task.

<br></br>

#### Optional HazelcastSessionManager Parameters

`HazelcastSessionManager` is used both in P2P and Client/Server mode. Following parameters are used to configure Jetty Session Replication Module to better serve your needs.

- `savePeriod` attribute. This attribute set the interval of session data saving to Hazelcast cluster. Jetty Web Session Replication Module has its own nature of caching. Attribute changes during the HTTP Request/HTTP Response cycle is cached by default. Distributing those changes to the Hazelcast Cluster is costly. Because of that, Session Replication is only done at the end of each request for updated and deleted attributes. The risk in this approach is to lose data in case a Jetty crash happens in the middle of HTTP Request operation.
You can change that behavior by setting this attribute.

*If save period is set to -2, HazelcastSessionManager.save method is called for every doPutOrRemove operation.*
*If save period is set to -1, HazelcastSessionManager.save is never called if jetty is not shutting down*
*If save period is set to 0 (default), HazelcastSessionManager.save is called at the end of request*
*If save period is set to 1, HazelcastSessionManager.save is called at the end of request if session is dirty*

<br></br>


#### Session Expiry

Based on Tomcat configuration or `sessionTimeout` setting in `web.xml`, sessions are expired over time. This requires a cleanup on Hazelcast Cluster as there is no need to keep expired sessions in the cluster. 

`cleanUpPeriod` which is defined in `HazelcastSessionIdManager` is the only setting to control the behavior of session expiry policy in Jetty Web Session Replication Module. By setting this, you can set the frequency of the session expiration checks in the Jetty Instance.


#### Session Affinity 

HazelcastSessionIdManager can work in sticky/non-sticky setups.

The clustered session mechanism works in conjunction with a load balancer that supports stickiness. 
Stickiness can be based on various data items, such as source IP address or characteristics of the session ID or a load-balancer specific mechanism. 
For those load balancers that examine the session ID, HazelcastSessionIdManager appends a node ID to the session ID, which can be used for routing.
You must configure the HazelcastSessionIdManager with a workerName that is unique across the cluster. 
Typically the name relates to the physical node on which the instance is executing. If this name is not unique, your load balancer might fail to distribute your sessions correctly.
If sticky sessions are enabled, `workerName` paramater has to be set.


```xml
<Set name="sessionIdManager">
    <New id="hazelcastIdMgr" class="com.hazelcast.session.HazelcastSessionIdManager">
        <Arg><Ref id="Server"/></Arg>
        <Set name="configLocation">etc/hazelcast.xml</Set>
        <Set name="workerName">unique-worker-1</Set>
    </New>
</Set>
```


<br></br>