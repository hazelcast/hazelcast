
### Jetty Based Web Session Replication

![image](images/NoteSmall.jpg) ***NOTE:*** *This feature is supported for Hazelcast Enterprise 3.4 or higher.*


![](images/enterprise-onlycopy.jpg)


***Sample Code:*** *Please see our [sample application](https://github.com/hazelcast/hazelcast-code-samples/tree/master/hazelcast-integration/enterprise-session-replication) for Jetty Based Web Session Replication.*
<br></br>

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
 - Hazelcast Jetty based Web Session Replication is built on top of `jetty-nosql` module. This module (`jetty-nosql-<*jettyversion*>.jar`) needs to be added to `$JETTY_HOME/lib/ext`.
   This module can be found [here](http://mvnrepository.com/artifact/org.eclipse.jetty/jetty-nosql).

#### How Jetty Session Replication works

Jetty Session Replication in Hazelcast Enterprise is a Hazelcast Module where each created `HttpSession` Object's state is kept in Hazelcast Distributed Map. 

As the session data are in Hazelcast Distributed Map, you can use all the available features offered by Hazelcast Distributed Map implementation such as MapStore and WAN Replication.

Jetty Web Session Replication runs in two different modes:

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
- Put `hazelcast.xml` to the folder `$JETTY_HOME/etc`.
- Put `$HAZELCAST_ENTERPRISE_ROOT/lib/hazelcast-enterprise-all-`<*version*>`.jar`,    `$HAZELCAST_ENTERPRISE_ROOT/lib/hazelcast-enterprise-`<*jettyversion*>`-`<*version*>`.jar` to the folder `$JETTY_HOME/lib/ext`.
- Configure Session ID Manager and Session Manager. Please see below explanations for configuring these managers.

*Configuring the HazelcastSessionIdManager*

You need to configure a `com.hazelcast.session.HazelcastSessionIdManager` instance in `jetty.xml`. Add below lines to your `jetty.xml`.

```xml
<Set name="sessionIdManager">
    <New id="hazelcastIdMgr" class="com.hazelcast.session.HazelcastSessionIdManager">
        <Arg><Ref id="Server"/></Arg>
        <Set name="configLocation">etc/hazelcast.xml</Set>
    </New>
</Set>
```

*Configuring the HazelcastSessionManager*
  
HazelcastSessionManager can be configured from a `context.xml` file. Each application has a context file under `$CATALINA_HOME$/contexts` folder. You need to create this file if it does not exist. 
Filename must be the same as the application name, e.g. `example.war` should have a context file named `example.xml`.

The file `context.xml` should have the below content.

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

- Start Jetty instances with a configured load balancer and deploy the web application.

![image](images/NoteSmall.jpg) ***NOTE:*** *In Jetty 9, there is no folder with name *`contexts`*. You have to put the file *`context.xml`* file under *`webapps`* directory. And you need to add below lines to *`context.xml`*.*


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
- Put `hazelcast.xml` to the folder `$JETTY_HOME/etc`.
- Put `$HAZELCAST_ENTERPRISE_ROOT/lib/hazelcast-enterprise-all-`<*version*>`.jar`,    `$HAZELCAST_ENTERPRISE_ROOT/lib/hazelcast-enterprise-`<*jettyversion*>`-`<*version*>`.jar`.
- Configure Session ID Manager and Session Manager. Please see below explanations for configuring these managers.

*Configuring the HazelcastSessionIdManager*

You need to configure a `com.hazelcast.session.HazelcastSessionIdManager` instance in `jetty.xml`. Add below lines to your `jetty.xml`.

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

HazelcastSessionManager can be configured from a `context.xml` file. Each application has a context file under `$CATALINA_HOME$/contexts` folder. You need to create this file if it does not exist. 
Filename must be the same as the application name, e.g. `example.war` should have a context file named `example.xml`.


```xml
    <Ref name="Server" id="Server">
        <Call id="hazelcastIdMgr" name="getSessionIdManager"/>
    </Ref>    
    <Set name="sessionHandler">
        <New class="org.eclipse.jetty.server.session.SessionHandler">
            <Arg>
                <New id="hazelMgr" class="com.hazelcast.session.HazelcastSessionManager">
                    <Set name="idManager">
                        <Ref id="hazelcastIdMgr"/>
                    </Set>
                </New>
            </Arg>
        </New>
    </Set>
```

![image](images/NoteSmall.jpg) ***NOTE:*** *In Jetty 9, there is no folder with name *`contexts`*. You have to put the file *`context.xml`* file under *`webapps`* directory. And you need to add below lines to *`context.xml`*.*

```xml
    <Ref name="Server" id="Server">
        <Call id="hazelcastIdMgr" name="getSessionIdManager"/>
    </Ref>    
    <Set name="sessionHandler">
        <New class="org.eclipse.jetty.server.session.SessionHandler">
            <Arg>
                <New id="hazelMgr" class="com.hazelcast.session.HazelcastSessionManager">
                    <Set name="sessionIdManager">
                        <Ref id="hazelcastIdMgr"/>
                    </Set>
                </New>
            </Arg>
        </New>
    </Set>
```

- Launch a Hazelcast Instance using `$HAZELCAST_ENTERPRISE_ROOT/bin/server.sh` or `$HAZELCAST_ENTERPRISE_ROOT/bin/server.bat`.

- Start Tomcat instances with a configured load balancer and deploy the web application.



#### Optional HazelcastSessionIdManager Parameters

`HazelcastSessionIdManager` is used both in P2P and Client/Server mode. Following parameters are used to configure Jetty Session Replication Module to better serve your needs:

- `workerName`: Set this attribute to a unique value for each Jetty instance to enable session affinity with a sticky-session configured load balancer.
- `cleanUpPeriod`: Defines the working period of session clean-up task in milliseconds.
- `configLocation`: You can specify location of `hazelcast.xml`.

<br></br>

#### Optional HazelcastSessionManager Parameters

`HazelcastSessionManager` is used both in P2P and Client/Server mode. Following parameters are used to configure Jetty Session Replication Module to better serve your needs:

- `savePeriod`: Sets the interval of saving session data to Hazelcast cluster. Jetty Web Session Replication Module has its own nature of caching. Attribute changes during the HTTP Request/HTTP Response cycle is cached by default. Distributing those changes to the Hazelcast Cluster is costly. Because of that, Session Replication is only done at the end of each request for updated and deleted attributes. The risk in this approach is to lose data in case a Jetty crash happens in the middle of HTTP Request operation.
You can change that behavior by setting this attribute.

Notes:

- If `savePeriod` is set to **-2**, `HazelcastSessionManager.save` method is called for every `doPutOrRemove` operation.
- If it is set to **-1**, the same method is never called if Jetty is not shut down.
- If it is set to **0** (the default value), the same method is called at the end of request.
- If it is set to **1**, the same method is called at the end of request if session is dirty.

<br></br>


#### Session Expiry

Based on Tomcat configuration or `sessionTimeout` setting in `web.xml`, the sessions are expired over time. This requires a cleanup on Hazelcast Cluster as there is no need to keep expired sessions in it. 

`cleanUpPeriod` which is defined in `HazelcastSessionIdManager` is the only setting to control the behavior of session expiry policy in Jetty Web Session Replication Module. By setting this, you can set the frequency of the session expiration checks in the Jetty Instance.


#### Session Affinity 

`HazelcastSessionIdManager` can work in sticky and non-sticky setups.

The clustered session mechanism works in conjunction with a load balancer that supports stickiness. Stickiness can be based on various data items, such as source IP address or characteristics of the session ID or a load-balancer specific mechanism. 
For those load balancers that examine the session ID, `HazelcastSessionIdManager` appends a node ID to the session ID, which can be used for routing.
You must configure the `HazelcastSessionIdManager` with a `workerName` that is unique across the cluster. 
Typically the name relates to the physical node on which the instance is executed. If this name is not unique, your load balancer might fail to distribute your sessions correctly.
If sticky sessions are enabled, `workerName` parameter has to be set, as shown below.


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