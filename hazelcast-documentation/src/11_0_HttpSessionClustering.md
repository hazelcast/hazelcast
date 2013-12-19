
# Http Session Clustering with HazelcastWM

Say you have more than one web servers (A, B, C) with a load balancer in front of them. If server A goes down then your users on that server will be directed to one of the live servers (B or C) but their sessions will be lost! So we have to have all these sessions backed up somewhere if we don't want to lose the sessions upon server crashes. Hazelcast WM allows you to cluster user http sessions automatically. The following are required for enabling Hazelcast Session Clustering:

-   Target application or web server should support Java 1.5+

-   Target application or web server should support Servlet 2.4+ spec

-   Session objects that needs to be clustered have to be Serializable

Here are the steps to setup Hazelcast Session Clustering:

1.  Put the `hazelcast` and `hazelcast-wm` jars in your `WEB-INF/lib` directory. Optionally if you wish to connect to a cluster as a client add `hazelcast-client` as well.

2.  Put the following xml into `web.xml` file. Make sure Hazelcast filter is placed before all the other filters if any; put it at the top for example.

```xml             
<filter>
    <filter-name>hazelcast-filter</filter-name>
    <filter-class>com.hazelcast.web.WebFilter</filter-class>
    <!--
        Name of the distributed map storing
        your web session objects
    -->
    <init-param>
        <param-name>map-name</param-name>
        <param-value>my-sessions</param-value>
    </init-param>
    <!--
        How is your load-balancer configured?
        stick-session means all requests of a session
        is routed to the node where the session is first created.
        This is excellent for performance.
        If sticky-session is set to false, when a session is updated
        on a node, entry for this session on all other nodes is invalidated.
        You have to know how your load-balancer is configured before
        setting this parameter. Default is true.
    -->
    <init-param>
        <param-name>sticky-session</param-name>
        <param-value>true</param-value>
    </init-param>
    <!--
        Name of session id cookie
    -->
    <init-param>
        <param-name>cookie-name</param-name>
        <param-value>hazelcast.sessionId</param-value>
    </init-param>
    <!--
        Domain of session id cookie. Default is based on incoming request.
    -->
    <init-param>
        <param-name>cookie-domain</param-name>
        <param-value>.mywebsite.com</param-value>
    </init-param>
    <!--
        Should cookie only be sent using a secure protocol? Default is false.
    -->
    <init-param>
        <param-name>cookie-secure</param-name>
        <param-value>false</param-value>
    </init-param>
    <!--
        Should HttpOnly attribute be set on cookie ? Default is false.
    -->
    <init-param>
        <param-name>cookie-http-only</param-name>
        <param-value>false</param-value>
    </init-param>
    <!--
        Are you debugging? Default is false.
    -->
    <init-param>
        <param-name>debug</param-name>
        <param-value>true</param-value>
    </init-param>
    <!--
        Configuration xml location;
            * as servlet resource OR
            * as classpath resource OR
            * as URL
        Default is one of hazelcast-default.xml
        or hazelcast.xml in classpath.
    -->
    <init-param>
        <param-name>config-location</param-name>
        <param-value>/WEB-INF/hazelcast.xml</param-value>
    </init-param>
    <!--
        Do you want to use an existing HazelcastInstance?
        Default is null.
    -->
    <init-param>
        <param-name>instance-name</param-name>
        <param-value>default</param-value>
    </init-param>
    <!--
        Do you want to connect as a client to an existing cluster?
        Default is false.
    -->
    <init-param>
        <param-name>use-client</param-name>
        <param-value>false</param-value>
    </init-param>
    <!--
        Client configuration location;
            * as servlet resource OR
            * as classpath resource OR
            * as URL
        Default is null.
    -->
    <init-param>
        <param-name>client-config-location</param-name>
        <param-value>/WEB-INF/hazelcast-client.properties</param-value>
    </init-param>
    <!--
        Do you want to shutdown HazelcastInstance during
        web application undeploy process?
        Default is true.
    -->
    <init-param>
        <param-name>shutdown-on-destroy</param-name>
        <param-value>true</param-value>
    </init-param>
</filter>
<filter-mapping>
    <filter-name>hazelcast-filter</filter-name>
    <url-pattern>/*</url-pattern>
    <dispatcher>FORWARD</dispatcher>
    <dispatcher>INCLUDE</dispatcher>
    <dispatcher>REQUEST</dispatcher>
</filter-mapping>

<listener>
    <listener-class>com.hazelcast.web.SessionListener</listener-class>
</listener>
```
3.  Package and deploy your war file as you would normally do.

It is that easy! All http requests will go through Hazelcast `WebFilter` and it will put the session objects into Hazelcast distributed map if needed.

**Info about sticky-sessions:**

Hazelcast holds whole session attributes in a distributed map and in local http session. Local session is required for fast access to data and distributed map is needed for fail-safety.

-   *If sticky-session is not used, whenever a session a attribute is updated in a node (in both node local session and clustered cache), that attribute should be invalidated in all other nodes' local sessions, because now they have dirty value. So when a request arrives one of those other nodes that attribute value is fetched from clustered cache.*

-   *To overcome performance penalty of sending invalidation messages during updates, sticky-sessions can be used. If Hazelcast knows sessions are sticky, invalidation will not be send, because Hazelcast assumes there is no other local session at the moment. When a server is down, requests belonging to a session hold in that server will routed to other one and that server will fetch session data from clustered cache. That means using sticky-sessions, one will not suffer performance penalty of accessing clustered data and can benefit recover from a server failure.*
