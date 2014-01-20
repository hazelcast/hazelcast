

# Monitoring with JMX

-   Add the following system properties to enable [jmx agent](http://download.oracle.com/javase/1.5.0/docs/guide/management/agent.html)

    -   -Dcom.sun.management.jmxremote

    -   -Dcom.sun.management.jmxremote.port=\_portNo\_ (to specify jmx port) *optional*

    -   -Dcom.sun.management.jmxremote.authenticate=false (to disable jmx auth) *optional*

-   Enable Hazelcast property *hazelcast.jmx*

    -   using Hazelcast configuration (api, xml, spring)

    -   or set system property -Dhazelcast.jmx=true

-   Use jconsole, jvisualvm (with mbean plugin) or another jmx-compliant monitoring tool.

**Following attributes can be monitored:**

-   Cluster

    -   config

    -   group name

    -   count of members and their addresses (host:port)

    -   operations: restart, shutdown cluster

-   Member

    -   inet address

    -   port

    -   lite member state

-   Statistics

    -   count of instances

    -   number of instances created, destroyed since startup

    -   max instances created, destroyed per second

-   AtomicLong

    -   name

    -   actual value

    -   operations: add, set, compareAndSet, reset

-   List, Set

    -   name

    -   size

    -   items (as strings)

    -   operations: clear, reset statistics

-   Map

    -   name

    -   size

    -   operations: clear

-   Queue

    -   name

    -   size

    -   received and served items

    -   operations: clear, reset statistics

-   Topic

    -   name

    -   number of messages dispatched since creation, in last second

    -   max messages dispatched per second


