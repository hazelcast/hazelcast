

# Monitoring with JMX

-   Add the following system properties to enable [JMX agent](http://download.oracle.com/javase/1.5.0/docs/guide/management/agent.html):

    - `-Dcom.sun.management.jmxremote`

    - `-Dcom.sun.management.jmxremote.port=\_portNo\_` (to specify JMX port) (*optional*)

    - `-Dcom.sun.management.jmxremote.authenticate=false` (to disable JMX auth) (*optional*)


-   Enable Hazelcast property `hazelcast.jmx` (refer to [System Property](#system-property));

    -   using Hazelcast configuration (API, XML, Spring)

    -   or set system property `-Dhazelcast.jmx=true`

-   Use jconsole, jvisualvm (with mbean plugin) or another JMX compliant monitoring tool.

**Following attributes can be monitored:**

-   Cluster

    -   configuration

    -   group name

    -   count of members and their addresses (*host:port*)

    -   operations: cluster restart, shutdown

-   Member

    -   inet address

    -   port


-   Statistics

    -   count of instances

    -   number of instances created/destroyed since startup

    -   maximum instances created/destroyed per second

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

    -   maximum messages dispatched per second


