

## Monitoring with JMX

You can monitor your Hazelcast members via the JMX protocol.

- Add the following system properties to enable [JMX agent](http://download.oracle.com/javase/1.5.0/docs/guide/management/agent.html):

   - `-Dcom.sun.management.jmxremote`
   - `-Dcom.sun.management.jmxremote.port=\_portNo\_` (to specify JMX port) (*optional*)
   - `-Dcom.sun.management.jmxremote.authenticate=false` (to disable JMX auth) (*optional*)


- Enable Hazelcast property `hazelcast.jmx` (please refer to [Advanced Configuration Properties](#advanced-configuration-properties));

   - using Hazelcast configuration (API, XML, Spring)
   - or by setting system property `-Dhazelcast.jmx=true`

- Use jconsole, jvisualvm (with mbean plugin) or another JMX compliant monitoring tool.
