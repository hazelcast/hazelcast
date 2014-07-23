

### Sample JBoss AS 7 / EAP 6 Web Application Configuration

Deployment on JBoss AS 7 or JBoss EAP 6 is a fairly straightforward process, steps of which are shown below. The only non-trivial step is the creation of a new JBoss module with Hazelcast libraries.     

- Create the directory `<jboss_home>/modules/system/layers/base/com/hazelcast/main`.
- Place the `hazelcast-`<*version*>`.jar` and `hazelcast-jca-`<*version*>`.jar` into the directory you created in the previous step.
- Create the file `module.xml` and place it to the same directory. This file should have the below content.

  ```xml
<module xmlns="urn:jboss:module:1.0" name="com.hazelcast">
  <resources>
    <resource-root path="."/>
    <resource-root path="hazelcast-<version>.jar"/>
    <resource-root path="hazelcast-jca-<version>.jar"/>
  </resources>
  <dependencies>
    <module name="javax.api"/>
    <module name="javax.resource.api"/>
    <module name="javax.validation.api"/>
    <module name="org.jboss.ironjacamar.api"/>
  </dependencies>
</module>
```

At this point, you have a new JBoss module with Hazelcast in it. You can now start JBoss and deploy the `hazelcast-jca-rar-`<*version*>`.rar` file via JBoss CLI or Administration Console.

Once the Hazelcast Resource Adapter is deployed, you can start using it. The easiest way is to let a container inject `ConnectionFactory` into your beans. 
    
```java
package com.hazelcast.examples.rar;

import com.hazelcast.core.TransactionalMap;
import com.hazelcast.jca.HazelcastConnection;

import javax.annotation.Resource;
import javax.resource.ResourceException;
import javax.resource.cci.ConnectionFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

@javax.ejb.Stateless
public class ExampleBean implements ExampleInterface {
    private final static Logger log = Logger.getLogger(ExampleBean.class.getName());

    @Resource(mappedName = "java:/HazelcastCF")
    protected ConnectionFactory connectionFactory;

    public void insert(String key, String value) {
        HazelcastConnection hzConn = null;
        try {
            hzConn = getConnection();
            TransactionalMap<String,String> txmap = hzConn.getTransactionalMap("txmap");
            txmap.put(key, value);
        } finally {
            closeConnection(hzConn);
        }
    }

    private HazelcastConnection getConnection() {
        try {
            return (HazelcastConnection) connectionFactory.getConnection();
        } catch (ResourceException e) {
            throw new RuntimeException("Error while getting Hazelcast connection", e);
        }
    }

    private void closeConnection(HazelcastConnection hzConn) {
        if (hzConn != null) {
            try {
                hzConn.close();
            } catch (ResourceException e) {
                log.log(Level.WARNING, "Error while closing Hazelcast connection.", e);
            }
        }
    }
}
```

#### Known Issues

- There is a regression in JBoss EAP 6.1.0 causing failure during Hazelcast Resource Adapter deployment. The issue is fixed in JBoss EAP 6.1.1. See [this](https://bugzilla.redhat.com/show_bug.cgi?id=976294) for additional details.  