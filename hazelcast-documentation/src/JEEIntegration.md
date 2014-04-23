
## J2EE Integration

Hazelcast can be integrated into J2EE containers via Hazelcast Resource Adapter (`hazelcast-ra-`*version*`.rar`). After proper configuration, Hazelcast can participate in standard J2EE transactions.

```java
<%@page import="javax.resource.ResourceException" %>
<%@page import="javax.transaction.*" %>
<%@page import="javax.naming.*" %>
<%@page import="javax.resource.cci.*" %>
<%@page import="java.util.*" %>
<%@page import="com.hazelcast.core.*" %>
<%@page import="com.hazelcast.jca.*" %>

<%
UserTransaction txn = null;
HazelcastConnection conn = null;
Config cfg = new Config();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);

try {
    Context context = new InitialContext();
    txn = (UserTransaction) context.lookup("java:comp/UserTransaction");
    txn.begin();

    HazelcastConnectionFactory cf = (HazelcastConnectionFactory) context.lookup ("java:comp/env/HazelcastCF");
    conn = cf.getConnection();

    TransactionalMap<String, String> txMap = conn.getTransactionalMap("default");
    txMap.put("key", "value");

    txn.commit();
} catch (Throwable e) {
    if (txn != null) {
        try {
            txn.rollback();
        } catch (Exception ix) {ix.printStackTrace();};
    }
    e.printStackTrace();
} finally {
    if (conn != null) {
        try {
            conn.close();
        } catch (Exception ignored) {};
    }
}
%>
```

### Resource Adapter Configuration

Deploying and configuring Hazelcast resource adapter is no different than any other resource adapter since it is a standard JCA resource adapter. However, resource adapter installation and configuration is container specific, so please consult your J2EE vendor documentation for details. Most common steps are:

1. Add the `hazelcast-`*version*`.jar` to container's classpath. Usually there is a lib directory that is loaded automatically by the container on startup.
2. Deploy `hazelcast-ra-`*version*`.rar`. Usually there is some kind of a deploy directory. Name of the directory varies by container.
3. Make container specific configurations when/after deploying `hazelcast-ra-`*version*`.rar`. Besides container specific configurations, JNDI name for Hazelcast resource is set.
4. Configure your application to use the Hazelcast resource. Update `web.xml` and/or `ejb-jar.xml` to let container know that your application will use the Hazelcast resource and define the resource reference.
5. Make container specific application configuration to specify JNDI name used for the resource in the application.


### Sample Glassfish v3 Web Application Configuration

1. Place the `hazelcast-`*version*`.jar` into `GLASSFISH_HOME/glassfish/domains/domain1/lib/ext/` directory.
2. Place the `hazelcast-ra-`*version*`.rar` into `GLASSFISH_HOME/glassfish/domains/domain1/autodeploy/` directory.
3. Add the following lines to the `web.xml` file.

```xml
<resource-ref>
    <res-ref-name>HazelcastCF</res-ref-name>
    <res-type>com.hazelcast.jca.ConnectionFactoryImpl</res-type>
    <res-auth>Container</res-auth>
</resource-ref>
```

Notice that, we did not have to put `sun-ra.xml` into the RAR file since it comes with the `hazelcast-ra-`*version*`.rar` file already.

If Hazelcast resource is used from EJBs, you should configure `ejb-jar.xml` for resource reference and JNDI definitions, just like we did for `web.xml`.

#### Sample JBoss 7 Enterprise Application Configuration
- Place the `hazelcast-jca-rar-`*version*`.rar` into `JBOSS_HOME/standalone/deployments/` directory.
- Add the resource adapter to `JBOSS_HOME/standalone/configuration/standalone.xml (Or live with the defaults from within the RAR archive)
```xml
...
<subsystem xmlns="urn:jboss:domain:resource-adapters:1.0">
    <resource-adapters>
        <resource-adapter>
            <archive>hazelcast-jca-rar-*version*.rar</archive>
            <config-property name="configurationLocation">hazelcast.xml</config-property>
            <transaction-support>XATransaction</transaction-support>
            <connection-definitions>
                <connection-definition class-name="com.hazelcast.jca.ManagedConnectionFactoryImpl"
                    jndi-name="java:/HazelcastCF" pool-name="HazelcastConnectionFactory">
                    <config-property name="connectionTracingEvents"></config-property>
                    <config-property name="connectionTracingDetail">false</config-property>
                    <xa-pool>
                        <min-pool-size>10</min-pool-size>
                        <max-pool-size>100</max-pool-size>
                    </xa-pool>
                    <security>
                        <application/>
                    </security>
                </connection-definition>
            </connection-definitions>
        </resource-adapter>
    </resource-adapters>
</subsystem>
...
```
- Add the following lines to the `EAR/META-INF/jboss-deployment-structure.xml` file.
```xml
<?xml version="1.0" encoding="UTF-8"?>
<jboss-deployment-structure xmlns="urn:jboss:deployment-structure:1.1">
    <!-- JBoss 7 specific file to add dependency on hazelcast-ra.rar so that 
        we can access the RA classes. Note: This will save us efforts to deploy Resource 
        Adapter as <global-module> -->
    <sub-deployment name="ejb-file.jar">
        <dependencies>
            <module name="deployment.hazelcast-jca-rar-*version*.rar" />
        </dependencies>
    </sub-deployment>
</jboss-deployment-structure> 
```
- Use the connection factory to access Hazelcast from within the EJB (contained in ejb-file.jar within the EAR):
```java
@Stateless
public class MyEjb {
	private static Logger logger = Logger.getLogger(CacheRefresher.class.getName());
	
	@Resource(mappedName="java:/HazelcastCF") 
	protected com.hazelcast.jca.HazelcastConnectionFactory hzFactory;
	
    public Object getFromCache() {
        // Open conneciton to access the methods as via HazelcastInstance
    	try (HazelcastConnection hazelcast = hzFactory.getConnection()) {
		    // Retrieve handle for cached set
    		ISet<Object> rs = hazelcast.getSet("myset");
			// Retrieve first object from cached set...
        	Object o = rs.iterator().next();
			// ... and log it
	    	logger.log(Level.FINE, "Retrieve first from set {0}", o);
			
	    	return o;
    	} catch (ResourceException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		// Connection is closed via Autoclosable
    }
    
	// Modifying operation is done transactional
	@Transactional
    public void putIntoCache(Object o) {
	    // Same pattern as with the retrieve: Connection is handled by Autoclosable
    	try (HazelcastConnection hazelcast = hzFactory.getConnection()) {
	    	TransactionalSet<Object> rs = hazelcast.getTransactionalSet("myset");
			rs.add(o);
	    	logger.log(Level.FINE, "Put {0} into cache", o);
	    	return s;
    	} catch (ResourceException e) {
    		throw new RuntimeException(e.getMessage(), e);
		}
    }
}

```

#### Sample JBoss 5 Web Application Configuration

- Place the `hazelcast-`*version*`.jar` into `JBOSS_HOME/server/deploy/default/lib` directory.
- Place the `hazelcast-ra-`*version*`.rar` into `JBOSS_HOME/server/deploy/default/deploy` directory
- Create a `hazelcast-ds.xml` file at `JBOSS_HOME/server/deploy/default/deploy` directory containing below content. Make sure to set the `rar-name` element to `hazelcast-ra-`*version*`.rar`.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE connection-factories
  PUBLIC "-//JBoss//DTD JBOSS JCA Config 1.5//EN"
  "http://www.jboss.org/j2ee/dtd/jboss-ds_1_5.dtd">

<connection-factories>
 <tx-connection-factory>
      <local-transaction/>
      <track-connection-by-tx>true</track-connection-by-tx>
      <jndi-name>HazelcastCF</jndi-name>
      <rar-name>hazelcast-ra-<version>.rar</rar-name>
      <connection-definition>
           javax.resource.cci.ConnectionFactory
      </connection-definition>
  </tx-connection-factory>
</connection-factories>
```

- Add the following lines to the `web.xml` file.

```xml
<resource-ref>
    <res-ref-name>HazelcastCF</res-ref-name>
    <res-type>com.hazelcast.jca.ConnectionFactoryImpl</res-type>
    <res-auth>Container</res-auth>
</resource-ref>
```

- Add the following lines to the `jboss-web.xml` file.

```xml
<resource-ref>
    <res-ref-name>HazelcastCF</res-ref-name>
    <jndi-name>java:HazelcastCF</jndi-name>
</resource-ref>
```

If Hazelcast resource is used from EJBs, you should configure `ejb-jar.xml` and `jboss.xml` for resource reference and JNDI definitions.


