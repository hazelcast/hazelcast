

## J2EE Integration

Hazelcast can be integrated into J2EE containers via the Hazelcast Resource Adapter (`hazelcast-jca-rar-`*version*`.rar`). After proper configuration, Hazelcast can participate in standard J2EE transactions.

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
HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

try {
  Context context = new InitialContext();
  txn = (UserTransaction) context.lookup( "java:comp/UserTransaction" );
  txn.begin();

  HazelcastConnectionFactory cf = (HazelcastConnectionFactory)
      context.lookup ( "java:comp/env/HazelcastCF" );
        
  conn = cf.getConnection();

  TransactionalMap<String, String> txMap = conn.getTransactionalMap( "default" );
  txMap.put( "key", "value" );

  txn.commit();
    
} catch ( Throwable e ) {
  if ( txn != null ) {
    try {
      txn.rollback();
    } catch ( Exception ix ) {
      ix.printStackTrace();
    };
  }
  e.printStackTrace();
} finally {
  if ( conn != null ) {
    try {
      conn.close();
    } catch (Exception ignored) {};
  }
}
%>
```

### Sample Code for J2EE Integration

Please see our sample application for [J2EE Integration](https://github.com/hazelcast/hazelcast-code-samples/tree/master/hazelcast-integration/jca-ra).


