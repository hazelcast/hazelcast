
## JCache Specification Compliance

Hazelcast JCache is fully compliant with the JSR 107 TCK (Technology Compatibility Kit), therefore it is officially a JCache
implementation. This is tested by running the TCK against the Hazelcast implementation.

You can test Hazelcast JCache for compliance by executing the TCK. Just perform the instructions below:


1. Checkout the TCK from [https://github.com/jsr107/jsr107tck](https://github.com/jsr107/jsr107tck).
2. Change the properties in `tck-parent/pom.xml` as shown below.
3. Run the TCK by `mvn clean install`.


```xml
<properties>
  <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  <project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding>

  <CacheInvocationContextImpl>
    javax.cache.annotation.impl.cdi.CdiCacheKeyInvocationContextImpl
  </CacheInvocationContextImpl>

  <domain-lib-dir>${project.build.directory}/domainlib</domain-lib-dir>
  <domain-jar>domain.jar</domain-jar>


  <!-- ################################################################# -->
  <!-- Change the following properties on the command line
       to override with the coordinates for your implementation-->
  <implementation-groupId>com.hazelcast</implementation-groupId>
  <implementation-artifactId>hazelcast</implementation-artifactId>
  <implementation-version>3.4</implementation-version>

  <!-- Change the following properties to your CacheManager and
       Cache implementation. Used by the unwrap tests. -->
  <CacheManagerImpl>
    com.hazelcast.client.cache.impl.HazelcastClientCacheManager
  </CacheManagerImpl>
  <CacheImpl>com.hazelcast.cache.ICache</CacheImpl>
  <CacheEntryImpl>
    com.hazelcast.cache.impl.CacheEntry
  </CacheEntryImpl>

  <!-- Change the following to point to your MBeanServer, so that
       the TCK can resolve it. -->
  <javax.management.builder.initial>
    com.hazelcast.cache.impl.TCKMBeanServerBuilder
  </javax.management.builder.initial>
  <org.jsr107.tck.management.agentId>
    TCKMbeanServer
  </org.jsr107.tck.management.agentId>
  <jsr107.api.version>1.0.0</jsr107.api.version>

  <!-- ################################################################# -->
</properties>
```

This will run the tests using an embedded Hazelcast Member.
