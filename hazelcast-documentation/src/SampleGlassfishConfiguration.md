


### Sample Glassfish v3 Web Application Configuration

1. Place the `hazelcast-`*version*`.jar` and `hazelcast-jca-`*version*`.jar` into the `GLASSFISH_HOME/glassfish/domains/domain1/lib/ext/` folder.
2. Place the `hazelcast-jca-rar-`*version*`.rar` into `GLASSFISH_HOME/glassfish/domains/domain1/autodeploy/` folder.
3. Add the following lines to the `web.xml` file.

```xml
<resource-ref>
  <res-ref-name>HazelcastCF</res-ref-name>
  <res-type>com.hazelcast.jca.ConnectionFactoryImpl</res-type>
  <res-auth>Container</res-auth>
</resource-ref>
```

Notice that we did not have to put `sun-ra.xml` into the RAR file since it already comes with the `hazelcast-ra-`*version*`.rar` file.

If the Hazelcast resource is used from EJBs, you should configure `ejb-jar.xml` for resource reference and JNDI definitions, just like for the `web.xml` file.

