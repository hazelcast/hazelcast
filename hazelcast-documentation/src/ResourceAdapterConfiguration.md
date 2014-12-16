

### Resource Adapter Configuration

Deploying and configuring the Hazelcast resource adapter is no different than configuring any other resource adapter since the Hazelcast resource adapter is a standard JCA one. However, resource adapter installation and configuration is container specific, so please consult your J2EE vendor documentation for details. The most common steps are:

1. Add the `hazelcast-`*version*`.jar` and `hazelcast-jca-`*version*`.jar` to the container's classpath. Usually there is a lib directory that is loaded automatically by the container on startup.
2. Deploy `hazelcast-jca-rar-`*version*`.rar`. Usually there is some kind of a deploy directory. The name of the directory varies by container.
3. Make container specific configurations when/after deploying `hazelcast-jca-rar-`*version*`.rar`. Besides container specific configurations, set the JNDI name for the Hazelcast resource.
4. Configure your application to use the Hazelcast resource. Update `web.xml` and/or `ejb-jar.xml` to let the container know that your application will use the Hazelcast resource and define the resource reference.
5. Make the container specific application configuration to specify the JNDI name used for the resource in the application.

