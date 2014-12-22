# Integrated Clustering

In this chapter, we show you how Hazelcast is integrated with Hibernate 2nd level cache and Spring, and how Hazelcast helps with your Filter, Tomcat and Jetty based web session replications.

The [Hibernate Second Level Cache section](#hibernate-second-level-cache) tells how you should configure Hazelcast and Hibernate to integrate them. It explains the modes of Hazelcast that can be used by Hibernate and also provides how to perform advanced settings like accessing the underlying HazelcastInstance used by Hibernate.

The [Web Session Replication section](#web-session-replication) provides information on how to cluster user HTTP sessions automatically. Also, you can learn how to enable session replication for JEE web applications with Tomcat and Jetty containers. Please note that Tomcat and Jetty based web session replications are Hazelcast Enterprise only modules.

The [Spring Integration section](#spring-integration) tells how you can integrate Hazelcast into a Spring project by explaining the Hazelcast instance and client configurations with the *hazelcast* namespace. It also lists the supported Spring bean attributes. 



