# What's New in Hazelcast 3.4



## Release Notes

### New Features
This section provides the new features introduced with Hazelcast 3.4 release. 

- **High-Density Memory Store**: Used with the Hazelcast JCache implementation, High-Density Memory Store is introduced with this release. High-Density Memory Store is the enterprise grade backend storage solution. This solution minimizes the garbage collection pressure and thus enables predictable application scaling and boosts performance. For more information, please see [High-Density Memory Store](#high-density-memory-store).
- **Jetty Based Session Replication**: We have introduced Jetty-based web session replication with this release. This is a feature of Hazelcast Enterprise. It enables session replication for Java EE web applications that are deployed into Jetty servlet containers, without having to perform any changes in those applications. For more information, please see [Jetty Based Web Session Replication](#jetty-based-web-session-replication).
- **Hazelcast Configuration Import**: This feature, which is an element named `<import>`, enables you to compose the Hazelcast declarative (XML) configuration file out of smaller configuration snippets. For more information, please see [Composing XML Configuration](#composing-xml-configuration).



