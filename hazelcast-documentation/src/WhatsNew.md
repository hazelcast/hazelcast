# What's New in Hazelcast 3.4



## Release Notes

### New Features
This section provides the new features introduced with Hazelcast 3.4 release. 

- **Hi-Density Cache**: As part of the Hazelcast JCache implementation, Hi-Density Cache has been introduced with this release. It is the enterprise grade backend storage solution. This solution minimizes the garbage collection pressure and thus enables predictable application scaling and boosts performance. For more information, please see [Hi-Density JCache](#hi-density-jcache).
- **Jetty Based Session Replication**: We have introduced Jetty based web session replication with this release. It is a feature of Hazelcast Enterprise. It enables session replication for Java EE web applications, that are deployed into Jetty servlet containers, without performing any changes in those applications. For more information, please see [Jetty Based Web Session Replication](#jetty-based-web-session-replication).
- **Hazelcast Configuration Import**: This feature enables to compose the Hazelcast declarative (XML) configuration file out of smaller configuration snippets. We have introduced an element named `<import>` for this purpose. For more information, please see [Composing XML Configuration](#composing-xml-configuration).
- **Back Pressure**: This feature helps the system to slow down instead of breaking down under high load.


