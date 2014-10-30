# What's New in Hazelcast 3.4



## Release Notes

### New Features
This section provides the new features introduced with Hazelcast 3.4 release. 

- High Density for JCache: ??? Please see [High Density for JCache](#high-density-for-jcache).
- Jetty Based Session Replication: We have introduced Jetty based web session replication with this release. It is a feature of Hazelcast Enterprise. It enables session replication for Java EE web applications, that are deployed into Jetty servlet containers, without performing any changes in those applications. For more information, please see [Jetty Based Web Session Replication](#jetty-based-web-session-replication).
- Hazelcast Configuration Import: This feature enables to compose the Hazelcast declarative (XML) configuration file out of smaller configuration snippets. We have introduced an element named `<import>` for this purpose. For more information, please see [Composing XML Configuration](#composing-xml-configuration).
- Back Pressure: This feature helps the system to slow down instead of breaking down under high load.


