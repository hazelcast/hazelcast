

## Document Revision History

|Chapter|Section|Description|
|:-------|:-------|:-----------|
|[Chapter 1 - Preface](#preface)||Added information on how to contribute to Hazelcast.|
|[Chapter 2 - What's New in Hazelcast 3.5](#what-s-new-in-hazelcast-3-5)|[Upgrading from 3.x](#upgrading-from-3-x)|Added as a new section.|
|[Chapter 3 - Getting Started](#getting-started)|[Deploying On Amazon EC2](#deploying-on-amazon-ec2)|Added as a new section to provide a sample deployment project.|
|[Chapter 4 - Overview](#overview)||Separated from [Getting Started](#getting-started) as a new chapter.|
||[Data Partitioning](#data-partitioning)|Added as a new section explaining how the partitioning works in Hazelcast.|
|[Chapter 5 - Hazelcast Clusters](#hazelcast-clusters)|[Creating Cluster Groups](#creating-cluster-groups)|Added as a new section explaining how to separate a Hazelcast cluster.|
|[Chapter 6 - Distributed Data Structures](#distributed-data-structures)|[Map](#map)|The content of the section, previously read as Entry Listener improved and its name changed to [Map Listener](#map-listener).<br><br> Description of the new element `write-coalescing` added to the [Write-Behind section](#write-behind).<br><br> [Incremental Key Loading](#incremental-key-loading) added as a new section.<br><br> [Example Map Eviction Scenario](#example-map-eviction-scenario) added as a new section.<br></br> [MapPartitionLostListener](#mappartitionlostlistener) added as a new section.<br></br>A note stating the change in the return type of the method `loadAllKeys()` added to the [Initialization On Startup section](#initialization-on-startup).|
||[Replicated Map](#replicated-map)|[Replicated Map Configuration](#replicated-map-configuration) added as a new section explaining the configuration elements.|
|[Chapter 7 - Distributed Events](#distributed-events)||The whole chapter improved by adding sections describing each listener.|
||[Partition Lost Listener](#partition-lost-listener)|Added as a new section.|
|[Chapter 8 - Distributed Computing](#distributed-computing)|[Execution Member Selector](#execution-member-selector)|Added as a new section explaining how to select a cluster member on which an execution will be performed.|
|[Chapter 9 - Distributed Query](#distributed-query)|[Paging Predicate](#paging-predicate)|Added a note related to random page access.|
|[Chapter 11 - Transactions](#transactions)||Added a note related to `REPEATABLE_READ` isolation level.|
|[Chapter 12 - Hazelcast JCache](#hazelcast-jcache)|[JCache Near Cache](#jcache-near-cache)|Added as a new section explaining the invalidation concept, eviction policies and configuration of JCache's near cache feature.|
|[Chapter 13 - Integrated Clustering](#integrated-clustering)||Added introduction paragraphs.|
||[Tomcat Based Web Session Replication](#tomcat-based-web-session-replication)|Updated the Overview paragraph to include the support for Tomcat 8.
|[Chapter 14 - Storage](#storage)|[Sizing Practices](#sizing-practices)|Added as a new section.|
|[Chapter 15 - Hazelcast Java Client](#hazelcast-java-client)||Separated from the formerly known "Clients" chapter to be a chapter of its own.|
|||Added an important note related to the new Java Client library in the chapter introduction.
|[Chapter 16 - Other Client Implementations](#other-client-implementations)||C++, .NET, Memcache and REST client sections separated from the formerly known "Clients" chapter.|
|[Chapter 18 - Management](#management)|[JMX API per Node](#jmx-api-per-node)|Two new bean definitions added to the Hazelcast Instance list (Cluster Safe State and LocalMember Safe State).|
||[Management Center](#management-center)|Added more information on the time travel data files to the [Time Travel section](#time-travel).|
|[Chapter 19 - Security](#security)|[ClusterLoginModule](#clusterloginmodule)|The [Enterprise Integration section](#enterprise-integration) added .|
|[Chapter 20 - Performance](#performance)|[Hazelcast Performance on AWS](#hazelcast-performance-on-aws)|Added as a new section that provides best practices to improve the Hazelcast performance on Amazon Web Service.|
||[Back Pressure](#back-pressure)|Added as a new section.
||[SlowOperationDetector](#slowoperationdetector)|Added as a new section explaining the `SlowOperationDetector`, a monitoring feature that collects information of all slow operations.
|[Chapter 21 - Hazelcast Simulator](#hazelcast-simulator)||Added as a new chapter providing comprehensive information on the Hazelcast Simulator feature.|
|[Chapter 22 - WAN](#wan)|[WAN Replication Queue Capacity](#wan-replication-queue-capacity)|The previous heading title (WAN Replication Queue Size) and the system property name (`hazelcast.enterprise.wanrep.queuesize`) changed to WAN Replication Queue Capacity and `hazelcast.enterprise.wanrep.queue.capacity`.|
||[Enterprise WAN Replication](#enterprise-wan-replication)|Added as a new section.
|[Chapter 23 - Hazelcast Configuration](#hazelcast-configuration)||Improved by adding missing configuration elements and attributes. Added introduction paragraphs to the chapter.|
||[Configuration Overview](#configuration-overview)|Added a note related to the invalid configurations.
||[Using Variables](#using-variables)| Added as a new section explaining how to use variables in declarative configuration.|
||[System Properties](#system-properties)|Updated by adding the new system properties.
||[Enterprise WAN Replication Configuration](#enterprise-wan-replication-configuration)|Added as a new section describing the elements and attributes of Enterprise WAN Replication configuration.
|[Chapter 25 - License Questions](#license-questions)||Added as a new chapter describing the license information of dependencies.|
|[Chapter 26 - Common Exception Types](#common-exception-types)||Added as a new chapter.|
|[Chapter 27 - FAQ](#frequently-asked-questions)||Added new questions/answers.|
|[Chapter 28 - Glossary](#glossary)||Added new glossary items.|






<br> </br>


