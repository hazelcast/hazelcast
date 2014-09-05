

## Use Cases

Some example usages are listed below. Hazelcast can be used:
-	To share server configuration/information to see how a cluster performs,



-	To cluster highly changing data with event notifications (e.g. user based events) and to queue and distribute background tasks,



-	As a simple Memcache with near cache,



-	As a cloud-wide scheduler of certain processes that need to be performed on some nodes,



-	To share information (user information, queues, maps, etc.) on the fly with multiple nodes in different installations under OSGI environments,



-	To share thousands of keys in a cluster where there is a web service interface on application server and some validation,



-	As a distributed topic (publish/subscribe server) to build scalable chat servers for smartphones,



-	As a front layer for Cassandra back end,



-	To distribute user object states across the cluster, to pass messages between objects and to share system data structures (static initialization state, mirrored objects, object identity generators),



-	As a multi-tenancy cache where each tenant has its own map,



-	To share datasets (e.g. table-like data structure) to be used by applications,



-	To distribute the load and collect status from Amazon EC2 servers where front-end is developed using, for example, Spring framework,



-	As a real time streamer for performance detection,

-	As a storage for session data in web applications (enables horizontal scalability of the web application).
