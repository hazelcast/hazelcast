
### Configuring Hazelcast

While Hazelcast is starting up, it checks for the configuration as follows:

-	First, it looks for `hazelcast.config` system property. If it is set, its value is used as the path. It is useful if you want to be able to change your Hazelcast configuration. This is possible because it is not embedded within the application. The `config` option can be set by the below command:
 
	`- Dhazelcast.config=`*`<path to the hazelcast.xml>`*.
	
	The path can be a normal one or a classpath reference with the prefix `CLASSPATH`.
-	If the above system property is not set, Hazelcast then checks whether there is a `hazelcast.xml` file in the working directory.
-	If not, then it checks whether `hazelcast.xml` exists on the classpath.
-	If none of the above works, Hazelcast loads the default configuration, i.e. `hazelcast-default.xml` that comes with `hazelcast.jar`.



When you download and unzip `hazelcast-`*version*`.zip` you will see the `hazelcast.xml` in **/bin** folder. This is the configuration XML file for Hazelcast, a part of which is shown below.

![](images/HazelcastXML.jpg)

For most of the users, default configuration should be fine. If not, you can tailor this XML file according to your needs by adding/removing/modifying properties (Declarative Configuration). Please refer to [Configuration Properties](#advanced-configuration-properties) for details.

Besides declarative configuration, you can configure your cluster programmatically (Programmatic Configuration). Just instantiate a `Config` object and add/remove/modify properties.
<br></br>


***RELATED INFORMATION***

*Please refer to [Configuration](#configuration) chapter for more information.*

#### Using Wildcard

Hazelcast supports wildcard configuration for all distributed data structures that can be configured using `Config` (i.e. for all except IAtomicLong, IAtomicReference). Using an asterisk (\*) character in the name, different instances of maps, queues, topics, semaphores, etc. can be configured by a single configuration.

Note that, with a limitation of a single usage, asterisk (\*) can be placed anywhere inside the configuration name.

For instance, a map named '`com.hazelcast.test.mymap`' can be configured using one of these configurations;

```xml
<map name="com.hazelcast.test.*">
...
</map>
```
```xml
<map name="com.hazel*">
...
</map>
```
```xml
<map name="*.test.mymap">
...
</map>
```
```xml
<map name="com.*test.mymap">
...
</map>
```
Or a queue '`com.hazelcast.test.myqueue`';

```xml
<queue name="*hazelcast.test.myqueue">
...
</queue>
```
```xml
<queue name="com.hazelcast.*.myqueue">
...
</queue>
```


