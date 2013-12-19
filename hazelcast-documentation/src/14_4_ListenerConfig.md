

## Listener Configurations

Hazelcast provides various event listener extensions to receive specific event types. These are:

-   **MembershipListener** for cluster membership events

-   **DistributedObjectListener** for distributed object creation and destroy events

-   **MigrationListener** for partition migration start and complete events

-   **LifecycleListener** for HazelcastInstance lifecycle events

-   **EntryListener** for IMap and MultiMap entry events

-   **ItemListener** for IQueue, ISet and IList item events

-   **MessageListener** for ITopic message events

These listeners can be added to and removed from related object using Hazelcast API. Such as

```java
MembershipListener listener = new MyMembershipListener();
hazelcastInstance.getCluster().addMembershipListener(listener);
hazelcastInstance.getCluster().removeMembershipListener(listener);
```
```java
EntryListener listener = new MyEntryListener();
IMap map = hazelcastInstance.getMap("default");
String id =map.addEntryListener(listener, true);
map.removeEntryListener(id);
```
```java
ItemListener listener = new MyItemListener();
IQueue queue = hazelcastInstance.getQueue("default");
queue.addItemListener(listener, true);
queue.removeItemListener(listener);
```
Downside of attaching listeners using API is possibility of missing events between creation of object and registering listener. To overcome this race condition Hazelcast introduces registration of listeners in configuration. Listeners can be registered using either Hazelcast XML configuration, Config API or Spring configuration.

-  **MembershipListener**

*Hazelcast XML configuration*

```xml
<listeners>
        <listener>com.hazelcast.examples.MembershipListener</listener>
</listeners>
```
*Config API*

```java
config.addListenerConfig(new ListenerConfig("com.hazelcast.examples.MembershipListener"));
```
*Spring XML configuration*

```xml
<hz:listeners>
    <hz:listener class-name="com.hazelcast.spring.DummyMembershipListener"/>
    <hz:listener implementation="dummyMembershipListener"/>
</hz:listeners>
```
-   **DistributedObjectListener**

*Hazelcast XML configuration*

```xml
<listeners>
        <listener>com.hazelcast.examples.DistributedObjectListener</listener>
</listeners>
```
*Config API*

```java
config.addListenerConfig(new ListenerConfig("com.hazelcast.examples.DistributedObjectListener"));
```
*Spring XML configuration*

```xml
<hz:listeners>
    <hz:listener class-name="com.hazelcast.spring.DummyDistributedObjectListener"/>
    <hz:listener implementation="dummyDistributedObjectListener"/>
</hz:listeners>
```
-   **MigrationListener**

*Hazelcast XML configuration*

```xml
<listeners>
        <listener>com.hazelcast.examples.MigrationListener</listener>
</listeners>
```
*Config API*

```java
config.addListenerConfig(new ListenerConfig("com.hazelcast.examples.MigrationListener"));
```
*Spring XML configuration*

```xml
<hz:listeners>
    <hz:listener class-name="com.hazelcast.spring.DummyMigrationListener"/>
    <hz:listener implementation="dummyMigrationListener"/>
</hz:listeners>
```
-   **LifecycleListener**

*Hazelcast XML configuration*

```xml
<listeners>
        <listener>com.hazelcast.examples.LifecycleListener</listener>
</listeners>
```
*Config API*

```java
config.addListenerConfig(new ListenerConfig("com.hazelcast.examples.LifecycleListener"));
```
*Spring XML configuration*

```xml
<hz:listeners>
    <hz:listener class-name="com.hazelcast.spring.DummyLifecycleListener"/>
    <hz:listener implementation="dummyLifecycleListener"/>
</hz:listeners>
```
-   **EntryListener** for IMap

*Hazelcast XML configuration*

```xml
<map name="default">
    ...
    <entry-listeners>
        <entry-listener include-value="true" local="false">com.hazelcast.examples.EntryListener</entry-listener>
    </entry-listeners>
</map>
```
*Config API*

```java
mapConfig.addEntryListenerConfig(new EntryListenerConfig("com.hazelcast.examples.EntryListener", false, false));
```
*Spring XML configuration*

```xml
<hz:map name="default">
    <hz:entry-listeners>
        <hz:entry-listener class-name="com.hazelcast.spring.DummyEntryListener" include-value="true"/>
        <hz:entry-listener implementation="dummyEntryListener" local="true"/>
    </hz:entry-listeners>
</hz:map>
```
-   **EntryListener** for MultiMap

*Hazelcast XML configuration*

```xml
<multimap name="default">
    <value-collection-type>SET</value-collection-type>
    <entry-listeners>
        <entry-listener include-value="true" local="false">com.hazelcast.examples.EntryListener</entry-listener>
    </entry-listeners>
</multimap>
```
*Config API*

```java
multiMapConfig.addEntryListenerConfig(new EntryListenerConfig("com.hazelcast.examples.EntryListener", false, false));
```
*Spring XML configuration*

```xml
<hz:multimap name="default" value-collection-type="LIST">
    <hz:entry-listeners>
        <hz:entry-listener class-name="com.hazelcast.spring.DummyEntryListener" include-value="true"/>
        <hz:entry-listener implementation="dummyEntryListener" local="true"/>
    </hz:entry-listeners>
</hz:multimap>
```
-   **ItemListener** for IQueue

*Hazelcast XML configuration*

```xml
<queue name="default">
    ...
    <item-listeners>
        <item-listener include-value="true">com.hazelcast.examples.ItemListener</item-listener>
    </item-listeners>
</queue>
```
*Config API*

```java
queueConfig.addItemListenerConfig(new ItemListenerConfig("com.hazelcast.examples.ItemListener", true));
```
*Spring XML configuration*

```xml
<hz:queue name="default" >
    <hz:item-listeners>
        <hz:item-listener class-name="com.hazelcast.spring.DummyItemListener" include-value="true"/>
    </hz:item-listeners>
</hz:queue>
```
-   **MessageListener** for ITopic

*Hazelcast XML configuration*

```xml
<topic name="default">
    <message-listeners>
        <message-listener>com.hazelcast.examples.MessageListener</message-listener>
    </message-listeners>
</topic>
```
*Config API*

```java
topicConfig.addMessageListenerConfig(new ListenerConfig("com.hazelcast.examples.MessageListener"));
```
*Spring XML configuration*

```xml
<hz:topic name="default">
    <hz:message-listeners>
        <hz:message-listener class-name="com.hazelcast.spring.DummyMessageListener"/>
    </hz:message-listeners>
</hz:topic>
```

