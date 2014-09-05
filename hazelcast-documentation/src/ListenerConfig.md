

## Listener Configurations

Event listeners can be added to and removed from the related object using Hazelcast API.

Downside of attaching listeners using API is the possibility of missing events between creation of object and registering listener. To overcome this race condition, Hazelcast introduces registration of listeners in configuration. Listeners can be registered using either declarative, programmatic or Spring configuration.

-  **MembershipListener**

	-	Declarative Configuration

		```xml
		<listeners>
          <listener>com.hazelcast.examples.MembershipListener</listener>
		</listeners>
```

	-	Programmatic Configuration

		```java
		config.addListenerConfig(
		    new ListenerConfig( "com.hazelcast.examples.MembershipListener" ) );
```

	-	Spring XML configuration

		```xml
		<hz:listeners>
		  <hz:listener class-name="com.hazelcast.spring.DummyMembershipListener"/>
		  <hz:listener implementation="dummyMembershipListener"/>
		</hz:listeners>
```

-   **DistributedObjectListener**

	-	Declarative Configuration

		```xml
		<listeners>
		  <listener>com.hazelcast.examples.DistributedObjectListener</listener>
		</listeners>
```

	-	Programmatic Configuration

		```java
		config.addListenerConfig(
		    new ListenerConfig( "com.hazelcast.examples.DistributedObjectListener" ) );
```

	-	Spring XML configuration

		```xml
		<hz:listeners>
		  <hz:listener class-name="com.hazelcast.spring.DummyDistributedObjectListener"/>
		  <hz:listener implementation="dummyDistributedObjectListener"/>
		</hz:listeners>
```

-   **MigrationListener**

	-	Declarative Configuration

		```xml
		<listeners>
		  <listener>com.hazelcast.examples.MigrationListener</listener>
		</listeners>
```

	-	Programmatic Configuration

		```java
		config.addListenerConfig( 
		    new ListenerConfig( "com.hazelcast.examples.MigrationListener" ) );
```

	-	Spring XML configuration

		```xml
		<hz:listeners>
		  <hz:listener class-name="com.hazelcast.spring.DummyMigrationListener"/>
		  <hz:listener implementation="dummyMigrationListener"/>
		</hz:listeners>
```

-   **LifecycleListener**

	-	Declarative Configuration

		```xml
		<listeners>
		  <listener>com.hazelcast.examples.LifecycleListener</listener>
		</listeners>
```

	-	Programmatic Configuration

		```java
		config.addListenerConfig(
		    new ListenerConfig( "com.hazelcast.examples.LifecycleListener" ) );
```

	-	Spring XML configuration

		```xml
		<hz:listeners>
		  <hz:listener class-name="com.hazelcast.spring.DummyLifecycleListener"/>
		  <hz:listener implementation="dummyLifecycleListener"/>
		</hz:listeners>
```

-   **EntryListener** for IMap

	-	Declarative Configuration

		```xml
		<map name="default">
		  ...
		  <entry-listeners>
		    <entry-listener include-value="true" local="false">
		        com.hazelcast.examples.EntryListener
		    </entry-listener>
		  </entry-listeners>
		</map>
```

	-	Programmatic Configuration

		```java
		mapConfig.addEntryListenerConfig(
		    new EntryListenerConfig( "com.hazelcast.examples.EntryListener", 
		                             false, false ) );
```

	-	Spring XML configuration

		```xml
		<hz:map name="default">
		  <hz:entry-listeners>
		    <hz:entry-listener include-value="true"
		         class-name="com.hazelcast.spring.DummyEntryListener"/>
		    <hz:entry-listener implementation="dummyEntryListener" local="true"/>
		  </hz:entry-listeners>
		</hz:map>
```

-   **EntryListener** for MultiMap

	-	Declarative Configuration

		```xml
		<multimap name="default">
		  <value-collection-type>SET</value-collection-type>
    	    <entry-listeners>
              <entry-listener include-value="true" local="false">
                  com.hazelcast.examples.EntryListener
              </entry-listener>
		    </entry-listeners>
		</multimap>
```
	-	Programmatic Configuration

		```java
		multiMapConfig.addEntryListenerConfig(
		    new EntryListenerConfig( "com.hazelcast.examples.EntryListener",
		                             false, false ) );
```

	-	Spring XML configuration

		```xml
		<hz:multimap name="default" value-collection-type="LIST">
		  <hz:entry-listeners>
		    <hz:entry-listener include-value="true"
		         class-name="com.hazelcast.spring.DummyEntryListener"/>
		    <hz:entry-listener implementation="dummyEntryListener" local="true"/>
		  </hz:entry-listeners>
		</hz:multimap>
```

-   **ItemListener** for IQueue

	-	Declarative Configuration

		```xml
		<queue name="default">
		  ...
		  <item-listeners>
	        <item-listener include-value="true">
	            com.hazelcast.examples.ItemListener
	        </item-listener>
		  </item-listeners>
		</queue>
```

	-	Programmatic Configuration

		```java
		queueConfig.addItemListenerConfig(
		    new ItemListenerConfig( "com.hazelcast.examples.ItemListener", true ) );
```

	-	Spring XML configuration

		```xml
		<hz:queue name="default" >
		  <hz:item-listeners>
		    <hz:item-listener include-value="true"
		        class-name="com.hazelcast.spring.DummyItemListener"/>
		  </hz:item-listeners>
		</hz:queue>
```

-   **MessageListener** for ITopic

	-	Declarative Configuration

		```xml
		<topic name="default">
		  <message-listeners>
		    <message-listener>
		        com.hazelcast.examples.MessageListener
		    </message-listener>
		  </message-listeners>
		</topic>
```

	-	Programmatic Configuration

		```java
		topicConfig.addMessageListenerConfig(
		    new ListenerConfig( "com.hazelcast.examples.MessageListener" ) );
```

	-	Spring XML configuration

		```xml
		<hz:topic name="default">
		  <hz:message-listeners>
		    <hz:message-listener 
		        class-name="com.hazelcast.spring.DummyMessageListener"/>
		  </hz:message-listeners>
		</hz:topic>
```

-   **ClientListener** 

	-	Declarative Configuration

		```xml
		<listeners>
		  <listener>com.hazelcast.examples.ClientListener</listener>
		</listeners>
```

	-	Programmatic Configuration

		```java
		topicConfig.addMessageListenerConfig(
		    new ListenerConfig( "com.hazelcast.examples.ClientListener" ) );
```

	-	Spring XML configuration

		```xml
		<hz:listeners>
		  <hz:listener class-name="com.hazelcast.spring.DummyClientListener"/>
		  <hz:listener implementation="dummyClientListener"/>
		</hz:listeners>
```
