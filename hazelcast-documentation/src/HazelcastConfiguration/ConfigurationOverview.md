

# Hazelcast Configuration

## Configuration Overview

Hazelcast can be configured declaratively (XML) or programmatically (API) or even by the mix of both.

**1- Declarative Configuration**

If you are creating new Hazelcast instance with passing `null` parameter to `Hazelcast.newHazelcastInstance(null)` or just using empty factory method (`Hazelcast.newHazelcastInstance()`), Hazelcast will look into two places for the configuration file:

-   **System property:** Hazelcast will first check if "`hazelcast.config`" system property is set to a file path. Example: `-Dhazelcast.config=C:/myhazelcast.xml`.

-   **Classpath:** If config file is not set as a system property, Hazelcast will check classpath for `hazelcast.xml` file.

If Hazelcast does not find any configuration file, it will happily start with default configuration (`hazelcast-default.xml`) located in `hazelcast.jar`. (Before configuring Hazelcast, please try to work with default configuration to see if it works for you. Default should be just fine for most of the users. If not, then consider custom configuration for your environment.)

If you want to specify your own configuration file to create `Config`, Hazelcast supports several ways including filesystem, classpath, InputStream, URL, etc.:

-   `Config cfg = new XmlConfigBuilder(xmlFileName).build();`

-   `Config cfg = new XmlConfigBuilder(inputStream).build();`

-   `Config cfg = new ClasspathXmlConfig(xmlFileName);`

-   `Config cfg = new FileSystemXmlConfig(configFilename);`

-   `Config cfg = new UrlXmlConfig(url);`

-   `Config cfg = new InMemoryXmlConfig(xml);`



**2- Programmatic Configuration**

To configure Hazelcast programmatically, just instantiate a `Config` object and set/change its properties/attributes due to your needs. Just to give an idea, below is a sample code in which some network, map, map store and near cache attributes are configured for a Hazelcast instance.

```java
Config config = new Config();
config.getNetworkConfig().setPort( 5900 );
config.getNetworkConfig().setPortAutoIncrement( false );
        
NetworkConfig network = config.getNetworkConfig();
JoinConfig join = network.getJoin();
join.getMulticastConfig().setEnabled( false );
join.getTcpIpConfig().addMember( "10.45.67.32" ).addMember( "10.45.67.100" )
            .setRequiredMember( "192.168.10.100" ).setEnabled( true );
network.getInterfaces().setEnabled( true ).addInterface( "10.45.67.*" );
        
MapConfig mapConfig = new MapConfig();
mapConfig.setName( "testMap" );
mapConfig.setBackupCount( 2 );
mapConfig.getMaxSizeConfig().setSize( 10000 );
mapConfig.setTimeToLiveSeconds( 300 );
        
MapStoreConfig mapStoreConfig = new MapStoreConfig();
mapStoreConfig.setClassName( "com.hazelcast.examples.DummyStore" )
    .setEnabled( true );
mapConfig.setMapStoreConfig( mapStoreConfig );

NearCacheConfig nearCacheConfig = new NearCacheConfig();
nearCacheConfig.setMaxSize( 1000 ).setMaxIdleSeconds( 120 )
    .setTimeToLiveSeconds( 300 );
mapConfig.setNearCacheConfig( nearCacheConfig );

config.addMapConfig( mapConfig );
```

After creating `Config` object, you can use it to create a new Hazelcast instance.

-   `HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance( config );`

-   To create a named `HazelcastInstance` you should set `instanceName` of `Config` object. 

```java
    Config config = new Config();
    config.setInstanceName( "my-instance" );
    Hazelcast.newHazelcastInstance( config );
    ```
-   To retrieve an existing `HazelcastInstance` using its name, use;

    `Hazelcast.getHazelcastInstanceByName( "my-instance" );`

-   To retrieve all existing `HazelcastInstance`s, use;

    `Hazelcast.getAllHazelcastInstances();`




Rest of the chapter will first explain the configuration items listed below.

- Network 
- Group
- Map
- MultiMap
- Queue
- Topic
- List
- Set
- Semaphore
- Executor Service
- Serialization
- Partition Group
- Jobtracker
- Services
- Management Center


Then, it will talk about Listener and Logging configurations. And finally, the chapter will end with the advanced system property definitions.

***ATTENTION:*** *Most of the sections below use the tags used in declarative configuration when explaining configuration items. We are assuming that the reader is familiar with their programmatic equivalents, since both approaches have the similar tag/method names (e.g. `port-count` tag in declarative configuration is equivalent to `setPortCount` in programmatic configuration).*

