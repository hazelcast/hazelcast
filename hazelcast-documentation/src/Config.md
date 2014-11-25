
# Configuration



You can configure Hazelcast declaratively (XML) or programmatically (API) or even by a mix of both.

## Declarative Configuration

If you are creating new Hazelcast instance with passing `null` parameter to `Hazelcast.newHazelcastInstance(null)` or just using empty factory method (`Hazelcast.newHazelcastInstance()`), Hazelcast will look into two places for the configuration file:

-   **System property:** Hazelcast will first check if "`hazelcast.config`" system property is set to a file path. Example: `-Dhazelcast.config=C:/myhazelcast.xml`.

-   **Classpath:** If config file is not set as a system property, Hazelcast will check classpath for `hazelcast.xml` file.

If Hazelcast does not find any configuration file, it will start with the default configuration (`hazelcast-default.xml`) located in `hazelcast.jar`. (Before configuring Hazelcast, please try that default configuration to see if it works for you. Default should be just fine for most users. If not, then consider custom configuration for your environment.)

If you want to specify your own configuration file to create `Config`, Hazelcast supports several ways, including filesystem, classpath, InputStream, URL, etc.:

-   `Config cfg = new XmlConfigBuilder(xmlFileName).build();`

-   `Config cfg = new XmlConfigBuilder(inputStream).build();`

-   `Config cfg = new ClasspathXmlConfig(xmlFileName);`

-   `Config cfg = new FileSystemXmlConfig(configFilename);`

-   `Config cfg = new UrlXmlConfig(url);`

-   `Config cfg = new InMemoryXmlConfig(xml);`



## Programmatic Configuration

To configure Hazelcast programmatically, instantiate a `Config` object and set/change its properties/attributes for your needs.

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

After creating a `Config` object, you can use it to create a new Hazelcast instance.

-   `HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance( config );`
<a name="named-hazelcastinstance"></a>
-   To create a named `HazelcastInstance` you should set `instanceName` of `Config` object.

```java
    Config config = new Config();
    config.setInstanceName( "my-instance" );
    Hazelcast.newHazelcastInstance( config );
    ```
-   To retrieve an existing `HazelcastInstance` using its name, use `getHazelcastInstanceByName`.

`Hazelcast.getHazelcastInstanceByName( "my-instance" );`

-   To retrieve all existing`HazelcastInstance`s, use `getAllHazelcastInstances`.

`Hazelcast.getAllHazelcastInstances();`


## Using Wildcard

Hazelcast supports wildcard configuration for all distributed data structures that can be configured using `Config` (i.e. for all except `IAtomicLong`, `IAtomicReference`). Using an asterisk (\*) character in the name, different instances of maps, queues, topics, semaphores, etc. can be configured by a single configuration.

Note that with a limitation of a single usage, an asterisk (\*) can be placed anywhere inside the configuration name.

For instance, a map named '`com.hazelcast.test.mymap`' can be configured using one of these configurations:

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
Or a queue '`com.hazelcast.test.myqueue`':

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

## Composing XML Configuration

You can compose your Hazelcast XML Configuration file from multiple XML configuration snippets. In order to compose XML configuration, you can use the `<import/>` element to load different XML configuration files. Please see the following examples.   

`hazelcast-config.xml`:

```xml
<hazelcast>
  <import resource="development-group-config.xml"/>
  <import resource="development-network-config.xml"/>
</hazelcast>
```

`development-group-config.xml`:

```xml
<hazelcast>
  <group>
      <name>dev</name>
      <password>dev-pass</password>
  </group>
</hazelcast>
```

`development-network-config.xml`:

```xml
<hazelcast>
  <network>
    <port auto-increment="true" port-count="100">5701</port>
    <join>
        <multicast enabled="true">
            <multicast-group>224.2.2.3</multicast-group>
            <multicast-port>54327</multicast-port>
        </multicast>
    </join>
  </network>
</hazelcast>
```
<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *You can only use `<import/>` element on top level of the XML hierarchy.*
<br></br>

- XML resources can be loaded from classpath and filesystem. For example:

```xml
<hazelcast>
  <import resource="file:///etc/hazelcast/development-group-config.xml"/> <!-- loaded from filesystem -->
  <import resource="classpath:development-network-config.xml"/>  <!-- loaded from classpath -->
</hazelcast>
```

- You can use property placeholders in the `<import/>` elements. For example:

```xml
<hazelcast>
  <import resource="${environment}-group-config.xml"/>
  <import resource="${environment}-network-config.xml"/>
</hazelcast>
```
