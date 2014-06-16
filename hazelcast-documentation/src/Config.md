
# Configuration



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

To configure Hazelcast programmatically, just instantiate a `Config` object and set/change its properties/attributes due to your needs.

```java
Config cfg = new Config();
cfg.getNetworkConfig().setPort(5900);
cfg.getNetworkConfig().setPortAutoIncrement(false);
        
NetworkConfig network = cfg.getNetworkConfig();
JoinConfig join = network.getJoin();
join.getMulticastConfig().setEnabled(false);
join.getTcpIpConfig().addMember("10.45.67.32").addMember("10.45.67.100")
            .setRequiredMember("192.168.10.100").setEnabled(true);
network.getInterfaces().setEnabled(true).addInterface("10.45.67.*");
        
MapConfig mapCfg = new MapConfig();
mapCfg.setName("testMap");
mapCfg.setBackupCount(2);
mapCfg.getMaxSizeConfig().setSize(10000);
mapCfg.setTimeToLiveSeconds(300);
        
MapStoreConfig mapStoreCfg = new MapStoreConfig();
mapStoreCfg.setClassName("com.hazelcast.examples.DummyStore").setEnabled(true);
mapCfg.setMapStoreConfig(mapStoreCfg);

NearCacheConfig nearCacheConfig = new NearCacheConfig();
nearCacheConfig.setMaxSize(1000).setMaxIdleSeconds(120).setTimeToLiveSeconds(300);
mapCfg.setNearCacheConfig(nearCacheConfig);

cfg.addMapConfig(mapCfg);
```

After creating `Config` object, you can use it to create a new Hazelcast instance.

-   `HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(cfg);`
<a name="named-hazelcastinstance"></a>
-   To create a named `HazelcastInstance` you should set `instanceName` of `Config` object. 

    ```java
    Config cfg = new Config();
    config.setInstanceName('my-instance');
    Hazelcast.newHazelcastInstance(config);
    ```
-   To retrieve an existing `HazelcastInstance` using its name, use;

    `Hazelcast.getHazelcastInstanceByName('my-instance');`

-   To retrieve all existing`HazelcastInstance`s, use;

    `Hazelcast.getAllHazelcastInstances();`


