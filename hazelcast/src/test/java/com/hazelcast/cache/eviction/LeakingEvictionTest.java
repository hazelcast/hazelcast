package com.hazelcast.cache.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.cache.eviction.HazelcastConfigurator.configureMap;
import static com.hazelcast.core.Hazelcast.newHazelcastInstance;
import static java.util.UUID.randomUUID;

@RunWith(JUnit4.class)
public class LeakingEvictionTest {

    protected static final String MAP_NAME = "my.custom.map.test";
    protected static final String YOUR_IP_TO_BIND = "192.168.0.1";

    protected HazelcastInstance hazelcast1;
    protected HazelcastInstance hazelcast2;
    protected int size = 500;
    protected int multiplier = 4;

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testCacheEviction() {
        final List<String> keys = new ArrayList<String>();

        Config hazelcastConfig = createHazelcastConfig();
        System.out.print("Starting first node of the cluster... ");
        hazelcast1 = newHazelcastInstance(hazelcastConfig);
        System.out.println("[Done]");

        IMap map1 = hazelcast1.getMap(MAP_NAME);
        System.out.print("Re-configuring map on node 1... ");
        configureMap(hazelcast1, MAP_NAME, new MyCustomSettings(size));
        System.out.println("[Done]");

        System.out.print("Populating and checking map size from node 1: ");
        for (int i = 0; i < size * multiplier; i++) {
            String key = randomUUID().toString();
            keys.add(key);
            map1.put(key, randomUUID().toString());
        }
        System.out.println("Map size is " + map1.size());

        System.out.print("Reading data from the map to populate near cache... ");
        for (String key: keys) {
            map1.get(key);
        }
        System.out.println("[Done]");
        System.out.println("Map size is " + map1.size());

        System.out.print("Starting second node of the cluster... ");
        hazelcastConfig = createHazelcastConfig();
        hazelcast2 = newHazelcastInstance(hazelcastConfig);
        System.out.println("[Done]");
        System.out.print("Re-configuring map on node 2... ");
        IMap map2 = hazelcast2.getMap(MAP_NAME);
        configureMap(hazelcast2, MAP_NAME, new MyCustomSettings(size));
        System.out.println("[Done]");

        System.out.print("Checking map size from node 2: ");
        keys.clear();
        for (int i = 0; i < size * multiplier; i++) {
            String key = randomUUID().toString();
            keys.add(key);
            map2.put(key, randomUUID().toString());
        }
        System.out.println("Map size is " + map2.size());
        for (String key: keys) {
            map2.get(key);
        }
        System.out.println("[Done]");
        System.out.println("Map size is " + map2.size());
    }

    protected Config createHazelcastConfig() {
        Config hazelcastConfig = new Config();
        hazelcastConfig.addMapConfig(new MapConfig(hazelcastConfig.getMapConfig("default")));

        hazelcastConfig.getNetworkConfig().setPortAutoIncrement(true);
        hazelcastConfig.setProperty("hazelcast.map.invalidation.batch.enabled", "false");
        hazelcastConfig.setProperty("hazelcast.map.invalidation.batch.size", "1");
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setCacheLocalEntries(true);

        hazelcastConfig.addMapConfig(new MapConfig(hazelcastConfig.getMapConfig("default"))
                .setName(MAP_NAME)
                .setMaxSizeConfig(new MaxSizeConfig(50000, MaxSizeConfig.MaxSizePolicy.PER_NODE))
                .setTimeToLiveSeconds(0)
                .setBackupCount(0)
                .setMaxIdleSeconds(0)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setAsyncBackupCount(0)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setReadBackupData(false)
                .setNearCacheConfig(nearCacheConfig)
                .setMergePolicy("com.hazelcast.map.merge.PutIfAbsentMapMergePolicy"));

        InterfacesConfig interfacesConfig = hazelcastConfig.getNetworkConfig().getInterfaces();
        interfacesConfig.setEnabled(true);
        interfacesConfig.addInterface(YOUR_IP_TO_BIND); // <- set correct value for your machine here
        hazelcastConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);

        return hazelcastConfig;
    }
}
