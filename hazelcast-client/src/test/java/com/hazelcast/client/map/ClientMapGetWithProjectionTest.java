package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.MapGetWithProjectionTest;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapGetWithProjectionTest extends MapGetWithProjectionTest {

    @Override
    public <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }

        TestHazelcastFactory factory = new TestHazelcastFactory();

        Config config = getConfig();
        config.setProperty("hazelcast.partition.count", "3");

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("aggr");
        mapConfig.setInMemoryFormat(inMemoryFormat);
        config.addMapConfig(mapConfig);

        doWithConfig(config);

        for (int i = 0; i < nodeCount; i++)
            factory.newHazelcastInstance(config);

        HazelcastInstance instance = factory.newHazelcastClient();
        return instance.getMap("aggr");
    }

}
