package com.hazelcast.jet.sql.impl.connector.map;

import com.google.common.base.Stopwatch;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlRemovingMappingTest extends SqlTestSupport {
    private static final String MAP_NAME = "test";
    private static Config config;

    @BeforeClass
    public static void setUpClass() {
        config = smallInstanceConfig();
        initializeWithClient(3, config, new ClientConfig());
    }

    private void createMap(HazelcastInstance instance) {
        IMap<Long, UUID> map = instance.getMap(MAP_NAME);
        map.put(1L, UUID.randomUUID()); // Comment this to make it fast
        createMapping(instance, MAP_NAME, Long.class, UUID.class);
    }

    @Test
    public void test_strange() {
        createMap(instance());
        HazelcastInstance newInstance = factory().newHazelcastInstance(config);
        assertClusterSizeEventually(4, newInstance);
        logger.info("Waiting");
        Stopwatch stopwatch = Stopwatch.createStarted();
        assertEqualsEventually(() -> newInstance.getReplicatedMap("__sql.catalog").size(), 1);
        stopwatch.stop();
        logger.info("Waiting took: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
    }
}
