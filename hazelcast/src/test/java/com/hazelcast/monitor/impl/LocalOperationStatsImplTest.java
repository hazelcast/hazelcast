package com.hazelcast.monitor.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.Node;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LocalOperationStatsImplTest extends HazelcastTestSupport {

    @Test
    public void testDefaultConstructor() {
        LocalOperationStatsImpl localOperationStats = new LocalOperationStatsImpl();

        assertEquals(Long.MAX_VALUE, localOperationStats.getMaxVisibleSlowOperationCount());
        assertEquals(0, localOperationStats.getSlowOperations().size());
        assertTrue(localOperationStats.getCreationTime() > 0);
        assertNotNull(localOperationStats.toString());
    }

    @Test
    public void testNodeConstructor() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_MC_MAX_VISIBLE_SLOW_OPERATION_COUNT, "139");

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        Node node = getNode(hazelcastInstance);
        LocalOperationStatsImpl localOperationStats = new LocalOperationStatsImpl(node);

        assertEquals(139, localOperationStats.getMaxVisibleSlowOperationCount());
        assertEquals(0, localOperationStats.getSlowOperations().size());
        assertTrue(localOperationStats.getCreationTime() > 0);
        assertNotNull(localOperationStats.toString());
    }

    @Test
    public void testSerialization() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_MC_MAX_VISIBLE_SLOW_OPERATION_COUNT, "127");

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        Node node = getNode(hazelcastInstance);
        LocalOperationStatsImpl localOperationStats = new LocalOperationStatsImpl(node);

        LocalOperationStatsImpl deserialized = new LocalOperationStatsImpl();
        deserialized.fromJson(localOperationStats.toJson());

        assertEquals(localOperationStats, deserialized);
        assertEquals(localOperationStats.hashCode(), deserialized.hashCode());
    }
}
