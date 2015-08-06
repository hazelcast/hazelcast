package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalTopicStatsImplTest {

    private LocalTopicStatsImpl localTopicStats;

    @Before
    public void setUp() {
        localTopicStats = new LocalTopicStatsImpl();

        localTopicStats.incrementPublishes();
        localTopicStats.incrementPublishes();
        localTopicStats.incrementPublishes();
        localTopicStats.incrementReceives();
        localTopicStats.incrementReceives();
    }

    @Test
    public void testDefaultConstructor() {
        assertTrue(localTopicStats.getCreationTime() > 0);
        assertEquals(3, localTopicStats.getPublishOperationCount());
        assertEquals(2, localTopicStats.getReceiveOperationCount());
        assertNotNull(localTopicStats.toString());
    }

    @Test
    public void testSerialization() {
        JsonObject serialized = localTopicStats.toJson();
        LocalTopicStatsImpl deserialized = new LocalTopicStatsImpl();
        deserialized.fromJson(serialized);

        assertTrue(deserialized.getCreationTime() > 0);
        assertEquals(3, deserialized.getPublishOperationCount());
        assertEquals(2, deserialized.getReceiveOperationCount());
        assertNotNull(deserialized.toString());
    }
}
